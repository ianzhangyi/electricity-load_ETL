def get_miso_transoutage(start_date=date.today(), end_date=date.today() + timedelta(days=180), as_of_date=date.today(), planned_rt='planned', simu_type='forecast', get_otheriso_outage=True):
    """ This function will pull all transmission outage between the start_date and end_date. The latest market_date BEFORE the as of_date will be used to pull.
        User can input the parameter as datetime.date(2013, 1, 1)
        Get transoutage_spp_df from DB
        simu_type should be one of the following: 'backtest', 'backforecast', 'forecast'
        'backtest': simulate history, e.g. Use last month's SE to simulate corresponding days, need to pull corresponding days's outage. Pull data from 'MISO_TransOutage_Historical' table
        'backforecast': assuming you are at a previous date, and want to simulate future, e.g. simulate this month using information you have by the beginning of last month
        'forecast': routine forecast, get the most up to date outage for the simulation period in the future
    """
    conn, engine = dbop.db_connect(db='MISO')
    # Get the latest MISO EMS-IDC mapping
    select_sql = "Select * FROM MISO_EMS_IDC_Mapping WHERE [Market_Date] IN (SELECT MAX([Market_Date]) FROM [MISO_EMS_IDC_Mapping])"
    mapping_df = read_sql(select_sql, engine)

    branch_df, gen_df, injgroup_df, gen_mapping_df = get_latest_cm_data(iso_name='MISO')
    branch_df = branch_df.loc[branch_df.Label != '', :].copy()
    branch_df['Label_nobranchid'] = branch_df['Label'].apply(lambda x: x.rsplit(' ', 1)[0])
    branch_df['Label_nospace'] = branch_df['Label'].apply(lambda x: x.replace(' ', ''))
    branch_df['BusNameFrom'] = branch_df['BusNameFrom'].astype(str).str.strip()
    branch_df['BusNameTo'] = branch_df['BusNameTo'].astype(str).str.strip()
    branch_df['NomkVFrom'] = branch_df['NomkVFrom'].astype(str).str.strip()
    branch_df['NomkVTo'] = branch_df['NomkVTo'].astype(str).str.strip()
    branch_df['Circuit'] = branch_df['Circuit'].astype(str).str.strip()
    branch_df['IDC Name_1'] = branch_df['BusNameFrom'].astype(str).str.ljust(12) + branch_df['NomkVFrom'].astype(str).str[:6] + ' ' + branch_df['BusNameTo'].astype(str).str.ljust(12) + branch_df['NomkVTo'].astype(str).str[:6] + ' ' + branch_df['Circuit'].astype(str) + ' '
    branch_df['IDC Name_2'] = branch_df['BusNameTo'].astype(str).str.ljust(12) + branch_df['NomkVTo'].astype(str).str[:6] + ' ' + branch_df['BusNameFrom'].astype(str).str.ljust(12) + branch_df['NomkVFrom'].astype(str).str[:6] + ' ' + branch_df['Circuit'].astype(str) + ' '
    branch_df_temp = branch_df.copy()
    branch_df_temp['Label_Update'] = branch_df_temp.Label.str[:-1].str.rstrip()
    # EMS_IDC mapping contains some errors, we use IDC_EQUIPMENT_NAME to Branch_Label[:-1] to update the mapping
    branch_df_temp = branch_df_temp[~branch_df_temp.duplicated(subset=['Label_Update'], keep=False)].copy()
    branch_df_temp.rename(columns={'Label': 'Branch_Label', 'Label_Update': 'Branch_Label_Update'}, inplace=True)
    if planned_rt.upper() == 'PLANNED' and simu_type.upper() == 'FORECAST':
        if as_of_date is None:
            as_of_date = start_date
        # Now pull outage
        select_sql = "Select * FROM [MISO_TransOutage_Planned] Where Market_Date IN (Select max(Market_Date) FROM [MISO_TransOutage_Planned] Where Market_Date <= \'{}\') ".format(as_of_date.strftime("%Y-%m-%d"))
        try:
            transoutage_df = read_sql(select_sql, engine)
        except:
            print("Retry after 10 sec")
            time.sleep(10)
            transoutage_df = read_sql(select_sql, engine)
        as_of_date = transoutage_df.iloc[0, 0]
        # transoutage_df.loc[pd.notnull(transoutage_df.ACTUAL_START), 'PLANNED_START'] = transoutage_df.loc[pd.notnull(transoutage_df.ACTUAL_START), 'ACTUAL_START']
        transoutage_df = transoutage_df.loc[((transoutage_df.PLANNED_START < (end_date + timedelta(days=1)).strftime("%Y-%m-%d")) | (transoutage_df.ACTUAL_START < (end_date + timedelta(days=1)).strftime("%Y-%m-%d"))) &
                                            ((transoutage_df.PLANNED_END >= start_date.strftime("%Y-%m-%d")) | (transoutage_df.ACTUAL_END >= start_date.strftime("%Y-%m-%d"))), :].copy()
        # Ignore outage with REQUEST_STATUS 'Completed'
        transoutage_df = transoutage_df.loc[transoutage_df.REQUEST_STATUS != 'Completed', :].copy()

    elif simu_type.upper() == 'BACKTEST':
        # Pull data from 'MISO_TransOutage_Historical' table
        select_sql = "Select * FROM [MISO_TransOutage_Historical] Where ACTUAL_END >= \'{}\' and ACTUAL_START <= \'{}\' ".format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        try:
            transoutage_df = read_sql(select_sql, engine)
        except:
            print("Retry after 10 sec")
            time.sleep(10)
            transoutage_df = read_sql(select_sql, engine)

    transoutage_df['Set_Status'] = ''  # 'Set_Status' will be used to set branch status in powerworld
    # Keep only Branch and Transformer
    transoutage_df = transoutage_df.loc[transoutage_df.EQUIPMENT_TYPE.isin(['Line', 'Transformer']), :].copy()
    # Use EQUIPMENT_REQUEST_TYPE to set 'Set_Status' column value
    transoutage_df.loc[transoutage_df.EQUIPMENT_REQUEST_TYPE == 'OOS', 'Set_Status'] = 'Open'
    transoutage_df.loc[transoutage_df.EQUIPMENT_REQUEST_TYPE.isin(['Info', 'GSP', 'HLW', 'InSvcNO']), 'Set_Status'] = 'Closed'

    # For EQUIPMENT_TYPE 'Transformer', first create Label by "FROM_STATION" + " " + "EMS_EQUIPMENT_NAME", then check if we have a match with latest SE model, keep the ones that match with EMS.
    transoutage_df.loc[transoutage_df.EQUIPMENT_TYPE == 'Transformer', 'Label'] = transoutage_df.loc[transoutage_df.EQUIPMENT_TYPE == 'Transformer', 'EMS_EQUIPMENT_NAME']

    # # For those that don't match with latest EMS model, reset 'Label' to ''
    # transoutage_df.loc[~transoutage_df.Label.isin(branch_df.Label.tolist()), 'Label'] = ''
    # Update 'IDC_Name' in mapping_df by adding a ' ' at the end, Pull EMS-IDC mapping from DB, use that to get Label
    mapping_df['IDC_Name'] = mapping_df['IDC_Name'] + ' '
    transoutage_df = pd.merge(transoutage_df, mapping_df[['IDC_Name', 'Label']], left_on=['IDC_EQUIPMENT_NAME'], right_on=['IDC_Name'], how='left', suffixes=('', '_x'))
    transoutage_df.loc[pd.isnull(transoutage_df.Label), 'Label'] = transoutage_df.loc[pd.isnull(transoutage_df.Label), 'Label_x']
    transoutage_df.drop(columns=['IDC_Name', 'Label_x'], inplace=True)
    print('Total branch_transformer outage data entry is {}'.format(str(transoutage_df.shape[0])))
    print('Total mapped outage data entry is {}'.format(str(transoutage_df.loc[transoutage_df.Label.isin(branch_df.Label.tolist()), :].shape[0])))

    transoutage_df = pd.merge(transoutage_df, branch_df[['IDC Name_1', 'Label']].drop_duplicates(['IDC Name_1']), left_on=['IDC_EQUIPMENT_NAME'], right_on=['IDC Name_1'], how='left', suffixes=('', '_y'))
    transoutage_df = pd.merge(transoutage_df, branch_df[['IDC Name_2', 'Label']].drop_duplicates(['IDC Name_2']), left_on=['IDC_EQUIPMENT_NAME'], right_on=['IDC Name_2'], how='left', suffixes=('', '_z'))
    transoutage_df.loc[pd.notnull(transoutage_df.Label_y), 'Label'] = transoutage_df.loc[pd.notnull(transoutage_df.Label_y), 'Label_y']
    transoutage_df.loc[pd.notnull(transoutage_df.Label_z), 'Label'] = transoutage_df.loc[pd.notnull(transoutage_df.Label_y), 'Label_z']
    transoutage_df.drop(columns=['IDC Name_1', 'Label_y', 'IDC Name_2', 'Label_z'], inplace=True)

    transoutage_df = pd.merge(transoutage_df, branch_df[['Label_nobranchid', 'Label']].drop_duplicates(['Label_nobranchid']), left_on=['EMS_EQUIPMENT_NAME'], right_on=['Label_nobranchid'], how='left', suffixes=('', '_x'))
    transoutage_df.loc[pd.isnull(transoutage_df.Label), 'Label'] = transoutage_df.loc[pd.isnull(transoutage_df.Label), 'Label_x']
    print('Total mapped outage data entry is {}'.format(str(transoutage_df.loc[transoutage_df.Label.isin(branch_df.Label.tolist()), :].shape[0])))

    transoutage_df = pd.merge(transoutage_df, branch_df[['Label_nospace', 'Label']].drop_duplicates(['Label_nospace']), left_on=['EMS_EQUIPMENT_NAME'], right_on=['Label_nospace'], how='left', suffixes=('', '_y'))
    transoutage_df.loc[pd.isnull(transoutage_df.Label), 'Label'] = transoutage_df.loc[pd.isnull(transoutage_df.Label), 'Label_y']
    transoutage_df.drop(columns=['Label_x', 'Label_y', 'Label_nobranchid', 'Label_nospace'], inplace=True)

    transoutage_df = pd.merge(transoutage_df, branch_df[['Label']].drop_duplicates(['Label']), left_on=['EMS_EQUIPMENT_NAME'], right_on=['Label'], how='left', suffixes=('', '_y'))
    transoutage_df.loc[pd.isnull(transoutage_df.Label), 'Label'] = transoutage_df.loc[pd.isnull(transoutage_df.Label), 'Label_y']
    transoutage_df.drop(columns=['Label_y'], inplace=True)

    # Update EMS_IDC mapping
    transoutage_df = transoutage_df.merge(branch_df_temp[['Branch_Label', 'Branch_Label_Update']], left_on='EMS_EQUIPMENT_NAME', right_on='Branch_Label_Update', how='left')
    transoutage_df.loc[transoutage_df.Branch_Label.notna() & (transoutage_df.Label != transoutage_df.Branch_Label), 'Label'] = transoutage_df.loc[transoutage_df.Branch_Label.notna() & (transoutage_df.Label != transoutage_df.Branch_Label), 'Branch_Label']
    transoutage_df.drop(columns=['Branch_Label', 'Branch_Label_Update'], inplace=True)

    transoutage_df = transoutage_df.loc[pd.notnull(transoutage_df.Label), :].copy()
    print('Total mapped branch_transformer outage data entry is {}'.format(str(transoutage_df.shape[0])))
    print('Total mapped outage data entry is {}'.format(str(transoutage_df.loc[transoutage_df.Label.isin(branch_df.Label.tolist()), :].shape[0])))

    transoutage_df.drop(columns=['Year', 'Month', 'Day'], inplace=True)
    if planned_rt.upper() == 'PLANNED' and simu_type.upper() == 'FORECAST':
        transoutage_df['Out_Days'] = (transoutage_df['PLANNED_END'] - transoutage_df['PLANNED_START']).dt.total_seconds() / (24 * 60 * 60)
    elif simu_type.upper() == 'BACKTEST':
        transoutage_df['Out_Days'] = (transoutage_df['ACTUAL_END'] - transoutage_df['ACTUAL_START']).dt.total_seconds() / (24 * 60 * 60)
    transoutage_df['Source'] = 'MISO'

    if get_otheriso_outage is True:
        # First pull SPP transmission outage
        transoutage_df_spp = get_spp_transoutage(start_date=start_date, end_date=end_date, as_of_date=as_of_date, planned_rt=planned_rt, simu_type=simu_type, get_otheriso_outage=False)
        # Get MISO_SPP_Branch mapping
        conn, engine = dbop.db_connect(db='MISO')
        select_sql = "Select * FROM MISO_SPP_Branch_Mapping"
        miso_spp_branch_mapping_df = read_sql(select_sql, engine)
        miso_spp_branch_mapping_df.drop_duplicates(['SPP_Label'], inplace=True)
        transoutage_df_spp = pd.merge(transoutage_df_spp, miso_spp_branch_mapping_df, left_on='Label', right_on='SPP_Label', how='left')
        transoutage_df_spp = transoutage_df_spp.loc[pd.notnull(transoutage_df_spp['MISO_Label']), :].copy()
        transoutage_df_spp.drop(columns=['Label'], inplace=True)
        transoutage_df_spp.rename(columns={'Rating': 'KV', 'Outage_Owner': 'COMPANY', 'IDC_Name': 'IDC_EQUIPMENT_NAME', 'Outage_Request_Status': 'REQUEST_STATUS', 'Priority': 'PRIORITY', 'Class_x': 'EQUIPMENT_TYPE', 'IDC_From_Bus_Name': 'FROM_STATION',
                                           'IDC_To_Bus_Name': 'TO_STATION', 'CROW_ID': 'OUTAGE_REQUEST_ID', 'SPP_Label': 'EMS_EQUIPMENT_NAME', 'Common_Name': 'COMMON_NAME', 'MISO_Label': 'Label'}, inplace=True)
        transoutage_df_spp['Source'] = 'SPP'
        for i in transoutage_df.columns:
            if i not in transoutage_df_spp.columns:
                transoutage_df_spp[i] = np.nan
        transoutage_df = pd.concat([transoutage_df, transoutage_df_spp[transoutage_df.columns]], axis=0)

        # Now pull TVA transmission outage
        transoutage_df_tva = get_tva_transoutage(start_date=start_date, end_date=end_date, as_of_date=as_of_date, planned_rt=planned_rt, simu_type=simu_type)
        if transoutage_df_tva.shape[0] > 1:
            transoutage_df_tva = transoutage_df_tva.loc[pd.notnull(transoutage_df_tva['Label']), :].copy()
            transoutage_df_tva.rename(columns={'PublishDate': 'Market_Date', 'Entered_By': 'COMPANY', 'IDC_Name': 'IDC_EQUIPMENT_NAME', 'EQUIPMENT_REQUEST_TYPE': 'REQUEST_STATUS', 'FrTLine_I': 'FROM_STATION', 'ToTLine_J': 'TO_STATION', 'MRID': 'OUTAGE_REQUEST_ID',
                                               'SDX_Common_Name': 'COMMON_NAME'}, inplace=True)
            for i in transoutage_df.columns:
                if i not in transoutage_df_tva.columns:
                    transoutage_df_tva[i] = np.nan
            transoutage_df = pd.concat([transoutage_df, transoutage_df_tva[transoutage_df.columns]], axis=0)

        # Now pull PJM Transmission outage
        # get_pjm_transoutage can only handle 'forecast' type of outage now
        if simu_type.upper() == 'FORECAST':
            transoutage_df_pjm = pd.DataFrame()
            try:
                print('Downloading PJM transmission outage...')
                transoutage_df_pjm = get_pjm_transoutage(start_date=start_date, end_date=end_date, as_of_date=as_of_date, planned_rt=planned_rt, simu_type=simu_type, get_otheriso_outage=False)
            except:
                print('Downloading PJM transmission outage again...')
                try:
                    transoutage_df_pjm = get_pjm_transoutage(start_date=start_date, end_date=end_date, as_of_date=as_of_date, planned_rt=planned_rt, simu_type=simu_type, get_otheriso_outage=False)
                except:
                    print('Error downloading PJM transmission outage...')
            if not transoutage_df_pjm.empty:
                if transoutage_df_pjm.shape[0] > 1:
                    transoutage_df_pjm = transoutage_df_pjm.loc[pd.notnull(transoutage_df_pjm['Label']), :].copy()
                    # Map PJM branches to MISO, out of ~26000 branches in PJM auction case, about 19000 can be mapped to MISO branch label
                    branch_df, gen_df, injgroup_df, gen_mapping_df = get_latest_cm_data(iso_name='MISO')
                    miso_pjm_mapping_df = get_miso_pjm_branch_mapping()
                    transoutage_df_pjm = pd.merge(transoutage_df_pjm, miso_pjm_mapping_df[['Label_PJM', 'Label_MISO', 'AreaNameFrom_PJM', 'AreaNameTo_PJM']].dropna().drop_duplicates(subset=['Label_PJM']).rename(columns={'Label_MISO': 'MISO_Label'}), left_on=['Label'], right_on=['Label_PJM'], how='left')
                    # transoutage_df_pjm['MISO_Label'] = transoutage_df_pjm['Label'].apply(lambda x: x[15:].strip() if len(x) > 15 else x.rsplit(' ')[-1])
                    # transoutage_df_pjm['MISO_Label'] = transoutage_df_pjm['MISO_Label'] + ' ' + transoutage_df_pjm['CKTID'].astype(str)
                    transoutage_df_pjm = pd.merge(transoutage_df_pjm, branch_df['Label'], left_on='MISO_Label', right_on='Label', how='left', suffixes=('_x', '_y'))
                    transoutage_df_pjm = transoutage_df_pjm.loc[pd.notnull(transoutage_df_pjm['Label_y']), :].copy()
                    transoutage_df_pjm.rename(columns={'Last Revised': 'Market_Date', 'Status': 'REQUEST_STATUS', 'FromBus': 'FROM_STATION', 'ToBus': 'TO_STATION', 'Ticket': 'OUTAGE_REQUEST_ID', 'Label_x': 'EMS_EQUIPMENT_NAME', 'Label_y': 'Label',
                                                       'Equipment': 'COMMON_NAME'}, inplace=True)
                    for i in transoutage_df.columns:
                        if i not in transoutage_df_pjm.columns:
                            transoutage_df_pjm[i] = np.nan
                    transoutage_df = pd.concat([transoutage_df, transoutage_df_pjm[transoutage_df.columns]], axis=0)
            else:
                print('PJM transmission outage has not been retrived.')

    # Take care of 'NOTES' column
    transoutage_df.loc[pd.isnull(transoutage_df['NOTES']), 'NOTES'] = ''
    transoutage_df['Label'] = transoutage_df['Label'].replace('\s+', ' ', regex=True)
    return transoutage_df

def get_latest_cm_data(iso_name='MISO'):
    """
    Check MISO/PJM_ComModel_ImportLog and find the commercial model with the latest Effective_Date
    Note that it can pull PJM commcrcial model data as well
    """
    # DB connection
    conn, engine = dbop.db_connect(db='MISO')  # MISO/SPP/PJM ComModel data are all stored in AnalysisDB

    select_sql0 = '''SELECT TOP 1 * from {}_ComModel_ImportLog ORDER BY Effective_Date DESC'''.format(iso_name)
    raw_df = read_sql(select_sql0, conn)
    cm_name = raw_df.iloc[0, :]['Com_Model_Name']

    # Now pull se_data
    select_sql1 = '''SELECT * from {}_ComModel_Branch Where Com_Model_Name = \'{}\' '''.format(iso_name, cm_name)
    branch_df = read_sql(select_sql1, conn)

    select_sql2 = '''SELECT * from {}_ComModel_Gen where Com_Model_Name = \'{}\' '''.format(iso_name, cm_name)
    gen_df = read_sql(select_sql2, conn)

    select_sql3 = '''SELECT * from {}_ComModel_InjectionGroup where Com_Model_Name = \'{}\' '''.format(iso_name, cm_name)
    injgroup_df = read_sql(select_sql3, conn)

    if iso_name != 'PJM':
        select_sql4 = '''SELECT * from {}_ComModel_Gen_Mapping where Com_Model_Name = \'{}\' '''.format(iso_name, cm_name)
        gen_mapping_df = read_sql(select_sql4, conn)

        select_sql5 = '''SELECT * from {}_ComModel_Cpnode where Com_Model_Name = \'{}\' '''.format(iso_name, cm_name)
        cpnodes_df = read_sql(select_sql5, conn)
        if cpnodes_df.shape[0] > 0:
            injgroup_df = injgroup_df.merge(cpnodes_df[['Commercial_Node_Name', 'EFFECTIVE_DATE', 'TERMINATION_DATE']].drop_duplicates(), left_on='Label', right_on='Commercial_Node_Name', how='left')
    else:
        gen_mapping_df = pd.DataFrame()

    return branch_df, gen_df, injgroup_df, gen_mapping_df
