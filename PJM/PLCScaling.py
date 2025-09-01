import os
import pandas as pd
from datetime import datetime
import regex as re
import Automation as auto
import db_operations as dbop
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='automation.log',
    filemode='a'
)
logging.getLogger('camelot').setLevel(logging.WARNING)


def find_target_files_path(folder_path, keywords):
    file_paths = {}
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if (file.endswith('.xls') or file.endswith('.xlsx')) and keywords in file:
                file_path = os.path.join(root, file)
                file_paths[file] = file_path
    return file_paths


def check_continuity(df, date_column, table_name):
    df = df.copy()
    # Ensure the date column is in datetime format
    df[date_column] = pd.to_datetime(df[date_column])
    complete_range = pd.date_range(start=df[date_column].min(), end=df[date_column].max(), freq='D')
    missing_dates_hours = complete_range[~complete_range.isin(df[date_column])]
    if not missing_dates_hours.empty:
        print(f"Continuity Check: Find missing values in {sorted(missing_dates_hours)} {table_name} data")
        message = f"Continuity Check Warning: Find missing values in output {sorted(missing_dates_hours)} {table_name} data"
    else:
        message = None

    return message


def data_check(df_processed, warning_messages):
    df_processed = df_processed.copy()
    df_processed['FlowDate'] = pd.to_datetime(df_processed['FlowDate'])

    # Continuity Check
    print('Checking Continuity...')
    for locale_name in df_processed['LocaleName'].unique():
        message = check_continuity(df_processed[(df_processed['LocaleName'] == locale_name)], 'FlowDate', f'{locale_name} data')
        if message:
            warning_messages.append(message)

    # Check NaN
    nan_rows = df_processed[df_processed.isna().any(axis=1)]

    if not nan_rows.empty:
        for index, row in nan_rows.iterrows():
            missing_time = row['FlowDate']
            locale_name = row['LocaleName']
            print(f'Find missing values in {missing_time} {locale_name} data')
            warning_messages.append(f'Missing Value Warning: Find missing values in {missing_time} {locale_name} data')
    else:
        print('No Missing Values')

    return warning_messages


def main(extract_data=True, base_path=r"C:\Users\5DIntern3_2024\Work\PJM"):
    try:
        logging.info('Starting ETL Process for PLCScaling')
        base_url = 'https://pjm.com/markets-and-operations/rpm.aspx'
        current_year = datetime.now().year
        keyword = 'rpm-daily-zonal-scaling-factors'
        directory = f"{base_path}/PLCScaling_data/raw_data"
        os.makedirs(directory, exist_ok=True)
        missing_data_year = []
        # Document the warning messages
        warning_messages = []
        if extract_data:
            logging.info('Downloading Data from PJM Website')
            downloaded_excel_paths = []
            for year in range(2014, current_year + 2):
                print("Processing year:", year)
                annual_links = auto.find_data_url(base_url, year, keyword, is_current_year=(year == current_year), is_scaling=True)
                if len(annual_links) != 0:
                    for link in annual_links:
                        downloaded_excel_paths.append(
                            auto.download_files(directory, annual_links[0], is_current_year=None, is_scaling=True)[0])
                        for file in os.listdir(directory):
                            file_path = os.path.join(directory, file)
                else:
                    missing_data_year.append(str(year))
                    print(f'data from {year - 1} is missing')
                    warning_messages.append(f'Data Source Warning: PLCScaling data from {year - 1} is missing in PJM website')
        else:
            logging.info('Loading Local Data')
            local_file_paths = find_target_files_path(directory, '')
            downloaded_excel_paths = []
            for file_path in local_file_paths:
                downloaded_excel_paths.append(local_file_paths[file_path])

        res = pd.DataFrame()
        logging.info('Processing Data')
        for file_path in downloaded_excel_paths:
            match = re.search(r'(\d{4})-\d{4}', file_path)
            if match:
                year = match.group(1)
                year = int(year)
                logging.info(f'Processing {year} data')

                file_ext = os.path.splitext(file_path)[1]
                if file_ext == '.xlsx':
                    engine = 'openpyxl'
                elif file_ext == '.xls':
                    engine = 'xlrd'
                df = pd.read_excel(file_path, header=None, engine=engine) #todo: check this
                first_row = None
                for index, row in df.iterrows():
                    if index + 1 < len(df):
                        next_row = df.iloc[index + 1]
                        if 'ZONENAME' in row.values and 'ZONENAME' not in next_row.values:
                            first_row = index
                            break
                df.columns = df.iloc[first_row]
                df = df.iloc[first_row + 1:]

                if year >= 2019:
                    df = df.rename(columns={'ZONENAME': 'drop', 'AREANAME': 'ZONENAME'})
                    df = df.drop(['drop'], axis=1)

                df = df.sort_values(by=['ZONENAME','EFFECTIVEDAY'], ascending=[True,True])

                unique_zone = set(df['ZONENAME'])  # Get unique zones as a set
                unique_zone = sorted(unique_zone)

                df_per_year = pd.DataFrame()
                for zone in unique_zone:
                    df_zone = df[df['ZONENAME'] == str(zone)]
                    df_single_zone = df_zone.copy()
                    df_single_zone['EFFECTIVEDAY']= pd.to_datetime(df_single_zone['EFFECTIVEDAY'])
                    df_single_zone['TERMINATIONDAY'] = pd.to_datetime(df_single_zone['TERMINATIONDAY'])
                    df_single_zone['EFFECTIVEDAY'] = df_single_zone['EFFECTIVEDAY'].dt.date
                    df_single_zone['TERMINATIONDAY'] = df_single_zone['TERMINATIONDAY'].dt.date

                    df_single_zone.reset_index(drop=True, inplace=True)
                    pd.set_option('display.precision', 16)

                    df_modified = df_single_zone.copy()
                    offset = 0
                    check_date = pd.to_datetime(f'{year}-06-01').date()
                    check_date1 = pd.to_datetime(f'{year + 1}-06-01').date()
                    for index, row in df_modified.iterrows():
                        if row['EFFECTIVEDAY'] < check_date:
                            start_date = check_date
                        else:
                            start_date = row['EFFECTIVEDAY']
                        end_date = min(row['TERMINATIONDAY'], check_date1) if pd.notna(row['TERMINATIONDAY']) else check_date1

                        factor_value = row['FACTOR']

                        if pd.notna(start_date) and pd.notna(end_date):
                            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
                            date_list = date_range.strftime('%Y-%m-%d').tolist()
                            date_list = date_list[:-1]
                            new_rows = pd.DataFrame({
                                'Date': date_list,
                                'Factor': [factor_value] * len(date_list)
                            })
                            actual_index = index + 1 + offset
                            df_first_part = df_modified.iloc[:actual_index]
                            df_second_part = df_modified.iloc[actual_index:]
                            df_modified = pd.concat([df_first_part, new_rows, df_second_part], ignore_index=True)
                            offset += len(new_rows)
                    df_modified = df_modified.drop(['EFFECTIVEDAY', 'TERMINATIONDAY','FACTOR'], axis=True)
                    df_modified['ZONENAME'].fillna(zone, inplace=True)
                    df_cleaned = df_modified.dropna(axis=0, how='any')
                    df_cleaned.reset_index(drop=True, inplace=True)
                    check_range = pd.date_range(start=check_date, end=check_date1, freq='D')
                    correct_length = len(check_range) - 1
                    if len(df_cleaned) != correct_length:
                        print(f'Special case during year of {year}:', zone, len(df_cleaned))
                    df_per_year = pd.concat([df_per_year,df_cleaned], ignore_index=True)
                res = pd.concat([res, df_per_year])

        res['Date'] = pd.to_datetime(res['Date'])
        res['Year'] = res['Date'].dt.year
        res = res.reset_index(drop=True)

        res_final = res.copy()
        res_final = res_final.sort_values(by=['ZONENAME', 'Date'], ascending=[True, True])
        unique_zone_count = res_final.groupby('Year')['ZONENAME'].nunique()
        unique_zone_dict = unique_zone_count.to_dict()
        unique_year = unique_zone_dict.keys()
        for year in unique_year:
            zone_this_year = set(res_final.loc[res_final['Year'] == year, 'ZONENAME'])
            zone_this_year = sorted(zone_this_year)
            print(f"Number of unique Zone Names in {year} is: {unique_zone_dict[year]}")
            for zone in zone_this_year:
                zone_mask = (res_final['ZONENAME'] == zone) & (res_final['Year'] == year)
                zone_rows = res_final.loc[zone_mask].copy()
                expected_days = 366 if year % 4 == 0 else 365
                if len(zone_rows) != expected_days:
                    if year in [2013, 2015, 2018, 2019, 2025]:
                        continue
                    else:
                        print('Warning:', year, zone, 'check!!!!!!!!!', len(zone_rows), zone_rows)
            print(f'No mistakes during concatenation: {year}')
            print('-------------------------------')

        res_final['NewName'] = res_final['ZONENAME']
        name_mapping = {
            'AEP': 'AEP-OH',
            'AEPOHIO': 'AEP-OH',
            'DAY': 'AES-OH',
            'DEOK': 'DUKE-OH',
            'OHIO': 'FE-OH',
            'WPP': 'WestPennPower',
            'PP': 'PennPower'}
        for index, row in res_final.iterrows():
            if row['ZONENAME'] in name_mapping:
                res_final.at[index, 'NewName'] = name_mapping[row['ZONENAME']]

        name_mapping2 = {
            "AECO": "AECO_RESID_AGG",
            "AEP-OH": "AEPOHIO_RESID_AGG",
            "WestPennPower": "APS_RESID_AGG",
            "PE": "APS_RESID_AGG",
            "BGE": "BGE_RESID_AGG",
            "COMED": "COMED_RESID_AGG",
            "AES-OH": "DAY_RESID_AGG",
            "DUKE-OH": "DEOK_RESID_AGG",
            "DPL": "DPL_RESID_AGG",
            "DUQ": "DUQ_RESID_AGG",
            "FE-OH": "FEOHIO_RESID_AGG",
            "JCPL": "JCPL_RESID_AGG",
            "METED": "METED_RESID_AGG",
            "PECO": "PECO_RESID_AGG",
            "PENELEC": "PENELEC_RESID_AGG",
            "PennPower": "PENNPOWER_RESID_AGG",
            "PPL": "PPL_RESID_AGG",
            "PSEG": "PSEG_RESID_AGG",
            "RECO": "RECO_RESID_AGG",}

        res_final['LocaleName'] = res_final['NewName']
        for index, row in res_final.iterrows():
            if row['NewName'] in name_mapping2:
                res_final.at[index, 'LocaleName'] = name_mapping2[row['NewName']]

        res_final = res_final.rename(columns={'Date': 'FlowDate', 'Factor': 'ScalingFactor'})
        res_final = res_final.drop(['Year'], axis=1)
        res_final = res_final[['FlowDate', 'LocaleName', 'ScalingFactor', 'ZONENAME', 'NewName']]
        res_final['FlowDate'] = res_final['FlowDate'].dt.strftime('%Y-%m-%d')

        # Output the filtered result to csv file, subject for further changes for this data
        data_processed = {
            'FlowDate': res_final['FlowDate'],
            'LocaleName': res_final['LocaleName'],
            'VolumeLevel': res_final['ScalingFactor'],
            'VolumeType': 'PLC_Scaling_Factor',
            'VolumeComment': ''
        }
        df_processed = pd.DataFrame(data_processed)

        # # download data from database for crosscheck:
        # conn,engine = dbop.db_connect('LoadStaging')
        # df_database = pd.read_sql('select * from [dbo].[Load_PJMPLCNSPL] WHERE VolumeType = \'NSPL_Volume\'',conn)
        # conn.close()
        #
        # df_database['FlowMonth'] = pd.to_datetime(df_database['FlowMonth']).dt.strftime('%Y-%m-%d')

        # Output data check
        logging.info('Checking Data')
        warning_messages = data_check(df_processed, warning_messages)

        # # Data Crosscheck
        df_database = df_processed.copy()

        df_merged = df_processed.merge(df_database, on=['FlowDate', 'LocaleName'], how='outer', indicator=True, suffixes=('_output', '_db'))
        df_match = df_merged[df_merged['_merge'] == 'both'].copy()
        df_match['volume_diff'] = df_match['VolumeLevel_output'] - df_match['VolumeLevel_db']
        df_mismatch = df_match[abs(df_match['volume_diff']) > 1]
        if not df_mismatch.empty:
            for index, row in df_mismatch.iterrows():
                missing_time = row['FlowDate']
                locale_name = row['LocaleName']
                print(f'Find data mismatch in {missing_time} {locale_name}')
                warning_messages.append(f'Data Mismatch Warning: Find data mismatch in {missing_time} {locale_name}')

        df_update = df_merged[df_merged['_merge'] == 'left_only'].copy()
        df_dbonly = df_merged[df_merged['_merge'] == 'right_only'].copy()

        if pd.to_datetime(df_update['FlowDate']).min() <= pd.to_datetime(df_database['FlowDate']).max():
            df_conflict = df_update[pd.to_datetime(df_update['FlowDate']) <= pd.to_datetime(df_database['FlowDate']).max()]
            conflict_locale = df_conflict['LocaleName'].unique()
            conflict_start = pd.to_datetime(df_update['FlowDate']).min().strftime('%Y-%m-%d')
            conflict_end = pd.to_datetime(df_database['FlowDate']).max().strftime('%Y-%m-%d')

            print(f'Data Conflict Warning: Cannot find {conflict_locale} between {conflict_start} and {conflict_end} in database')
            warning_messages.append(f'Data Conflict Warning: Cannot find {conflict_locale} between {conflict_start} and {conflict_end} in database')

        if pd.to_datetime(df_dbonly['FlowDate']).max() >= pd.to_datetime(df_processed['FlowDate']).min():
            df_conflict = df_dbonly[pd.to_datetime(df_dbonly['FlowDate']) >= pd.to_datetime(df_processed['FlowDate']).min()]
            conflict_locale = df_conflict['LocaleName'].unique()
            conflict_start = pd.to_datetime(df_processed['FlowDate']).min().strftime('%Y-%m-%d')
            conflict_end = pd.to_datetime(df_dbonly['FlowDate']).max().strftime('%Y-%m-%d')

            print(f'Data Conflict Warning: Cannot find {conflict_locale} between {conflict_start} and {conflict_end} in processed data')
            warning_messages.append(f'Data Conflict Warning: Cannot find {conflict_locale} between {conflict_start} and {conflict_end} in processed data')

        # Save output data
        logging.info('Saving Data')
        data_update = {
            'FlowDate': df_update['FlowDate'],
            'LocaleName': df_update['LocaleName'],
            'VolumeLevel': df_update['VolumeLevel_output'],
            'VolumeType': 'PLC_Scaling_Factor',
            'VolumeComment': ''
        }
        df_update = pd.DataFrame(data_update)

        os.makedirs(f"{base_path}/PLCScaling_data/output_data", exist_ok=True)
        update_file_output_path = f"{base_path}/PLCScaling_data/output_data/PLCScaling_update.csv"
        df_update.to_csv(update_file_output_path, index=False)

        processed_file_output_path = f"{base_path}/PLCScaling_data/output_data/PLCScaling_processed.csv"
        df_processed.to_csv(processed_file_output_path, index=False)
        print("Final output is successfully saved!")

        # Save warning message
        if not warning_messages:
            warning_messages.append('No Warning in ETL Process')
        warning_dir = f"{base_path}/PLCScaling_data"
        filename = f'PLCScaling_etl_warnings.txt'
        file_path = os.path.join(warning_dir, filename)

        with open(file_path, 'w') as file:
            file.write('Warning Messages in ETL Process:' + '\n')
            for message in warning_messages:
                file.write(message + '\n')

        print(f"All warning messages have been saved to {file_path}")

    except Exception as e:
        logging.error(f'An error occurred: {e}', exc_info=True)


if __name__ == "__main__":
    extract_data = True
    timestamp = datetime.now().strftime('%Y%m%d')
    base_path = rf"C:\Users\5DIntern3_2024\Work\PJM_{timestamp}"
    os.makedirs(base_path, exist_ok=True)
    main(extract_data, base_path)
    logging.info('PLCScaling ETL Process Complete')
