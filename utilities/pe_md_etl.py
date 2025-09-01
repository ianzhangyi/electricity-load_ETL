import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from jinja2 import Environment, FileSystemLoader
import base64
from datetime import datetime

def load_hourly_volume_data(file_paths_hourly):
    # Res 23
    hourly_df_23_res = pd.read_excel(file_paths_hourly['hourly_volume_23'], sheet_name='RESIDENTIAL', header=[0, 1])
    hourly_df_23_res.columns = ['_'.join(col).strip() for col in hourly_df_23_res.columns.values]

    column_mapping = {
        'res_shopping': 'SHOPPING',
        'res_nonshopping': 'NON-SHOPPING'
    }
    for column_name in column_mapping:
        target_column = [col for col in hourly_df_23_res.columns if col.startswith(column_mapping[column_name])]
        hourly_df_23_res[column_name] = hourly_df_23_res[target_column].sum(axis=1)
    hourly_df_23_res = hourly_df_23_res[['Unnamed: 0_level_0_DATE', 'res_shopping', 'res_nonshopping']].copy()
    hourly_df_23_res = hourly_df_23_res.rename(columns={'Unnamed: 0_level_0_DATE': 'Date'})

    # Non-Res 23
    hourly_df_23_non_res = pd.read_excel(file_paths_hourly['hourly_volume_23'], sheet_name='TYPE II, I', header=[0, 1, 2])
    hourly_df_23_non_res.columns = ['_'.join(col).strip() for col in hourly_df_23_non_res.columns.values]

    column_mapping = {
        'type1_shopping': 'TYPE I_SHOPPING',
        'type2_shopping': 'TYPE II_SHOPPING',
        'type1_nonshopping': 'TYPE I_NON-SHOPPING',
        'type2_nonshopping': 'TYPE II_NON-SHOPPING',
    }
    for column_name in column_mapping:
        target_column = [col for col in hourly_df_23_non_res.columns if col.startswith(column_mapping[column_name])]
        hourly_df_23_non_res[column_name] = hourly_df_23_non_res[target_column].sum(axis=1)

    hourly_df_23_non_res = hourly_df_23_non_res[['Unnamed: 0_level_0_Unnamed: 0_level_1_DATE', 'type1_shopping', 'type2_shopping', 'type1_nonshopping', 'type2_nonshopping']].copy()
    hourly_df_23_non_res = hourly_df_23_non_res.rename(columns={'Unnamed: 0_level_0_Unnamed: 0_level_1_DATE': 'Date'})

    hourly_df_23 = hourly_df_23_non_res.merge(hourly_df_23_res, left_on='Date', right_on='Date')

    # RES 19
    hourly_df_res_19 = pd.read_excel(file_paths_hourly[f'hourly_volume_res_19'], sheet_name='RES Data through 08-31-2022', header=2)
    hourly_df_res_19 = hourly_df_res_19.rename(columns={'All Elig': 'res_shopping', 'All Elig.1': 'res_nonshopping'})
    hourly_df_res_19 = hourly_df_res_19[['Date', 'HE_EPT', 'res_shopping', 'res_nonshopping']].copy()
    hourly_df_res_19 = hourly_df_res_19[hourly_df_res_19['Date'] < '2022-07-01'].copy()

    hourly_df_res_19_2 = pd.read_excel(file_paths_hourly[f'hourly_volume_res_19'], sheet_name='RES  ', header=2)
    hourly_df_res_19_2['res_shopping'] = hourly_df_res_19_2['PERSHT (AE and WWH)'] + hourly_df_res_19_2['PERSNH (WOWH)']
    hourly_df_res_19_2['res_nonshopping'] = hourly_df_res_19_2['PERSHT (AE and WWH).1'] + hourly_df_res_19_2['PERSNH (WOWH).1']
    hourly_df_res_19_2 = hourly_df_res_19_2[['Date', 'HE_EPT', 'res_shopping', 'res_nonshopping']].copy()

    hourly_df_res_19 = pd.concat([hourly_df_res_19, hourly_df_res_19_2], ignore_index=True)
    hourly_df_res_19['Date'] = pd.to_datetime(hourly_df_res_19['Date']).dt.strftime('%Y-%m-%d')

    # Type1 19
    hourly_df_type1_19 = pd.read_excel(file_paths_hourly[f'hourly_volume_type1_19'], sheet_name='TYPE_I_By_Rate_Schedule', header=2)
    hourly_df_type1_19 = hourly_df_type1_19.drop(index=0)
    hourly_df_type1_19 = hourly_df_type1_19.rename(columns={'Unnamed: 0': 'Date', 'Unnamed: 1': 'HE_EPT'})
    hourly_df_type1_19['type1_shopping'] = hourly_df_type1_19['CA_CSH'] + hourly_df_type1_19['C_G_HF'] + hourly_df_type1_19['C'] + hourly_df_type1_19['G']
    hourly_df_type1_19['type1_nonshopping'] = hourly_df_type1_19['CA_CSH.1'] + hourly_df_type1_19['C_G_HF.1'] + hourly_df_type1_19['C.1'] + hourly_df_type1_19['G.1']
    hourly_df_type1_19 = hourly_df_type1_19[['Date', 'HE_EPT', 'type1_shopping', 'type1_nonshopping']].copy()

    hourly_df_type1_19_2 = pd.read_excel(file_paths_hourly[f'hourly_volume_type1_19'], sheet_name='TypeI_Total through 8-31-2022', header=2)
    hourly_df_type1_19_2['type1_shopping'] = hourly_df_type1_19_2['TOTAL']
    hourly_df_type1_19_2['type1_nonshopping'] = hourly_df_type1_19_2['TOTAL.1']
    hourly_df_type1_19_2 = hourly_df_type1_19_2[['Date', 'HE_EPT', 'type1_shopping', 'type1_nonshopping']].copy()
    hourly_df_type1_19_2 = hourly_df_type1_19_2[hourly_df_type1_19_2['Date']<'2022-07-01'].copy()
    hourly_df_type1_19 = pd.concat([hourly_df_type1_19_2, hourly_df_type1_19])
    hourly_df_type1_19['Date'] = pd.to_datetime(hourly_df_type1_19['Date']).dt.strftime('%Y-%m-%d')

    # Type2 19
    hourly_df_type2_19 = pd.read_excel(file_paths_hourly[f'hourly_volume_type2_19'], sheet_name='Type_II_By_Rate_Schedule', header=2)
    hourly_df_type2_19 = hourly_df_type2_19.drop(index=0)
    hourly_df_type2_19 = hourly_df_type2_19.rename(columns={'Unnamed: 0': 'Date', 'Unnamed: 1': 'HE_EPT'})
    hourly_df_type2_19['type2_shopping'] = hourly_df_type2_19['CA_CSH'] + hourly_df_type2_19['C'] + hourly_df_type2_19['G'] + hourly_df_type2_19['PH']
    hourly_df_type2_19['type2_nonshopping'] = hourly_df_type2_19['CA_CSH.1'] + hourly_df_type2_19['C.1'] + hourly_df_type2_19['G.1'] + hourly_df_type2_19['PH.1']
    hourly_df_type2_19 = hourly_df_type2_19[['Date', 'HE_EPT', 'type2_shopping', 'type2_nonshopping']].copy()

    hourly_df_type2_19_2 = pd.read_excel(file_paths_hourly[f'hourly_volume_type2_19'], sheet_name='TypeII_Total through 8-31-2022', header=2)
    hourly_df_type2_19_2['type2_shopping'] = hourly_df_type2_19_2['TOTAL']
    hourly_df_type2_19_2['type2_nonshopping'] = hourly_df_type2_19_2['TOTAL.1']
    hourly_df_type2_19_2 = hourly_df_type2_19_2[['Date', 'HE_EPT', 'type2_shopping', 'type2_nonshopping']].copy()
    hourly_df_type2_19_2 = hourly_df_type2_19_2[hourly_df_type2_19_2['Date'] < '2022-07-01'].copy()
    hourly_df_type2_19 = pd.concat([hourly_df_type2_19_2, hourly_df_type2_19])
    hourly_df_type2_19['Date'] = pd.to_datetime(hourly_df_type2_19['Date']).dt.strftime('%Y-%m-%d')

    hourly_df_19 = hourly_df_res_19.merge(hourly_df_type1_19, left_on=['Date', 'HE_EPT'], right_on=['Date', 'HE_EPT'])
    hourly_df_19 = hourly_df_19.merge(hourly_df_type2_19, left_on=['Date', 'HE_EPT'], right_on=['Date', 'HE_EPT'])

    # hourly volume 10
    hourly_df_list_10 = []
    for customer_class in ['res', 'type1', 'type2']:
        for volume_type in ['sos', 'eli']:

            df = pd.read_excel(file_paths_hourly[f'hourly_volume_{customer_class}_{volume_type}_10'], header=0)
            df.columns.values[1] = 'Date'
            column_list = [f'{hour:02d}00' for hour in range(1, 25)]
            column_list.append('NOV DST')
            df_list = []
            for column_name in column_list:
                sub_df = df[['Date', column_name]].copy()
                sub_df['HE_EPT'] = column_name
                sub_df = sub_df.rename(columns={column_name: 'Volume'})
                sub_df['volume_type'] = f'{volume_type}'
                sub_df['customer_type'] = f'{customer_class}'
                df_list.append(sub_df)
            df = pd.concat(df_list, ignore_index=True)
            hourly_df_list_10.append(df)
    hourly_df_10 = pd.concat(hourly_df_list_10, ignore_index=True)

    return hourly_df_23, hourly_df_19, hourly_df_10


def load_monthly_volume_data(file_paths_monthly):
    # Load monthly data after 2023
    monthly_df_list=[]
    for sheet_name in ['Residential', 'Type I', 'Type II']:
        monthly_sheet_df = pd.read_excel(file_paths_monthly[f'customer_count_23'], sheet_name=sheet_name)
        monthly_df_list.append(monthly_sheet_df)
    monthly_df_23 = pd.concat(monthly_df_list, ignore_index=True)

    # Load monthly type1, type2 data after 2013
    df_monthly_list_13 = []
    for volume_type in ['eli', 'sos']:
        for customer_class in ['type1', 'type2']:
            for year in range(2013, 2024):
                xls = pd.ExcelFile(file_paths_monthly[f'customer_count_{customer_class}_{volume_type}_13'])
                sheets = [sheet for sheet in xls.sheet_names if sheet[:4] == f'{year}']
                df = pd.read_excel(xls, sheet_name=sheets[0], header=0)
                for month in range(1,13):
                    if volume_type == 'sos':
                        Eligible_MonthlyVolume = [0]
                        Default_MonthlyVolume = [df.iloc[-1, month]]
                    else:
                        Eligible_MonthlyVolume = [df.iloc[-1, month]]
                        Default_MonthlyVolume = [0]

                    df_monthly_data = pd.DataFrame({'FlowMonth': [f'{year}-{month}-01'],
                                                    'CustomerClass': customer_class,
                                                    'Default_MonthlyVolume': Default_MonthlyVolume,
                                                    'Eligible_MonthlyVolume': Eligible_MonthlyVolume})
                    df_monthly_list_13.append(df_monthly_data)
    df_monthly_13 = pd.concat(df_monthly_list_13, ignore_index=True)
    df_monthly_13 = df_monthly_13.groupby(['FlowMonth', 'CustomerClass']).sum().reset_index()

    # Load monthly res data after 2013
    df_monthly_list_13_res = []
    for volume_type in ['eli', 'sos']:
        df_monthly_res_13 = pd.read_excel(file_paths_monthly[f'customer_count_res_{volume_type}_13'])
        df_monthly_res_13 = df_monthly_res_13[df_monthly_res_13.iloc[:,0] =='TOTAL '].copy().reset_index(drop=True)
        for year in range(0,len(df_monthly_res_13)):
            for month in range(1,13):
                if volume_type == 'sos':
                    Eligible_MonthlyVolume = [0]
                    Default_MonthlyVolume = [df_monthly_res_13.iloc[year,month]]
                else:
                    Eligible_MonthlyVolume = [df_monthly_res_13.iloc[year,month]]
                    Default_MonthlyVolume = [0]

                data_res_13 = {
                    'FlowMonth': [f'{year+2013}-{month}-01'],
                    'CustomerClass': 'RES',
                    'Default_MonthlyVolume': Default_MonthlyVolume,
                    'Eligible_MonthlyVolume': Eligible_MonthlyVolume
                }
                df_monthly_list_13_res.append(pd.DataFrame(data_res_13))
    df_monthly_13_res = pd.concat(df_monthly_list_13_res, ignore_index=True)
    df_monthly_13_res = df_monthly_13_res.groupby(['FlowMonth', 'CustomerClass']).sum().reset_index()

    monthly_df_13 = pd.concat([df_monthly_13, df_monthly_13_res], ignore_index=True)
    monthly_df_13['FlowMonth'] = pd.to_datetime(monthly_df_13['FlowMonth'])

    return monthly_df_23, monthly_df_13


def load_daily_volume_data(file_path_daily):
    daily_volume_list = []
    # Load data 2013-2022
    for data_type in ['PLC', 'NSPL']:
        df_13 = pd.read_excel(file_path_daily, sheet_name=f'{data_type} 2013-2022', header=5)
        df_13 = df_13.drop(index=0)
        df_13['Unnamed: 0'] = pd.to_datetime(df_13['Unnamed: 0']).dt.strftime('%Y-%m-%d')
        for column_name in ['Residential', 'Type 1', 'Type 2']:
            if column_name == 'Residential':
                CustomerClass = 'RES'
            elif column_name == 'Type 1':
                CustomerClass = 'Type 1 Non-RES'
            else:
                CustomerClass = 'Type 2 Non-RES'

            data_13 = {
                'FlowDate': df_13['Unnamed: 0'],
                'CustomerClass': CustomerClass,
                'VolumeType': f'{data_type}_Unscaled',
                'EGS_DailyVolume': df_13[f'{column_name}.1'],
                'Default_DailyVolume': df_13[column_name],
                'Eligible_DailyVolume': 0
            }
            daily_volume_list.append(pd.DataFrame(data_13))

    # Load data 2023 onwards
    for data_type in ['PLC', 'NSPL']:
        df_23 = pd.read_excel(file_path_daily, sheet_name=f'{data_type} 2023+', header=1)
        df_23['Unnamed: 0'] = pd.to_datetime(df_23['Unnamed: 0']).dt.strftime('%Y-%m-%d')
        for column_name in ['RESIDENTIAL', 'TYPE I', 'TYPE II']:
            if column_name == 'RESIDENTIAL':
                CustomerClass = 'RES'
            elif column_name == 'TYPE I':
                CustomerClass = 'Type 1 Non-RES'
            else:
                CustomerClass = 'Type 2 Non-RES'

            data_23 = {
                'FlowDate': df_23['Unnamed: 0'],
                'CustomerClass': CustomerClass,
                'VolumeType': f'{data_type}_Unscaled',
                'EGS_DailyVolume': df_23[f'{column_name}.1'].astype(float)/1000,
                'Default_DailyVolume': df_23[column_name].astype(float)/1000,
                'Eligible_DailyVolume': 0
            }
            daily_volume_list.append(pd.DataFrame(data_23))

    daily_volume_df = pd.concat(daily_volume_list, ignore_index=True)

    return daily_volume_df


def load_ufe_data(ufe_file_path):
    ufe_df_19 = pd.read_excel(ufe_file_path, sheet_name='2019-2022', header=3)
    df_19 = pd.DataFrame({
        'Date': pd.to_datetime(ufe_df_19['Unnamed: 0']),
        'Hour': ufe_df_19['HR'],
        'ufe_factor': ufe_df_19['UFE Factor']
    })

    ufe_df_23 = pd.read_excel(ufe_file_path, sheet_name='2023+', header=3)
    ufe_df_23[['date', 'hour']] = ufe_df_23['Date'].str.split(' ', expand=True)
    ufe_df_23['hour'] = ufe_df_23['hour'].str.split(':').str[0].astype(int)

    def parse_date(date_str):
        """Attempt to parse a date string using multiple patterns."""
        try:
            # First pattern: %m/%d/%Y
            return pd.to_datetime(date_str, format="%m/%d/%Y")
        except ValueError:
            pass

        try:
            # Second pattern: %Y-%m-%d
            return pd.to_datetime(date_str, format="%Y-%m-%d")
        except ValueError:
            pass

        raise ValueError(f"Date format not recognized: {date_str}")

    ufe_df_23['date_converted'] = ufe_df_23['date'].apply(parse_date)

    df_23 = pd.DataFrame({
        'Date': ufe_df_23['date_converted'],
        'Hour': ufe_df_23['hour'],
        'ufe_factor': ufe_df_23['UFE Factor']
    })

    ufe_df = pd.concat([df_19, df_23], ignore_index=True)
    return ufe_df


def process_ufe_data(hourly_volume_processed, ufe_df, edc_name):
    ufe_df_processed = ufe_df.copy()
    ufe_df_processed['Hour'] = ufe_df_processed['Hour'] - 1
    indices_24 = ufe_df_processed[ufe_df_processed['Hour'] == 24].index
    for index_24 in indices_24:
        if index_24 >= 22:
            ufe_df_processed.loc[index_24 - 22:index_24, 'Hour'] -= 1

    # Handle wrong datetime pattern on '2023-11-05', '2023-03-12', '2024-03-10'
    target_index = ufe_df_processed[(ufe_df_processed['Date'] == '2023-11-05')&(ufe_df_processed['Hour'] == 0)].index[1]
    ufe_df_processed.loc[target_index, 'Hour'] = 1

    ufe_df_processed.loc[(ufe_df_processed['Date'] == '2023-03-12') & (ufe_df_processed['Hour'] == 2), 'Hour'] = 1
    ufe_df_processed.loc[(ufe_df_processed['Date'] == '2024-03-10') & (ufe_df_processed['Hour'] == 2), 'Hour'] = 1


    ufe_df_processed["Datetime_beginning_ept"] = ufe_df_processed["Date"] + pd.to_timedelta(ufe_df_processed["Hour"], unit="h")
    ufe_df_processed["Datetime_beginning_ept"] = ufe_df_processed["Datetime_beginning_ept"].dt.tz_localize(tz='America/New_York', ambiguous='infer')
    ufe_df_processed["Datetime_beginning_utc"] = ufe_df_processed["Datetime_beginning_ept"].dt.tz_convert("UTC")

    ufe_df_merged = ufe_df_processed.merge(hourly_volume_processed, left_on=['Datetime_beginning_utc'], right_on=['Datetime_beginning_utc'])

    for column_name in ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']:
        ufe_df_merged[column_name] = ufe_df_merged[column_name] * (1 - 1/ufe_df_merged['ufe_factor'])

    ufe_processed = pd.DataFrame({
        'Datetime_beginning_utc': ufe_df_merged['Datetime_beginning_utc'],
        'EDCName': edc_name,
        'CustomerClass': ufe_df_merged['CustomerClass'],
        'VolumeType': 'UFE_volume',
        'EGS_HourlyVolume': ufe_df_merged['EGS_HourlyVolume'],
        'Default_HourlyVolume': ufe_df_merged['Default_HourlyVolume'],
        'Eligible_HourlyVolume': ufe_df_merged['Eligible_HourlyVolume'],
        'VolumeComment': ''
    })
    return ufe_processed


def process_hourly_volume_data(hourly_df_23, hourly_df_19, hourly_df_10, edc_name):
    # Process Hourly Volume 23
    df_23 = hourly_df_23.copy()
    df_23['datetime'] = df_23['Date'].str.split(' ').str[0]
    df_23['hour'] = df_23['Date'].str.split(' ').str[1]
    df_23['hour'] = df_23['hour'].str.split(':').str[0].astype(int)
    # Transfer to beginning hours
    df_23['hour'] = df_23['hour'] - 1

    def parse_date(date_str):
        try:
            return pd.to_datetime(date_str, format='%m/%d/%Y')
        except ValueError:
            return pd.to_datetime(date_str, format='%Y-%m-%d')

    df_23["datetime"] = df_23["datetime"].apply(parse_date)

    # Change the hourly info to match the EPT pattern
    df_23.loc[df_23['Date']=='2023-11-05 01::00s', 'hour'] = 1
    df_23.loc[df_23['Date']=='2023-03-12 03:00d', 'hour'] = 1
    df_23.loc[df_23['Date'] == '2024-03-10 03:00d', 'hour'] = 1

    datetime_beginning_ept = df_23["datetime"] + pd.to_timedelta(df_23["hour"], unit="h")
    df_23["Datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York', ambiguous='infer')
    df_23["Datetime_beginning_utc"] = df_23["Datetime_beginning_ept"].dt.tz_convert("UTC")

    customer_class_map = {
        'res': 'RES',
        'type1': 'Type 1 Non-RES',
        'type2': 'Type 2 Non-RES'
    }
    hourly_list_23 = []
    for customer_class in ['res', 'type1', 'type2']:
        date_23 = {
            'Datetime_beginning_utc': df_23['Datetime_beginning_utc'],
            'EDCName': edc_name,
            'CustomerClass': customer_class_map[customer_class],
            'VolumeType': 'Retail_Premise',
            'EGS_HourlyVolume': (df_23[f'{customer_class}_shopping']/1000).astype(float).round(3),
            'Default_HourlyVolume': (df_23[f'{customer_class}_nonshopping']/1000).astype(float).round(3),
            'Eligible_HourlyVolume': 0,
            'VolumeComment': ''
        }
        hourly_list_23.append(pd.DataFrame(date_23))
    hourly_df_processed_23 = pd.concat(hourly_list_23, ignore_index=True)
    hourly_df_processed_23['Eligible_HourlyVolume'] = hourly_df_processed_23['EGS_HourlyVolume'] + hourly_df_processed_23['Default_HourlyVolume']

    # Process Hourly Volume 19
    df_19 = hourly_df_19.copy()
    df_19['Hour'] = df_19['HE_EPT'].astype(int) - 1

    indices_24 = df_19[df_19['Hour'] == 24].index
    for index_24 in indices_24:
        if index_24 >= 22:
            df_19.loc[index_24 - 22:index_24, 'Hour'] -= 1

    datetime_beginning_ept = pd.to_datetime(df_19["Date"]) + pd.to_timedelta(df_19["Hour"], unit="h")
    df_19["Datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York', ambiguous='infer')
    df_19["Datetime_beginning_utc"] = df_19["Datetime_beginning_ept"].dt.tz_convert("UTC")

    hourly_list_19 = []
    for customer_class in ['res', 'type1', 'type2']:
        date_19 = {
            'Datetime_beginning_utc': df_19['Datetime_beginning_utc'],
            'EDCName': edc_name,
            'CustomerClass': customer_class_map[customer_class],
            'VolumeType': 'Retail_Premise',
            'EGS_HourlyVolume': (df_19[f'{customer_class}_shopping']/1000).astype(float).round(3),
            'Default_HourlyVolume': (df_19[f'{customer_class}_nonshopping']/1000).astype(float).round(3),
            'Eligible_HourlyVolume': 0,
            'VolumeComment': ''
        }
        hourly_list_19.append(pd.DataFrame(date_19))
    hourly_df_processed_19 = pd.concat(hourly_list_19, ignore_index=True)
    hourly_df_processed_19['Eligible_HourlyVolume'] = hourly_df_processed_19['EGS_HourlyVolume'] + hourly_df_processed_19['Default_HourlyVolume']

    # Process Hourly Volume 10
    df_10 = hourly_df_10.copy()
    df_10 = df_10.dropna(subset=['Volume'])
    df_10 = df_10[~((df_10['Volume']==0) & (df_10['HE_EPT']=='NOV DST'))].copy()
    # Interpolate missing dates
    df_10['Date'] = pd.to_datetime(df_10['Date'], errors='coerce')
    df_10.loc[df_10['Date'].isna(),'Date'] = pd.to_datetime('2/23/2016')

    df_10_sos = df_10[df_10['volume_type']=='sos'].copy()
    df_10_eli = df_10[df_10['volume_type']=='eli'].copy()

    df_10_merged = df_10_sos.merge(df_10_eli, left_on=['Date', 'HE_EPT', 'customer_type'], right_on=['Date', 'HE_EPT', 'customer_type'], suffixes=('_sos', '_eli'))

    # Convert to EPT time pattern
    df_10_merged.loc[df_10_merged['HE_EPT']=='NOV DST', 'HE_EPT'] = '0200'
    df_10_merged['Hour'] = df_10_merged['HE_EPT'].str[:2].astype(int) - 1
    # Drop all daylight saving hours in March
    df_10_merged = df_10_merged[df_10_merged['Volume_sos']!=0].copy()

    hourly_list_10 = []
    for customer_class in df_10_merged['customer_type'].unique():

        sub_df = df_10_merged[(df_10_merged['customer_type'] == customer_class)].copy()
        sub_df = sub_df.sort_values(by=['Date', 'Hour'], ignore_index=True)
        datetime_beginning_ept = pd.to_datetime(sub_df['Date']) + pd.to_timedelta(sub_df["Hour"], unit="h")
        sub_df["Datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York', ambiguous='infer')
        sub_df["Datetime_beginning_utc"] = sub_df["Datetime_beginning_ept"].dt.tz_convert("UTC")

        data_10 = {
            'Datetime_beginning_utc': sub_df['Datetime_beginning_utc'],
            'EDCName': edc_name,
            'CustomerClass': customer_class_map[customer_class],
            'VolumeType': 'Retail_Premise',
            'EGS_HourlyVolume': 0,
            'Default_HourlyVolume': (sub_df[f'Volume_sos']/1000).astype(float).round(3),
            'Eligible_HourlyVolume': (sub_df[f'Volume_eli']/1000).astype(float).round(3),
            'VolumeComment': ''
        }

        hourly_list_10.append(pd.DataFrame(data_10))
    hourly_df_processed_10 = pd.concat(hourly_list_10, ignore_index=True)
    hourly_df_processed_10['EGS_HourlyVolume'] = hourly_df_processed_10['Eligible_HourlyVolume'] - hourly_df_processed_10['Default_HourlyVolume']

    # Combine and Sort processed hourly volume df
    hourly_volume_processed = pd.concat([hourly_df_processed_10, hourly_df_processed_19, hourly_df_processed_23], ignore_index=True)
    hourly_volume_processed = hourly_volume_processed.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'], ignore_index=True)

    # Cut off data prior to 2012 for data alignment
    hourly_volume_processed = hourly_volume_processed[hourly_volume_processed['Datetime_beginning_utc']>='2012-01-01 05:00:00+0000'].copy()
    return hourly_volume_processed


def derate_hourly_volume(hourly_volume_processed, deration_df):

    deration_df['Datetime_beginning_utc'] = pd.to_datetime(deration_df['Datetime_beginning_utc'])
    df_merged = hourly_volume_processed.merge(deration_df, left_on='Datetime_beginning_utc', right_on='Datetime_beginning_utc')

    for column_name in ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']:
        df_merged.loc[:,column_name] = df_merged.loc[:,column_name] * (1-df_merged['DerationFactor'])

    hourly_volume_processed = df_merged[['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType',
       'EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume',
       'VolumeComment']].copy()

    return hourly_volume_processed


def process_monthly_volume_data(monthly_df_23, monthly_df_13, edc_name):
    monthly_df_list = []
    # Process monthly volume after 2023
    monthly_df_23_processed = monthly_df_23.copy()
    monthly_df_23_processed['YearMonth'] = pd.to_datetime(monthly_df_23_processed['YearMonth']).dt.strftime('%Y-%m-01')
    monthly_df_23_processed = monthly_df_23_processed[['YearMonth', 'Type', 'All Eligible', 'Non-Shopping']].copy()
    monthly_df_23_processed[['All Eligible', 'Non-Shopping']] = monthly_df_23_processed[['All Eligible', 'Non-Shopping']].astype(str)
    monthly_df_23_processed['All Eligible'] = monthly_df_23_processed['All Eligible'].str.replace(',', '').astype(int)
    monthly_df_23_processed['Non-Shopping'] = monthly_df_23_processed['Non-Shopping'].str.replace(',', '').astype(int)

    monthly_df_23_processed = monthly_df_23_processed.groupby(['YearMonth', 'Type']).sum().reset_index()
    monthly_map_23 = {
        'Residential': 'RES',
        'Type I': 'Type 1 Non-RES',
        'Type II': 'Type 2 Non-RES'
    }
    monthly_df_23_processed['Type'] = monthly_df_23_processed['Type'].map(monthly_map_23)
    monthly_data_23 = {
        'FlowMonth': monthly_df_23_processed['YearMonth'],
        'EDCName': edc_name,
        'CustomerClass': monthly_df_23_processed['Type'],
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': 0,
        'Default_MonthlyVolume': monthly_df_23_processed['Non-Shopping'],
        'Eligible_MonthlyVolume': monthly_df_23_processed['All Eligible'],
        'VolumeComment': ''
    }
    monthly_df_list.append(pd.DataFrame(monthly_data_23))

    # Process monthly volume after 2013
    monthly_df_13_processed = monthly_df_13.copy()
    monthly_df_13_processed = monthly_df_13_processed[monthly_df_13_processed['Eligible_MonthlyVolume']!=0].copy()
    monthly_map_13 = {
        'RES': 'RES',
        'type1': 'Type 1 Non-RES',
        'type2': 'Type 2 Non-RES'
    }
    monthly_df_13_processed['CustomerClass'] = monthly_df_13_processed['CustomerClass'].map(monthly_map_13)

    monthly_data_13 = {
        'FlowMonth': pd.to_datetime(monthly_df_13_processed['FlowMonth']).dt.strftime('%Y-%m-01'),
        'EDCName': edc_name,
        'CustomerClass': monthly_df_13_processed['CustomerClass'],
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': 0,
        'Default_MonthlyVolume': monthly_df_13_processed['Default_MonthlyVolume'],
        'Eligible_MonthlyVolume': monthly_df_13_processed['Eligible_MonthlyVolume'],
        'VolumeComment': ''
    }
    monthly_df_list.append(pd.DataFrame(monthly_data_13))

    # Combine and sort df for output
    monthly_volume_processed = pd.concat(monthly_df_list, ignore_index=True)
    monthly_volume_processed['EGS_MonthlyVolume'] = monthly_volume_processed['Eligible_MonthlyVolume'] - monthly_volume_processed['Default_MonthlyVolume']
    monthly_volume_processed = monthly_volume_processed.sort_values(by=['FlowMonth', 'CustomerClass'], ignore_index=True)

    return monthly_volume_processed


def process_daily_volume_data(daily_volume_df, edc_name):
    daily_df = daily_volume_df.copy()
    daily_df[['EGS_DailyVolume', 'Default_DailyVolume']] = daily_df[['EGS_DailyVolume', 'Default_DailyVolume']].astype(float)

    daily_data = {
        'FlowDate': daily_df['FlowDate'],
        'EDCName': edc_name,
        'CustomerClass': daily_df['CustomerClass'],
        'VolumeType': daily_df['VolumeType'],
        'EGS_DailyVolume': daily_df['EGS_DailyVolume'].round(3),
        'Default_DailyVolume': daily_df['Default_DailyVolume'].round(3),
        'Eligible_DailyVolume': daily_df['EGS_DailyVolume'].round(3)+daily_df['Default_DailyVolume'].round(3),
        'VolumeComment': ''
    }

    daily_volume_processed = pd.DataFrame(daily_data)
    daily_volume_processed = daily_volume_processed.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'], ignore_index=True)

    return daily_volume_processed


def handle_missing_data(daily_volume_processed, edc_name):
    daily_volume_processed = daily_volume_processed.copy()
    missing_data_list = []
    for customer_class in ['RES', 'Type 1 Non-RES', 'Type 2 Non-RES']:
        for volume_type in ['PLC_Unscaled', 'NSPL_Unscaled']:
            # Handle missing on 2023-03-12
            missing_EGS_DailyVolume = daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == '2023-03-11')&
                                                                 (daily_volume_processed['CustomerClass'] == customer_class)&
                                                                 (daily_volume_processed['VolumeType'] == volume_type),'EGS_DailyVolume'].values/2 + \
                                      daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == '2023-03-13')&
                                                                 (daily_volume_processed['CustomerClass'] == customer_class)&
                                                                 (daily_volume_processed['VolumeType'] == volume_type),'EGS_DailyVolume'].values/2

            missing_Default_DailyVolume = daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == '2023-03-11')&
                                                                 (daily_volume_processed['CustomerClass'] == customer_class)&
                                                                 (daily_volume_processed['VolumeType'] == volume_type),'Default_DailyVolume'].values/2 + \
                                      daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == '2023-03-13')&
                                                                 (daily_volume_processed['CustomerClass'] == customer_class)&
                                                                 (daily_volume_processed['VolumeType'] == volume_type),'Default_DailyVolume'].values/2

            missing_data = {
                'FlowDate': pd.to_datetime('2023-03-12').strftime('%Y-%m-%d'),
                'EDCName': edc_name,
                'CustomerClass': customer_class,
                'VolumeType': volume_type,
                'EGS_DailyVolume': missing_EGS_DailyVolume.round(3),
                'Default_DailyVolume': missing_Default_DailyVolume.round(3),
                'Eligible_DailyVolume': missing_EGS_DailyVolume.round(3) + missing_Default_DailyVolume.round(3),
                'VolumeComment': ''
            }
            missing_data_list.append(pd.DataFrame(missing_data))
            # Handle missing on 2024-03-10
            missing_EGS_DailyVolume = daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == '2024-03-09')&
                                                                 (daily_volume_processed['CustomerClass'] == customer_class)&
                                                                 (daily_volume_processed['VolumeType'] == volume_type),'EGS_DailyVolume'].values/2 + \
                                      daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == '2024-03-11')&
                                                                 (daily_volume_processed['CustomerClass'] == customer_class)&
                                                                 (daily_volume_processed['VolumeType'] == volume_type),'EGS_DailyVolume'].values/2

            missing_Default_DailyVolume = daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == '2024-03-09')&
                                                                 (daily_volume_processed['CustomerClass'] == customer_class)&
                                                                 (daily_volume_processed['VolumeType'] == volume_type),'Default_DailyVolume'].values/2 + \
                                      daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == '2024-03-11')&
                                                                 (daily_volume_processed['CustomerClass'] == customer_class)&
                                                                 (daily_volume_processed['VolumeType'] == volume_type),'Default_DailyVolume'].values/2

            missing_data = {
                'FlowDate': pd.to_datetime('2024-03-10').strftime('%Y-%m-%d'),
                'EDCName': edc_name,
                'CustomerClass': customer_class,
                'VolumeType': volume_type,
                'EGS_DailyVolume': missing_EGS_DailyVolume.round(3),
                'Default_DailyVolume': missing_Default_DailyVolume.round(3),
                'Eligible_DailyVolume': missing_EGS_DailyVolume.round(3) + missing_Default_DailyVolume.round(3),
                'VolumeComment': ''
            }
            missing_data_list.append(pd.DataFrame(missing_data))
    missing_df = pd.concat(missing_data_list, ignore_index=True)
    daily_volume_processed = pd.concat([missing_df, daily_volume_processed], ignore_index=True)
    daily_volume_processed = daily_volume_processed.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'],
                                                                ignore_index=True)
    return daily_volume_processed


def plot_monthly_data(df, output_dir, edc_name):
    data = df.copy()
    data['Difference between Eligible and sum of EGS and Default'] = (data['Eligible_MonthlyVolume'] - data['Default_MonthlyVolume'] - data['EGS_MonthlyVolume']).round(3)
    plot_path = {}
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    columns_to_plot = ['EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume',
                       'Difference between Eligible and sum of EGS and Default']
    customer_classes = data['CustomerClass'].unique()

    # Set larger font size for all text elements
    plt.rcParams.update({'font.size': 14})

    for customer_class in customer_classes:
        fig, axs = plt.subplots(len(columns_to_plot), 1, figsize=(20, 20), sharex=False)

        class_data = data[data['CustomerClass'] == customer_class]

        for col_idx, column in enumerate(columns_to_plot):
            axs[col_idx].plot(pd.to_datetime(class_data['FlowMonth']), class_data[column])
            axs[col_idx].set_title(f'{column} - {customer_class}', fontsize=18)
            axs[col_idx].set_ylabel('Customer Counts', fontsize=16)

            axs[col_idx].xaxis.set_major_locator(mdates.MonthLocator(bymonth=6))
            axs[col_idx].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            axs[col_idx].xaxis.set_visible(True)

            if col_idx == len(columns_to_plot) - 1:
                axs[col_idx].set_xlabel('FlowMonth', fontsize=16)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/{edc_name}_CustomerCounts_{customer_class}_plot.png')
        plot_path[f'monthly_{customer_class}'] = f'{output_dir}/{edc_name}_CustomerCounts_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_hourly_data(df, output_dir, edc_name):
    data = df.copy()
    data['Difference between Eligible and sum of EGS and Default'] = (data['Eligible_HourlyVolume']
                                                                      - data['Default_HourlyVolume']
                                                                      - data['EGS_HourlyVolume']).round(3)
    plot_path = {}
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    columns_to_plot = ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume',
                       'Difference between Eligible and sum of EGS and Default']
    customer_classes = data['CustomerClass'].unique()

    # Set larger font size for all text elements
    plt.rcParams.update({'font.size': 14})

    for volume_type in df['VolumeType'].unique():
        for customer_class in customer_classes:
            fig, axs = plt.subplots(len(columns_to_plot), 1, figsize=(20, 20), sharex=False)

            class_data = data[data['CustomerClass'] == customer_class]

            for col_idx, column in enumerate(columns_to_plot):
                axs[col_idx].plot(pd.to_datetime(class_data['Datetime_beginning_utc']), class_data[column])
                axs[col_idx].set_title(f'{column} - {customer_class}', fontsize=18)
                axs[col_idx].set_ylabel('Hourly Volume', fontsize=16)

                axs[col_idx].xaxis.set_major_locator(mdates.MonthLocator(bymonth=6))
                axs[col_idx].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
                axs[col_idx].xaxis.set_visible(True)

                if col_idx == len(columns_to_plot) - 1:
                    axs[col_idx].set_xlabel('Datetime_beginning_utc', fontsize=16)

            plt.tight_layout()
            plt.savefig(f'{output_dir}/{edc_name}_HourlyLoad_{customer_class}_{volume_type}_plot.png')
            plot_path[f'hourly_{customer_class}_{volume_type}'] = f'{output_dir}/{edc_name}_HourlyLoad_{customer_class}_{volume_type}_plot.png'
            plt.close(fig)

    return plot_path


def plot_daily_data(df, output_dir, edc_name):
    data = df.copy()
    data['Difference Check'] = (data['Eligible_DailyVolume'] - data['Default_DailyVolume'] - data['EGS_DailyVolume']).round(3)

    plot_path = {}
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    columns_to_plot = ['EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume', 'Difference Check']
    customer_classes = data['CustomerClass'].unique()
    volume_types = data['VolumeType'].unique()

    # Set larger font size for all text elements
    plt.rcParams.update({'font.size': 14})

    for customer_class in customer_classes:
        fig, axs = plt.subplots(len(columns_to_plot), 1, figsize=(20, 20), sharex=False)

        class_data = data[data['CustomerClass'] == customer_class]

        for col_idx, column in enumerate(columns_to_plot):
            for volume_type in volume_types:
                type_data = class_data[class_data['VolumeType'] == volume_type]
                axs[col_idx].plot(pd.to_datetime(type_data['FlowDate']), type_data[column], label=volume_type)

            axs[col_idx].set_title(f'{column} - {customer_class}', fontsize=18)
            axs[col_idx].set_ylabel('Volume (MW)', fontsize=16)

            axs[col_idx].xaxis.set_major_locator(mdates.MonthLocator(bymonth=6))
            axs[col_idx].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            axs[col_idx].xaxis.set_visible(True)
            axs[col_idx].legend()

            if col_idx == len(columns_to_plot) - 1:
                axs[col_idx].set_xlabel('FlowDate', fontsize=16)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/{edc_name}_PLC_NSPL_{customer_class}_plot.png')
        plot_path[f'daily_{customer_class}'] = f'{output_dir}/{edc_name}_PLC_NSPL_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def save_processed_data(processed_df, output_path, data_type):
    if data_type == 'hourly':
        # Reorder columns
        column_order = [
            'Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType',
            'EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume', 'VolumeComment'
        ]
        processed_df = processed_df[column_order]
        # Set a copy to avoid warning
        processed_df = processed_df.copy()

        # Convert output Datetime_beginning_utc to string to avoid timezone error
        processed_df['Datetime_beginning_utc'] = processed_df['Datetime_beginning_utc'].astype(str)

        processed_df.to_excel(output_path, index=False, float_format='%.3f')  # Accuracy
        print(f"Data processing complete. The file '{output_path}' has been created.")

    elif data_type == 'monthly':

        column_order = ['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType', 'EGS_MonthlyVolume',
                        'Default_MonthlyVolume', 'Eligible_MonthlyVolume', 'VolumeComment']
        processed_df = processed_df[column_order]

        processed_df.to_excel(output_path, index=False)
        print(f"Data processing complete. The file '{output_path}' has been created.")

    elif data_type == 'daily':

        column_order = ['FlowDate', 'EDCName', 'CustomerClass', 'VolumeType', 'EGS_DailyVolume',
                        'Default_DailyVolume', 'Eligible_DailyVolume', 'VolumeComment']
        processed_df = processed_df[column_order]

        processed_df.to_excel(output_path, index=False)
        print(f"Data processing complete. The file '{output_path}' has been created.")

    elif data_type == 'UFE':

        # Reorder columns
        column_order = [
            'Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType',
            'EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume', 'VolumeComment'
        ]
        processed_df = processed_df[column_order]
        # Set a copy to avoid warning
        processed_df = processed_df.copy()

        # Convert output Datetime_beginning_utc to string to avoid timezone error
        processed_df['Datetime_beginning_utc'] = processed_df['Datetime_beginning_utc'].astype(str)

        processed_df.to_excel(output_path, index=False, float_format='%.3f')  # Accuracy
        print(f"Data processing complete. The file '{output_path}' has been created.")


def generate_keystats(monthly_df, hourly_df, daily_df):
    data = {'daily': daily_df, 'hourly': hourly_df, 'monthly': monthly_df}
    report_keystats_table = {}
    for freq in ['daily', 'hourly', 'monthly']:
        columns = [f'EGS_{freq.capitalize()}Volume', f'Default_{freq.capitalize()}Volume', f'Eligible_{freq.capitalize()}Volume']
        if freq == 'hourly':
            df = data[freq]

            for volume_type in df['VolumeType'].unique():
                sub_df = df[df['VolumeType'] == volume_type]
                customer_classes_list = sub_df['CustomerClass'].unique()
                for customer_classes in customer_classes_list:
                    report_keystats_table.update({f'{freq}_{customer_classes}_{volume_type}': sub_df[sub_df['CustomerClass'] == customer_classes][columns].describe().T})
        else:
            df = data[freq]
            customer_classes_list = df['CustomerClass'].unique()
            for customer_classes in customer_classes_list:
                report_keystats_table.update({f'{freq}_{customer_classes}': df[df['CustomerClass'] == customer_classes][columns].describe().T})

    return report_keystats_table


def generate_report(etl_report_output_path, report_keystats_table, plot_path, edc_name):
    def encode_image_to_base64(png_file_path):
        with open(png_file_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        return f"data:image/png;base64,{encoded_string}"

    def format_with_commas(x):
        if isinstance(x, (int, float)):
            return "{:,}".format(x)
        return x

    def format_dataframe(df):
        return df.apply(lambda col: col.map(format_with_commas))

    sections = []
    for data_type in report_keystats_table:
        sections.append({
            'subtitle': data_type,
            'dataframe': format_dataframe(report_keystats_table[data_type].round(2)).to_html(index=True),
            'plot': encode_image_to_base64(plot_path[data_type])
        })

    date_today = datetime.today().strftime('%Y-%m-%d')

    # Data for the template
    report_data = {
        'report_title': f'{edc_name} ETL Report',
        'report_description': f'Generated on {date_today}',
        'sections': sections
    }

    # Set up the Jinja2 environment
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = script_dir
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('ETL_report_template.html')

    # Render the template with data
    rendered_html = template.render(report_data)

    # Save the rendered HTML to a file
    with open(f'{etl_report_output_path}/etl_report.html', 'w') as f:
        f.write(rendered_html)

    print(f'Report saved to {etl_report_output_path}/etl_report.html')


def check_continuity(df, date_column, freq, table_name):
    df = df.copy()
    # Ensure the date column is in datetime format
    df[date_column] = pd.to_datetime(df[date_column])

    if freq == 'M':
        complete_range = pd.date_range(start=df[date_column].min().replace(day=1), end=df[date_column].max(), freq='MS')
    else:
        complete_range = pd.date_range(start=df[date_column].min(), end=df[date_column].max(), freq=freq)

    # Identify missing dates and hours
    missing_dates_hours = complete_range[~complete_range.isin(df[date_column])]

    if missing_dates_hours.empty:
        print(f"Continuity Check: No Missing Value in {table_name}")
    else:
        print(f"Continuity Check: Find Missing Values in {table_name}:")
        print(missing_dates_hours)


def main(base_path):
    # File Paths
    file_paths_hourly = {
        # Hourly Volume Files
        'hourly_volume_23': f'{base_path}\PE_Hourly Load.xlsx',
        'hourly_volume_res_19': f'{base_path}\Residential_Hourly_Load_Profiles.xlsx',
        'hourly_volume_type1_19': f'{base_path}\TypeI_Hourly_Load_Profiles.xlsx',
        'hourly_volume_type2_19': f'{base_path}\TypeII_Hourly_Load_Profiles.xlsx',
        'hourly_volume_res_eli_10': f'{base_path}\Residential_Hourly_Load_Profiles_All_Eligible.xlsx',
        'hourly_volume_res_sos_10': f'{base_path}\Residential_Hourly_Load_Profiles_SOS.xlsx',
        'hourly_volume_type1_eli_10': f'{base_path}\TypeI_Hourly_Load_Profiles_All_Eligible.xlsx',
        'hourly_volume_type1_sos_10': f'{base_path}\TypeI_Hourly_Load_Profiles_SOS.xlsx',
        'hourly_volume_type2_eli_10': f'{base_path}\TypeII_Hourly_Load_Profiles_All_Eligible.xlsx',
        'hourly_volume_type2_sos_10': f'{base_path}\TypeII_Hourly_Load_Profiles_SOS.xlsx'
    }

    file_paths_monthly = {
        # Monthly Customer Counts Files
        'customer_count_23': f'{base_path}\Total_Customer_Counts_All_Types.xlsx',
        'customer_count_res_eli_13': f'{base_path}\Residential_Customer_Count_All_Eligible.xlsx',
        'customer_count_res_sos_13': f'{base_path}\Residential_Customer_Count_SOS.xlsx',
        'customer_count_type1_eli_13': f'{base_path}\TypeI_Customer_Count_All_Eligible.xlsx',
        'customer_count_type1_sos_13': f'{base_path}\TypeI_Customer_Count_SOS.xlsx',
        'customer_count_type2_eli_13': f'{base_path}\TypeII_Customer_Count_All_Eligible.xlsx',
        'customer_count_type2_sos_13': f'{base_path}\TypeII_Customer_Count_SOS.xlsx'
    }

    # Daily Volume Files
    file_path_daily = f'{base_path}\PE_PLC_NSPL_by Type.xlsx'

    deration_factor_path = f'{base_path}\DerationFactor_APS.csv'

    ufe_file_path = f'{base_path}\PE_UFE.xlsx'

    # Read Excel Files
    hourly_df_23, hourly_df_19, hourly_df_10 = load_hourly_volume_data(file_paths_hourly)
    deration_df = pd.read_csv(deration_factor_path)
    monthly_df_23, monthly_df_13 = load_monthly_volume_data(file_paths_monthly)
    daily_volume_df = load_daily_volume_data(file_path_daily)
    ufe_df = load_ufe_data(ufe_file_path)

    print('Processing data...')
    edc_name = 'MD_PE'
    # Process hourly volume
    hourly_volume_processed = process_hourly_volume_data(hourly_df_23, hourly_df_19, hourly_df_10, edc_name)
    # Derate hourly volume with deration factor (No need to derate for premise level data)
    # hourly_volume_processed = derate_hourly_volume(hourly_volume_processed, deration_df)

    # Process monthly volume
    monthly_volume_processed = process_monthly_volume_data(monthly_df_23, monthly_df_13, edc_name)

    # Process daily volume
    daily_volume_processed = process_daily_volume_data(daily_volume_df, edc_name)

    # Handle Missing Values in daily_volume_processed
    daily_volume_processed = handle_missing_data(daily_volume_processed, edc_name)

    # Process UFE volume
    ufe_processed = process_ufe_data(hourly_volume_processed, ufe_df, edc_name)

    # Fix abnormal data
    monthly_volume_processed.loc[(monthly_volume_processed['FlowMonth'] == '2017-06-01') & (monthly_volume_processed['CustomerClass'] == 'RES'), 'Eligible_MonthlyVolume'] = 225052
    monthly_volume_processed.loc[(monthly_volume_processed['FlowMonth'] == '2017-07-01') & (monthly_volume_processed['CustomerClass'] == 'RES'), 'Eligible_MonthlyVolume'] = 228506
    monthly_volume_processed['EGS_MonthlyVolume'] = monthly_volume_processed['Eligible_MonthlyVolume'] - monthly_volume_processed['Default_MonthlyVolume']

    # Output path
    output_path = f'{base_path}/output_data'
    # Create output directory if it does not exist
    os.makedirs(output_path, exist_ok=True)

    hourly_output_path = f'{output_path}/{edc_name}_HourlyVolume_processed.xlsx'
    monthly_output_path = f'{output_path}/{edc_name}_CustomerCount_processed.xlsx'
    daily_output_path = f'{output_path}/{edc_name}_NSPL_PLC_processed.xlsx'
    ufe_output_path = f'{output_path}/{edc_name}_UFE_processed.xlsx'

    etl_report_output_path = f'{output_path}/ETL_report'

    # Plot data for correction
    print('Saving plot...')
    plot_path = {}
    plot_path.update(plot_monthly_data(monthly_volume_processed, etl_report_output_path, edc_name))
    plot_path.update(plot_hourly_data(hourly_volume_processed, etl_report_output_path, edc_name))
    plot_path.update(plot_daily_data(daily_volume_processed, etl_report_output_path, edc_name))

    # Generate ETL report
    report_keystats_table = generate_keystats(monthly_volume_processed, hourly_volume_processed, daily_volume_processed)
    generate_report(etl_report_output_path, report_keystats_table, plot_path, edc_name)

    # Check Continuity
    print('Checking continuity...')
    # Check monthly data continuity
    customer_class_list = monthly_volume_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        check_continuity(monthly_volume_processed[monthly_volume_processed['CustomerClass'] == customer_class],
                         'FlowMonth', 'M', f'{customer_class} monthly volume')

    customer_class_list = hourly_volume_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        check_continuity(hourly_volume_processed[hourly_volume_processed['CustomerClass'] == customer_class],
                         'Datetime_beginning_utc', 'H', f'{customer_class} hourly volume')

    customer_class_list = daily_volume_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        for volume_type in ['NSPL_Unscaled', 'PLC_Unscaled']:
            check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == customer_class) & (daily_volume_processed['VolumeType'] == volume_type)], 'FlowDate', 'D', f'{customer_class} {volume_type} daily volume')

    customer_class_list = ufe_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        check_continuity(ufe_processed[ufe_processed['CustomerClass'] == customer_class],
                         'Datetime_beginning_utc', 'H', f'{customer_class} ufe volume')

    # Save processed data
    print('Saving data...')
    save_processed_data(hourly_volume_processed, hourly_output_path, 'hourly')
    save_processed_data(monthly_volume_processed, monthly_output_path, 'monthly')
    save_processed_data(daily_volume_processed, daily_output_path, 'daily')
    save_processed_data(ufe_processed, ufe_output_path, 'UFE')


if __name__ == "__main__":
    base_path = 'C:\\Users\\5DIntern3_2024\\Work\\PE_MD'
    main(base_path)
