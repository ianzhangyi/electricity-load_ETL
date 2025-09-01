"""
Change base_path in main function before using
"""

import os
import pandas as pd
from datetime import datetime
import regex as re
import Automation as auto
import camelot
import pytz
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
            if file.endswith('.pdf') and keywords in file:
                file_path = os.path.join(root, file)
                file_paths[file] = file_path
    return file_paths


def check_continuity(df, date_column, table_name):
    df = df.copy()
    # Ensure the date column is in datetime format
    df[date_column] = pd.to_datetime(df[date_column])

    # Extract the year
    df['year'] = df[date_column].dt.year

    # Check for continuity
    all_years = set(range(df['year'].min(), df['year'].max() + 1))
    present_years = set(df['year'])
    missing_years = all_years - present_years

    if missing_years:
        print(f"Continuity Check: Find missing values in {sorted(missing_years)} {table_name} data")
        message = f"Continuity Check Warning: Find missing values in output {sorted(missing_years)} {table_name} data"
    else:
        message = None

    return message


def data_check(df_processed, warning_messages):
    df_processed = df_processed.copy()
    df_processed['FlowMonth'] = pd.to_datetime(df_processed['FlowMonth'])

    # Continuity Check
    df_annual = df_processed[df_processed['VolumeType'] == 'PLC_Annual']
    df_hour = df_processed[df_processed['VolumeType'] != 'PLC_Annual']
    print('Checking Continuity...')
    for locale_name in df_annual['LocaleName'].unique():
        for volume_type in df_annual['VolumeType'].unique():
            message = check_continuity(df_annual[(df_annual['LocaleName'] == locale_name) & (df_annual['VolumeType'] == volume_type)], 'FlowMonth', f'annual {locale_name} {volume_type}')
            if message:
                warning_messages.append(message)
    for locale_name in df_hour['LocaleName'].unique():
        for volume_type in df_hour['VolumeType'].unique():
            message = check_continuity(df_hour[(df_hour['LocaleName'] == locale_name) & (df_hour['VolumeType'] == volume_type)], 'FlowMonth', f'annual {locale_name} {volume_type}')
            if message:
                warning_messages.append(message)

    print('Done Continuity Checking')

    # Check NaN
    nan_rows = df_processed[df_processed.isna().any(axis=1)]

    if not nan_rows.empty:
        for index, row in nan_rows.iterrows():
            missing_time = row['FlowMonth']
            locale_name = row['LocaleName']
            volume_type = row['VolumeType']
            print(f'Find missing values in {missing_time} {locale_name} {volume_type} data')
            warning_messages.append(f'Missing Value Warning: Find missing values in {missing_time} {locale_name} {volume_type} data')
    else:
        print('No Missing Values')

    return warning_messages


def main(extract_data=True, base_path=r"C:\Users\5DIntern3_2024\Work\PJM"):
    try:
        logging.info('Starting ETL Process for 5coincidentpeaks')
        base_url = 'https://www.pjm.com/planning/resource-adequacy-planning/load-forecast-dev-process.aspx'
        current_year = datetime.now().year
        keyword = '-peaks-and-5cps'
        directory = f"{base_path}/5coincidentpeaks_data/raw_data"
        os.makedirs(directory, exist_ok=True)
        missing_data_year = []

        # Document the warning messages
        warning_messages = []

        if extract_data:
            logging.info('Downloading Data from PJM Website')
            # Data download starts
            downloaded_pdf_paths = []
            for year in range(2019, current_year + 1):
                print("Processing year:", year)
                annual_links = auto.find_data_url(base_url, year, keyword, is_current_year=(year == current_year),
                                                  is_5cps=True)
                if len(annual_links) != 0:
                    downloaded_pdf_paths.append(auto.download_pdf_files(directory, annual_links[0], is_5cps=True)[0])
                    print('                                                ')
                else:
                    missing_data_year.append(str(year))
                    print(f'5CoincidentPeaks data from {year} is missing')
                    print('                                                ')
                    warning_messages.append(f'Data Source Warning: 5CoincidentPeaks data from {year} is missing in PJM website')
        else:
            logging.info('Loading Local Data')
            # Load with local data
            local_file_paths = find_target_files_path(directory, '5CPS')
            downloaded_pdf_paths = []
            for file_path in local_file_paths:
                downloaded_pdf_paths.append(local_file_paths[file_path])

        logging.info('Processing Data')
        res = pd.DataFrame()

        # Read-in data and formatting starts
        for pdf_path in downloaded_pdf_paths:
            match = re.search(r'(\d{4})\.pdf', pdf_path)
            if match:
                year = match.group(1)
                logging.info(f'Processing {year} data')
            tables = camelot.read_pdf(pdf_path, flavor='stream', pages='all')
            df1 = tables[0].df
            df2 = tables[1].df

            # Formatting the first page
            if year == '2023':
                df1 = df1.iloc[2:-2].reset_index(drop=True)
            else:
                df1 = df1.iloc[2:].reset_index(drop=True)

            df1.columns = ['LocaleName', 'temp', 'VolumeLevel']
            df1.loc[df1['temp'] != '', 'VolumeLevel'] = df1.loc[df1['temp'] != '', 'temp']

            df1 = df1.drop('temp', axis=1)
            mapping_dict = {'Vineland': 'VINELAND', 'DAYTON': 'DAY', 'DLCo': 'DUQ', 'PENLC': 'PENELEC', 'PL': 'PPL', 'PS': 'PSEG', 'PJM RTO':'PJM_RTO'}
            for idx in df1.index:
                zone = df1.loc[idx, 'LocaleName']
                if zone in mapping_dict:
                    df1.loc[idx, 'LocaleName'] = mapping_dict[zone]

            df1['VolumeType'] = 'PLC_Annual'
            df1['VolumeComment'] = 'average'
            df1['VolumeUnit'] = 'MW'
            df1['FlowMonth'] = f'{year}-06-01'
            df1 = df1[['FlowMonth', 'LocaleName', 'VolumeLevel', 'VolumeType', 'VolumeUnit', 'VolumeComment']]

            # Formatting the second page
            new_row = df2.iloc[0] + ' ' + df2.iloc[1]
            new_row_df = pd.DataFrame([new_row], index=[len(df2)])
            df2 = pd.concat([new_row_df, df2])
            df2 = df2.drop([0, 1], axis=0)
            df2.columns = ['LocaleName', '1', '2', '3', '4', '5']
            for i in range(1, 6):
                df2[f'c{i}'] = df2[str(i)].iloc[0]

            df2 = df2.iloc[1:]

            mapping_dict = {'DAYTON': 'DAY', 'DLCo': 'DUQ', 'PENLC': 'PENELEC', 'PL': 'PPL', 'PS': 'PSEG',
                            'PPL-EU': 'PPL', 'PJM RTO':'PJM_RTO', 'Vineland': 'VINELAND'}
            for idx in df2.index:
                zone = df2.loc[idx, 'LocaleName']
                if zone in mapping_dict:
                    df2.loc[idx, 'LocaleName'] = mapping_dict[zone]

            df2_final = pd.DataFrame()
            for i in range(1, 6):
                df_h_i = df2[['LocaleName', str(i), f'c{i}']].copy()
                df_h_i[f'c{i}'] = pd.to_datetime(df_h_i[f'c{i}'])
                df_h_i['utc'] = df_h_i[f'c{i}'].dt.tz_localize('UTC')
                eastern = pytz.timezone('America/New_York')
                df_h_i['utc'] = df_h_i['utc'].dt.tz_convert(eastern)
                df_h_i['timezone_offset'] = df_h_i['utc'].astype(str).str[-6:]
                df_h_i['VolumeComment'] = df_h_i[f'c{i}'].astype(str) + df_h_i['timezone_offset']
                df_h_i = df_h_i.drop([f'c{i}', 'utc', 'timezone_offset'], axis=1)
                df_h_i.rename(columns={str(i): 'VolumeLevel'}, inplace=True)
                df_h_i['VolumeType'] = f'PLC_Hour_{i}'

                df2_final = pd.concat([df2_final, df_h_i], ignore_index=True)

            df2_final['VolumeUnit'] = 'MW'
            df2_final['FlowMonth'] = f'{year}-06-01'
            df2_final = df2_final[['FlowMonth', 'LocaleName', 'VolumeLevel', 'VolumeType', 'VolumeUnit', 'VolumeComment']]

            # Put two tables together
            temp_res = pd.concat([df1, df2_final])
            temp_res = temp_res.sort_values(by=['LocaleName', 'VolumeType'], ascending=[True, True])
            res = pd.concat([res, temp_res])

        res['FlowMonth'] = pd.to_datetime(res['FlowMonth'])
        res['VolumeLevel'] = res['VolumeLevel'].str.replace(',', '').astype(float)
        res['VolumeLevel'] = res['VolumeLevel'].round(2)
        res = res.reset_index(drop=True)

        # Output formatting
        data_processed = {
            'FlowMonth': res['FlowMonth'].dt.strftime('%Y-%m-%d'),
            'LocaleName': res['LocaleName'],
            'VolumeLevel': res['VolumeLevel'].astype(float),
            'VolumeType': res['VolumeType'],
            'VolumeUnit': 'MW',
            'VolumeComment': res['VolumeComment'].astype(str)
        }
        df_processed = pd.DataFrame(data_processed)
        df_processed.sort_values(by=['FlowMonth', 'LocaleName', 'VolumeType'], ignore_index=True)

        logging.info('Checking Data')
        # download data from database for crosscheck:
        conn, engine = dbop.db_connect('LoadStaging')
        df_database = pd.read_sql('select * from [dbo].[Load_PJMPLCNSPL] where VolumeType != \'NSPL_Volume\'', conn)
        conn.close()
        df_database['FlowMonth'] = pd.to_datetime(df_database['FlowMonth']).dt.strftime('%Y-%m-%d')
        # Output data check
        warning_messages = data_check(df_processed, warning_messages)

        # Data Crosscheck
        df_merged = df_processed.merge(df_database, on=['FlowMonth', 'LocaleName', 'VolumeType'], how='outer', indicator=True, suffixes=('_output', '_db'))
        df_match = df_merged[df_merged['_merge'] == 'both'].copy()
        df_match['volume_diff'] = df_match['VolumeLevel_output'] - df_match['VolumeLevel_db']
        df_mismatch = df_match[abs(df_match['volume_diff']) > 1]
        if not df_mismatch.empty:
            for index, row in df_mismatch.iterrows():
                missing_time = row['FlowMonth']
                locale_name = row['LocaleName']
                volume_type = row['VolumeType']
                print(f'Find data mismatch in {missing_time} {locale_name} {volume_type}')
                warning_messages.append(f'Data Mismatch Warning: Find data mismatch in {missing_time} {locale_name} {volume_type}')

        df_update = df_merged[df_merged['_merge'] == 'left_only'].copy()
        df_dbonly = df_merged[df_merged['_merge'] == 'right_only'].copy()

        if pd.to_datetime(df_update['FlowMonth']).min() <= pd.to_datetime(df_database['FlowMonth']).max():
            df_conflict = df_update[pd.to_datetime(df_update['FlowMonth']) <= pd.to_datetime(df_database['FlowMonth']).max()]
            conflict_locale = df_conflict['LocaleName'].unique()
            conflict_start = pd.to_datetime(df_update['FlowMonth']).min().strftime('%Y-%m-%d')
            conflict_end = pd.to_datetime(df_database['FlowMonth']).max().strftime('%Y-%m-%d')

            print(f'Data Conflict Warning: Cannot find {conflict_locale} between {conflict_start} and {conflict_end} in database')
            warning_messages.append(f'Data Conflict Warning: Cannot find {conflict_locale} between {conflict_start} and {conflict_end} in database')

        if pd.to_datetime(df_dbonly['FlowMonth']).max() >= pd.to_datetime(df_processed['FlowMonth']).min():
            df_conflict = df_dbonly[pd.to_datetime(df_dbonly['FlowMonth']) >= pd.to_datetime(df_processed['FlowMonth']).min()]
            conflict_locale = df_conflict['LocaleName'].unique()
            conflict_start = pd.to_datetime(df_processed['FlowMonth']).min().strftime('%Y-%m-%d')
            conflict_end = pd.to_datetime(df_dbonly['FlowMonth']).max().strftime('%Y-%m-%d')

            print(f'Data Conflict Warning: Cannot find {conflict_locale} between {conflict_start} and {conflict_end} in processed data')
            warning_messages.append(f'Data Conflict Warning: Cannot find {conflict_locale} between {conflict_start} and {conflict_end} in processed data')

        # Save output data
        logging.info('Saving Data')
        data_update = {
            'FlowMonth': df_update['FlowMonth'],
            'LocaleName': df_update['LocaleName'],
            'VolumeLevel': df_update['VolumeLevel_output'],
            'VolumeType': df_update['VolumeType'],
            'VolumeUnit': 'MW',
            'VolumeComment': df_update['VolumeComment_output']
        }
        df_update = pd.DataFrame(data_update)

        os.makedirs(f"{base_path}/5coincidentpeaks_data/output_data", exist_ok=True)
        update_file_output_path = f"{base_path}/5coincidentpeaks_data/output_data/5CoincidentPeaks_update.csv"
        df_update.to_csv(update_file_output_path, index=False)

        processed_file_output_path = f"{base_path}/5coincidentpeaks_data/output_data/5CoincidentPeaks_processed.csv"
        df_processed.to_csv(processed_file_output_path, index=False)
        print("Final output is successfully saved!")

        # Save warning message
        if not warning_messages:
            warning_messages.append('No Warning in ETL Process')
        warning_dir = f"{base_path}/5coincidentpeaks_data"
        filename = f'5coincidentpeaks_etl_warnings.txt'
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
    logging.info('5coincidentpeaks ETL Process Complete')

