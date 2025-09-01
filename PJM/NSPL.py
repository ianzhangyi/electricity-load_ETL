import os
import pandas as pd
from datetime import datetime
import regex as re
import Automation as auto
import pdfplumber
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
    complete_range = pd.date_range(start=df[date_column].min().replace(day=1), end=df[date_column].max(), freq='MS')
    missing_dates_hours = complete_range[~complete_range.isin(df[date_column])]
    if not missing_dates_hours.empty:
        print(f"Continuity Check: Find missing values in {sorted(missing_dates_hours)} {table_name} data")
        message = f"Continuity Check Warning: Find missing values in output {sorted(missing_dates_hours)} {table_name} data"
    else:
        message = None

    return message


def data_check(df_processed, warning_messages):
    df_processed = df_processed.copy()
    df_processed['FlowMonth'] = pd.to_datetime(df_processed['FlowMonth'])

    # Continuity Check
    print('Checking Continuity...')
    for locale_name in df_processed['LocaleName'].unique():
        message = check_continuity(df_processed[(df_processed['LocaleName'] == locale_name)], 'FlowMonth', f'{locale_name} data')
        if message:
            warning_messages.append(message)

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


def find_locale_name(check_value, df_map_total):
    """
    Searches a DataFrame for a row where the 'check_value' is a substring of the 'Cleaned' column.

    Parameters:
    - check_value (str): The substring to search for within the 'Cleaned' column of df_map_total (a predefined df to store mapping info).

    Returns:
    - str: The 'LocaleName' of the matching row if a row is found where the substring matches.
    - None: Returns None if no matching row is found.
    """
    words = check_value.split()
    pattern = '.*'.join(map(re.escape, words))
    for _, row in df_map_total.iterrows():
        # Search for the pattern in 'Cleaned', checking if the words appear in order
        if re.search(pattern, row['Cleaned'], re.IGNORECASE):
            return row['LocaleName']
    return None


def main(extract_data=True, base_path=r"C:\Users\5DIntern3_2024\Work\PJM"):
    try:
        logging.info('Starting ETL Process for NSPL')
        base_url = 'https://pjm.com/markets-and-operations/billing-settlements-and-credit.aspx'
        current_year = datetime.now().year
        keyword = 'network-service-peak-loads'
        directory = f"{base_path}/NSPL_data/raw_data"
        os.makedirs(directory, exist_ok=True)
        downloaded_pdf_paths = []
        missing_data_year = []
        # Document the warning messages
        warning_messages = []

        if extract_data:
            logging.info('Downloading Data from PJM Website')
            # Data download starts
            for year in range(2016, current_year+1):
                print("Processing year:", year)
                annual_links = auto.find_data_url(base_url, year, keyword, is_current_year=(year == current_year))
                if len(annual_links) != 0:
                    downloaded_pdf_paths.append(auto.download_pdf_files(directory, annual_links[0], is_NSPL=True)[0])

                else:
                    missing_data_year.append(str(year))
                    print(f'NSPL data from {year} is missing')
                    warning_messages.append(f'Data Source Warning: NSPL data from {year} is missing in PJM website')

        else:
            logging.info('Loading Local Data')
            local_file_paths = find_target_files_path(directory, 'NSPL')
            downloaded_pdf_paths = []
            for file_path in local_file_paths:
                downloaded_pdf_paths.append(local_file_paths[file_path])

        res = pd.DataFrame()
        df_map_total = pd.DataFrame()
        downloaded_pdf_paths.sort(reverse=True)
        '''
        Read-in data and formatting starts
        '''
        logging.info('Processing Data')
        for pdf_path in downloaded_pdf_paths:
            match = re.search(r'(\d{4})\.pdf', pdf_path)
            if match:
                year = match.group(1)
            logging.info(f'Processing {int(year)} data')
            # New table format starts after 2023
            if int(year) >= 2023:
                data = []
                with pdfplumber.open(pdf_path) as pdf:
                    for page in pdf.pages:
                        tables = page.extract_tables()
                        for table in tables:
                            for row in table:
                                data.append(row)

                df = pd.DataFrame(data)
                df.columns = df.iloc[0]
                df = df[1:].reset_index(drop=True)

                df = df.rename(columns={'Transmission Zone\nShort Name': 'LocaleName'})

                # Save a mapping dictionary using the current year table as a reference (Assume pattern stays the same after 2023)
                if int(year) == int(current_year):
                    # Save the mapping dictionary for later usage
                    df['Cleaned'] = df['Transmission Zone'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
                    df_map = df[['LocaleName','Cleaned']]
                    df_map_total = pd.concat([df_map_total, df_map], ignore_index= True)

                # Continue to the rest of formatting
                df = df.drop('Transmission Zone', axis=1)

                df['HourEnding (EPT)'] = df['HourEnding (EPT)'].astype(int) - 1
                df['HourEnding (EPT)'] = df['HourEnding (EPT)'].astype(str)
                df['Datetime'] = df['Date'] + ' ' + df['HourEnding (EPT)']
                df['Datetime'] = pd.to_datetime(df['Datetime'])

                df1 = pd.DataFrame()
                df1['new'] = df['Datetime'].copy()
                df1['utc'] = df1['new'].dt.tz_localize('UTC')

                eastern = pytz.timezone('America/New_York')
                df1['utc'] = df1['utc'].dt.tz_convert(eastern)
                df1['timezone_offset'] = df1['utc'].astype(str).str[-6:]
                df1['new'] = df1['new'].astype(str)
                df1['timezone_offset'] = df1['timezone_offset'].astype(str)
                df1['VolumeComment'] = df1['new'] + df1['timezone_offset']
                df1 = df1.drop(['new', 'utc', 'timezone_offset'], axis=1)

                df['VolumeComment'] = df1['VolumeComment'].values
                df = df.drop(['Date','HourEnding (EPT)','Datetime'], axis=1)

                df['VolumeType'] = 'NSPL_Volume'
                df["VolumeUnit"] = 'MW'

                dates = pd.date_range(start=f'{year}-01-01', end=f'{year}-12-01', freq='MS')
                dates_df = pd.DataFrame({'Day': dates})
                dates_df['Day'] = dates_df['Day'].astype(str)
                df = pd.merge(df, dates_df, how='cross')

                df = df.rename(columns={'Zonal Peak (MW)': 'VolumeLevel','Day': 'FlowMonth'})
                df = df[['FlowMonth', 'LocaleName', 'VolumeLevel', 'VolumeType', 'VolumeUnit', 'VolumeComment']]

                res = pd.concat([res, df], ignore_index=True)
            else:
                tables = camelot.read_pdf(pdf_path, flavor='stream', pages='all')
                df = tables[0].df
                df = df.iloc[2:].reset_index(drop=True)
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)

                # Deal with 2017 special rows and updated values
                if int(year) == 2017:
                    correction_AEP1 = df[df['Zone'] == 'AEP']['Zonal Peak (MW)'][0]
                    correction_AEP2 = df[df['Zone'] == 'AEP']['Zonal Peak (MW)'][21]
                    correction_DAY1 = df[df['Zone'] == 'Dayton']['Zonal Peak (MW)'][6]
                    correction_DAY2 = df[df['Zone'] == 'Dayton']['Zonal Peak (MW)'][22]
                    df = df[:-3]

                # Mapping takes place
                df['Check'] = df['Zone'].str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)
                df['Zone'] = df['Check']
                df['LocaleName'] = df['Check'].apply(lambda x: find_locale_name(x, df_map_total))
                df = pd.merge(df, df_map_total, on='LocaleName', how='left')

                # Special cases in naming that cannot be directly mapped
                for i in range(len(df)):
                    if df['LocaleName'].iloc[i] is None:
                        df['LocaleName'].iloc[i] = df['Zone'].iloc[i]
                    if df['LocaleName'].iloc[i] in ['ComEd','Dominion','Duke Energy OHKY']:
                        special_name_map = {'ComEd': 'COMED', 'Dominion': 'DOM', 'Duke Energy OHKY': 'DEOK'}
                        df['LocaleName'].iloc[i] = special_name_map[df['LocaleName'].iloc[i]]

                df.drop(columns=['Zone', 'Check', 'Cleaned'], inplace=True)

                df['Hour Ending (Eastern Prevailing Time)'] = df['Hour Ending (Eastern Prevailing Time)'].str.strip().replace(r'\s+', ' ', regex=True)
                split_df = df['Hour Ending (Eastern Prevailing Time)'].str.split(' ', expand=True)
                df['Date'] = split_df[0]
                df['Hour'] = split_df[1]
                df['Hour'] = df['Hour'].astype(int) - 1
                df['Hour'] = df['Hour'].astype(str)

                df['Datetime'] = df['Date']+ ' ' + df['Hour']

                if year == '2016':
                    df['Datetime'] = pd.to_datetime(df['Datetime'], format='%m/%d/%Y %H')
                else:
                    df['Datetime'] = pd.to_datetime(df['Datetime'], format='%m/%d/%y %H')

                df1 = pd.DataFrame()
                df1['new'] = df['Datetime'].copy()
                df1['utc'] = df1['new'].dt.tz_localize('UTC')
                eastern = pytz.timezone('America/New_York')
                df1['utc'] = df1['utc'].dt.tz_convert(eastern)
                df1['timezone_offset'] = df1['utc'].astype(str).str[-6:]

                df1['new'] = df1['new'].astype(str)
                df1['timezone_offset'] = df1['timezone_offset'].astype(str)
                df1['VolumeComment'] = df1['new'] + df1['timezone_offset']
                df1 = df1.drop(['new', 'utc', 'timezone_offset'], axis=1)

                df['VolumeComment'] = df1['VolumeComment'].values
                df = df.drop(['Hour Ending (Eastern Prevailing Time)'], axis=1)

                dates = pd.date_range(start=f'{year}-01-01', end=f'{year}-12-01', freq='MS')
                dates_df = pd.DataFrame({'Day': dates})
                dates_df['Day'] = dates_df['Day'].astype(str)
                df = pd.merge(df, dates_df, how='cross')

                # Fill-in corrected values in 2017
                if int(year) == 2017:
                    df.loc[(df['Day'] <= '2017-05-01') & (df['LocaleName'] == 'AEP'), 'Zonal Peak (MW)'] = correction_AEP1
                    df.loc[(df['Day'] <= '2017-05-01') & (df['LocaleName'] == 'DAY'), 'Zonal Peak (MW)'] = correction_DAY1

                    df.loc[(df['Day'] > '2017-05-01') & (df['LocaleName'] == 'AEP'), 'Zonal Peak (MW)'] = correction_AEP2
                    df.loc[(df['Day'] > '2017-05-01') & (df['LocaleName'] == 'DAY'), 'Zonal Peak (MW)'] = correction_DAY2

                df['VolumeType'] = 'NSPL_Volume'
                df["VolumeUnit"] = 'MW'
                df = df.drop(['Date','Hour','Datetime'], axis=1)

                df = df.rename(columns={'Zonal Peak (MW)': 'VolumeLevel','Day':'FlowMonth'})
                df = df[['FlowMonth', 'LocaleName', 'VolumeLevel', 'VolumeType', 'VolumeUnit', 'VolumeComment']]
                res = pd.concat([res, df], ignore_index=True)

        res['FlowMonth'] = pd.to_datetime(res['FlowMonth'])
        res['VolumeLevel'] = res['VolumeLevel'].str.replace(',', '').astype(float)
        res = res.sort_values(by=['FlowMonth','LocaleName'], ascending=[True,True])
        res.reset_index(drop=True, inplace=True)
        res['FlowMonth'] = res['FlowMonth'].dt.date

        # Output formatting
        data_processed = {
            'FlowMonth': pd.to_datetime(res['FlowMonth']).dt.strftime('%Y-%m-%d'),
            'LocaleName': res['LocaleName'],
            'VolumeLevel': res['VolumeLevel'],
            'VolumeType': res['VolumeType'],
            'VolumeUnit': res['VolumeUnit'],
            'VolumeComment': res['VolumeComment']
        }
        df_processed = pd.DataFrame(data_processed)

        # download data from database for crosscheck:
        conn,engine = dbop.db_connect('LoadStaging')
        df_database = pd.read_sql('select * from [dbo].[Load_PJMPLCNSPL] WHERE VolumeType = \'NSPL_Volume\'',conn)
        conn.close()

        df_database['FlowMonth'] = pd.to_datetime(df_database['FlowMonth']).dt.strftime('%Y-%m-%d')
        # Output data check
        logging.info('Checking Data')
        warning_messages = data_check(df_processed, warning_messages)

        # Data Crosscheck
        df_merged = df_processed.merge(df_database, on=['FlowMonth', 'LocaleName'], how='outer', indicator=True, suffixes=('_output', '_db'))
        df_match = df_merged[df_merged['_merge'] == 'both'].copy()
        df_match['volume_diff'] = df_match['VolumeLevel_output'] - df_match['VolumeLevel_db']
        df_mismatch = df_match[abs(df_match['volume_diff']) > 1]
        if not df_mismatch.empty:
            for index, row in df_mismatch.iterrows():
                missing_time = row['FlowMonth']
                locale_name = row['LocaleName']
                print(f'Find data mismatch in {missing_time} {locale_name}')
                warning_messages.append(f'Data Mismatch Warning: Find data mismatch in {missing_time} {locale_name}')

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
            'VolumeType': df_update['VolumeType_output'],
            'VolumeUnit': df_update['VolumeUnit_output'],
            'VolumeComment': df_update['VolumeComment_output']
        }
        df_update = pd.DataFrame(data_update)

        os.makedirs(f"{base_path}/NSPL_data/output_data", exist_ok=True)
        update_file_output_path = f"{base_path}/NSPL_data/output_data/NSPL_update.csv"
        df_update.to_csv(update_file_output_path, index=False)

        processed_file_output_path = f"{base_path}/NSPL_data/output_data/NSPL_processed.csv"
        df_processed.to_csv(processed_file_output_path, index=False)
        print("Final output is successfully saved!")

        # Save warning message
        if not warning_messages:
            warning_messages.append('No Warning in ETL Process')
        warning_dir = f"{base_path}/NSPL_data"
        filename = f'NSPL_etl_warnings.txt'
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
    logging.info('NSPL ETL Process Complete')
