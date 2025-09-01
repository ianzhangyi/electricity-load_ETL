import pandas as pd
from datetime import datetime
import os
import regex as re
import Automation as auto
import numpy as np
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
            print(f'Find missing values in {missing_time} {locale_name} data')
            warning_messages.append(f'Missing Value Warning: Find missing values in {missing_time} {locale_name} data')
    else:
        print('No Missing Values')

    return warning_messages



def main(extract_data=True, base_path=r"C:\Users\5DIntern3_2024\Work\PJM"):
    try:
        logging.info('Starting ETL Process for BlackStart')
        base_url = 'https://pjm.com/markets-and-operations/billing-settlements-and-credit.aspx'
        current_year = datetime.now().year
        keyword = 'black-start-revenue-requirements'
        directory = f"{base_path}/BlackStart_data/raw_data"
        os.makedirs(directory, exist_ok=True)
        missing_data_year = []
        missing_data_month = []
        # Document the warning messages
        warning_messages = []

        if extract_data:
            logging.info('Downloading Data from PJM Website')
            # For current year data
            print("Processing year:", current_year)
            current_year_links = auto.find_data_url(base_url, current_year, keyword, is_current_year=True)
            for link in current_year_links:
                auto.download_files(directory, link, is_current_year=True)

            # For past year data
            for year in range(2019, current_year): # change to time period you selected
                print(f"Processing year: {year}")
                past_year_links = auto.find_data_url(base_url, year, keyword, is_current_year=False)
                if len(past_year_links) != 0:
                    for link in past_year_links:
                        zip_path = auto.download_files(directory, link, is_current_year=False)
                        auto.unzip_files(zip_path.pop(0), directory)
                        auto.delete_contents(directory, delete_zip=True, target_year=str(year))
                        checking_year = str(year)
                        temp_list = auto.check_missing_months(directory, checking_year)
                        if temp_list:
                            missing_data_month.append({str(year): temp_list})

                else:
                    missing_data_year.append(str(year))
                    print(f'BlackStartRevenue data from {year} is missing')
                    warning_messages.append(f'Data Source Warning: BlackStartRevenue data from {year} is missing in PJM website')
        else:
            logging.info('Loading Local Data')

        unique_locales = set()
        res = pd.DataFrame()
        total_sorted_paths = []

        for year in range(2018, current_year+1): # change to your selected time frame
            target_year = str(year)
            sorted_paths = auto.sort_files_by_date(directory, target_year)
            total_sorted_paths.extend(sorted_paths)

        logging.info('Processing Data')
        for file_path in total_sorted_paths:
            df = pd.read_excel(file_path, skiprows=1, usecols=[1, 2], header=None, engine='openpyxl')
            df = df.drop(0)
            df.columns = ['Transmission Zone', 'Current Black Start Revenue Requirement']

            df = df[df['Transmission Zone'].str.contains('^[A-Z]+$', na=False)]
            unique_locales.update(df['Transmission Zone'])

            flow_month_match = re.search(r"(\w+)-(\d{4})", file_path)
            flow_month = f"{flow_month_match.group(2)}-{pd.to_datetime(flow_month_match.group(1), format='%b').month}"
            df['FlowMonth'] = flow_month
            df['FlowMonth'] = pd.to_datetime(df['FlowMonth'], format='%Y-%m').dt.strftime('%Y-%m-01')

            df['PriceType'] = 'BlackStartRevenue'
            df['PriceUnit'] = 'USD'
            df['PriceComment'] = None

            df = df.rename(
                columns={'Transmission Zone': 'LocaleName', 'Current Black Start Revenue Requirement': 'PriceLevel'})
            df = df[['FlowMonth', 'LocaleName', 'PriceLevel', 'PriceType', 'PriceUnit', 'PriceComment']]

            res = pd.concat([res, df], ignore_index=True)

        res['FlowMonth'] = pd.to_datetime(res['FlowMonth']).dt.strftime('%Y-%m-%d')
        res['PriceLevel'] = np.float64(res['PriceLevel'])
        res['PriceLevel'] = res['PriceLevel'].round(2)

        # Output the filtered result to csv file, subject for further changes for this data
        data_processed = {
            'FlowMonth': res['FlowMonth'],
            'LocaleName': res['LocaleName'],
            'PriceLevel': res['PriceLevel'],
            'PriceType': 'BlackStartRevenue',
            'PriceUnit': 'USD',
            'PriceComment': ''
        }
        df_processed = pd.DataFrame(data_processed)

        # download data from database for crosscheck:
        conn, engine = dbop.db_connect('LoadStaging')
        df_database = pd.read_sql('select * from [dbo].[Load_PJMMonthlyPriceHist] where PriceType = \'BlackStartRevenue\'', conn)
        conn.close()

        df_database['FlowMonth'] = pd.to_datetime(df_database['FlowMonth']).dt.strftime('%Y-%m-%d')

        # Output data check
        logging.info('Checking Data')
        warning_messages = data_check(df_processed, warning_messages)

        df_merged = df_processed.merge(df_database, on=['FlowMonth', 'LocaleName'], how='outer', indicator=True, suffixes=('_output', '_db'))
        df_match = df_merged[df_merged['_merge'] == 'both'].copy()
        df_match['PriceLevel_diff'] = df_match['PriceLevel_output'] - df_match['PriceLevel_db']
        df_mismatch = df_match[abs(df_match['PriceLevel_diff']) > 1]
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
            'PriceLevel': df_update['PriceLevel_output'],
            'PriceType': 'BlackStartRevenue',
            'PriceUnit': 'USD',
            'PriceComment': ''
        }
        df_update = pd.DataFrame(data_update)

        os.makedirs(f"{base_path}/BlackStart_data/output_data", exist_ok=True)
        update_file_output_path = f"{base_path}/BlackStart_data/output_data/BlackStartRevenue_update.csv"
        df_update.to_csv(update_file_output_path, index=False)

        processed_file_output_path = f"{base_path}/BlackStart_data/output_data/BlackStartRevenue_processed.csv"
        df_processed.to_csv(processed_file_output_path, index=False)
        print("Final output is successfully saved!")

        # Save warning message
        if not warning_messages:
            warning_messages.append('No Warning in ETL Process')
        warning_dir = f"{base_path}/BlackStart_data"
        filename = f'BlackStart_etl_warnings.txt'
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
    logging.info('BlackStart ETL Process Complete')


