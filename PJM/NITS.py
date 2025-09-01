import os
import pandas as pd
import numpy as np
from datetime import datetime
import regex as re
import pdfplumber
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
        message = check_continuity(df_processed[(df_processed['LocaleName'] == locale_name)], 'FlowMonth', f'annual {locale_name}')
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


def main(extract_data=True, base_path=r"C:\Users\5DIntern3_2024\Work\PJM"):
    try:
        logging.info('Starting ETL Process for NITS')
        base_url = 'https://pjm.com/markets-and-operations/billing-settlements-and-credit.aspx'
        current_year = datetime.now().year
        keyword = 'network-integration-trans-service'
        directory = f"{base_path}/NITS_data/raw_data"
        os.makedirs(directory, exist_ok=True)
        downloaded_pdf_paths_current = []
        downloaded_pdf_paths_past = []
        missing_data_year = []
        # Document the warning messages
        warning_messages = []
        if extract_data:
            logging.info('Downloading Data from PJM Website')
            # For current year data:
            print("Processing year:", current_year)
            current_year_links = auto.find_data_url(base_url, current_year, keyword, is_current_year=True)

            if len(current_year_links) != 0:
                for i in range(len(current_year_links)):
                    downloaded_pdf_paths_current.append(auto.download_pdf_files(directory, current_year_links[i], is_NITS=True, is_current_year=True)[0])
                    i+=1

            # For past years data
            for year in range(2021, current_year):
                print("Processing year:", year)
                past_year_links = auto.find_data_url(base_url, year, keyword, is_current_year=(year == current_year))
                if len(past_year_links) != 0:
                    downloaded_pdf_paths_past.append(auto.download_pdf_files(directory, past_year_links[0], is_NITS=True, is_current_year=False)[0])

                else:
                    missing_data_year.append(str(year))
                    print(f'NITS data from {year} is missing')
                    warning_messages.append(f'Data Source Warning: NITS data from {year} is missing in PJM website')

            downloaded_pdf_paths = downloaded_pdf_paths_past + downloaded_pdf_paths_current
            downloaded_pdf_paths = sorted(downloaded_pdf_paths)
        else:
            logging.info('Loading Local Data')
            # Load with local data
            local_file_paths = find_target_files_path(directory, 'NITS')
            downloaded_pdf_paths = []
            for file_path in local_file_paths:
                downloaded_pdf_paths.append(local_file_paths[file_path])

        logging.info('Processing Data')
        # Read-in data and formatting starts
        year_total = []
        for pdf_path in downloaded_pdf_paths:
            match = re.search(r'(\w+-)?(\d{4})\.pdf', pdf_path)
            if match:
                year = match.group(2)
                year_total.append(year)
            else:
                print('Could not find the year in pdf link, please check your pdf path.')

        # zip the year number and pdf_path together for convenience of read-in pdf_path
        zipped_list = list(zip(year_total, downloaded_pdf_paths))

        pdfs_by_year = {}
        for year, pdf_path in zipped_list:
            if year not in pdfs_by_year:
                pdfs_by_year[year] = []
            pdfs_by_year[year].append(pdf_path)

        res_total = pd.DataFrame()
        for year, pdf_paths in pdfs_by_year.items():
            logging.info(f'Processing {year} data')
            for pdf_path in pdf_paths:
                res_per_year = pd.DataFrame()
                df_time_total = pd.DataFrame()
                with pdfplumber.open(pdf_path) as pdf:
                    for index, page in enumerate(pdf.pages):
                        tables = page.extract_tables()
                        flat_list = [str(item) for sublist in tables for item in sublist if item is not None]
                        targets = {
                            'target1': 'Annual Transmission Revenue Requirements and Rates',
                            'target2': 'Annual Transmission Revenue Requirements (ATRR) and Network Integration Transmission Service (NITS) Rates'
                        }
                        results = {key: any(target in item for item in flat_list) for key, target in targets.items()}
                        for key, found in results.items():
                            if found:
                                output = f"{key}"

                        # first table type before June 2022
                        if output == 'target1':
                            data = pd.DataFrame(tables[0][4:],columns = ['Transmission Owner', 'Comment', 'Level', 'col1', 'col2'])
                            data.drop(['col1','col2'], axis=1, inplace=True)
                            data.reset_index(drop=True, inplace=True)
                            df_i = data

                            segmented_texts = []
                            page_text = page.extract_text()
                            if page_text:
                                segments = page_text.split('Annual Transmission Revenue Requirements and Rates')
                                for segment in segments[1:]:
                                    segmented_texts.append(segment.strip())

                            date_pattern1 = r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|' \
                                           r'Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)' \
                                           r'(?: \d{1,2})?,? \d{4} (?: \(([^)]+)\))?'

                            date_pattern2 = r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|' \
                                           r'Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?) \d{1,2}, \d{4}'
                            dates_matched = []
                            for text in segmented_texts:
                                parts = text.split('\n')
                                match = re.search(date_pattern1, parts[-1]) or re.search(date_pattern2, parts[-1])
                                if match:
                                    date_str = match.group(0)
                                    date_str = date_str.rstrip()
                                    date_obj = None  # Initialize date_obj
                                    try:
                                        date_obj = datetime.strptime(date_str, '%B %d, %Y')
                                    except ValueError:
                                        pass
                                    if date_obj is None:
                                        try:
                                            date_obj = datetime.strptime(date_str, '%B %Y')
                                            date_obj = date_obj.replace(day=1)
                                        except ValueError:
                                            pass
                                    if date_obj:
                                        formatted_date = date_obj.strftime('%Y-%m-%d')
                                        dates_matched.append(formatted_date)
                            df_time = pd.DataFrame(dates_matched, columns=['Date'])
                            df_time_total = pd.concat([df_time_total,df_time])

                            df_i.reset_index(drop=True, inplace=True)
                            rockland_indices = df_i[df_i['Transmission Owner'] == 'Rockland (RECO)'].index
                            df_i = df_i.loc[:rockland_indices[0]]
                            df_i = df_i.copy()
                            df_i['FilteredComment'] = df_i['Comment'].str.replace('[^\d.]', '', regex=True)
                            df_i['FilteredLevel'] = df_i['Level'].str.replace('[^\d.]', '', regex=True)

                            for idx in df_i.index:
                                zone = df_i.loc[idx, 'Transmission Owner']
                                match = re.search(r'\(\s*([^)]+)\s*\)', zone)
                                if match:
                                    df_i.loc[idx, 'Zone'] = match.group(1)
                                else:
                                    df_i.loc[idx, 'Zone'] = zone

                            df_i = df_i.drop(['Comment', 'Level'], axis=1)
                            df_i['FilteredComment'] = df_i['FilteredComment'].astype(float)
                            df_i['FilteredLevel'] = df_i['FilteredLevel'].astype(float)
                            df_i.loc[df_i['Zone'] == 'CE', 'Zone'] = 'COMED'
                            df_i.loc[df_i['Zone'] == 'DLCO', 'Zone'] = 'DUQ'
                            df_i.loc[df_i['Zone'] == 'METED, PENELEC', 'Zone'] = 'MAIT'

                            dom_index = df_i[df_i['Zone'] == 'DOM'].index
                            df_i.iloc[dom_index[0], df_i.columns.get_loc('FilteredComment')] += df_i.iloc[dom_index[1], df_i.columns.get_loc('FilteredComment')]
                            df_i.iloc[dom_index[0], df_i.columns.get_loc('FilteredLevel')] += df_i.iloc[dom_index[1], df_i.columns.get_loc('FilteredLevel')]
                            df_i.at[dom_index[0], 'FilteredLevel'] = df_i.at[dom_index[0], 'FilteredLevel'].round(2)
                            df_i = df_i.drop(dom_index[1], axis=0)

                            df_i = df_i.drop(['Transmission Owner'], axis=1)
                            df_i = df_i.rename(columns={'Zone': 'LocaleName', 'FilteredLevel': 'PriceLevel', 'FilteredComment': 'PriceComment'})
                            df_i = df_i.sort_values(by='LocaleName', ascending=True)
                            time = df_time['Date'].iloc[0]
                            df_i['FlowMonth'] = time
                            df_i = df_i[['FlowMonth', 'LocaleName', 'PriceLevel', 'PriceComment']]

                            res_per_year = pd.concat([res_per_year, df_i])

                        # second table type after June 2022
                        if output == 'target2':
                            page_text = page.extract_text()
                            if page_text:
                                parts = page_text.split('\n')
                                for part in parts:
                                    if part.startswith('As of '):
                                        temp_df = pd.DataFrame({'Text': [part]})
                            date_str = temp_df['Text'].iloc[0].split(' ')[2]
                            date_str = date_str.split('(')[0]
                            date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                            temp_df.loc[0, 'Formatted Date'] = date_obj.strftime('%Y-%m-%d')

                            df_time = pd.DataFrame(temp_df['Formatted Date'].values, columns=['Date'])
                            df_time_total = pd.concat([df_time_total,df_time])

                            tables = page.extract_tables()

                            data = pd.DataFrame(tables[0][1:])
                            df_i = data

                            df_i = df_i.reset_index(drop=True)
                            df_i.columns = df_i.iloc[0]
                            df_i.columns = df_i.columns.str.replace('\n', '', regex=True).str.replace(' ', '')
                            if ('TransmissionZone' and 'AnnualRevenueRequirement')in df_i.columns:
                                rockland_indices = df_i[df_i['TransmissionZone'] == 'RECO'].index
                                df_i = df_i.loc[:rockland_indices[0]] #
                            if 'TransmissionZoneShortName' in df_i.columns:
                                rockland_indices = df_i[df_i['TransmissionZoneShortName'] == 'RE'].index
                                df_i = df_i.loc[:rockland_indices[0]]
                            df_i = df_i.iloc[1:]

                            if 'TransmissionZoneShortName' in df_i.columns:
                                df_i = df_i.rename(columns={'TransmissionZoneShortName': 'ZoneShort'})
                            elif 'TransmissionZone' in df_i.columns:
                                df_i = df_i.rename(columns={'TransmissionZone': 'ZoneShort'})
                            if 'TransmissionZone' in df_i.columns:
                                df_i = df_i.drop(['TransmissionZone'],axis=1)
                            if 'TranmissionOwnerAnnualTransmissionRevenueRequirement' in df_i.columns:
                                df_i = df_i.rename(columns={'TranmissionOwnerAnnualTransmissionRevenueRequirement':'AnnualRevenueRequirement',
                                                            'TotalAnnualZonalRevenueRequirement':'TotalZonalAnnualRevenueRequirement'})

                            df_i = df_i.drop(['TransmissionOwner', 'AnnualRevenueRequirement'], axis=1)

                            df_i.replace('', np.nan, inplace=True)
                            df_i.replace(' ', np.nan, inplace=True)
                            df_i.fillna('None', inplace=True)
                            df_i.replace('None', np.nan, inplace=True)
                            df_i = df_i.rename(columns={'ZoneShort': 'Zone', 'TotalZonalAnnualRevenueRequirement': 'Comment',
                                               'NetworkIntegrationTransmissionServiceRate($/MW-Year)':'Level'})

                            rows_with_all_none = df_i.isna().all(axis=1)
                            indices_with_all_none = df_i[rows_with_all_none].index
                            df_i = df_i.drop(indices_with_all_none, axis=0)

                            # 2022:
                            df_i.loc[df_i['Zone'] == 'AEC', 'Zone'] = 'AECO'
                            df_i.loc[df_i['Zone'] == 'Dayton', 'Zone'] = 'DAY'
                            df_i.loc[df_i['Zone'] == 'DL', 'Zone'] = 'DUQ'
                            df_i.loc[df_i['Zone'] == 'Dominion', 'Zone'] = 'DOM'
                            df_i.loc[df_i['Zone'] == 'RE', 'Zone'] = 'RECO'

                            # Locale = 'ComEd':
                            df_i.loc[df_i['Zone'] == 'ComEd', 'Zone'] = 'COMED'
                            # Locale = 'PENELEC':
                            df_i.loc[df_i['Zone'] == 'PENELEC', 'Comment'] = df_i.loc[df_i['Zone'] == 'ME', 'Comment'].values[0]
                            # make all str to be float:
                            df_i['Comment'] = df_i['Comment'].replace('[\$, ]', '', regex=True).astype(float)
                            df_i['Level'] = df_i['Level'].replace('[\$, ]', '', regex=True).astype(float)
                            df_i['Zone'] = df_i['Zone'].ffill()

                            consolidated_rows = []  # List to hold all the data frames or series
                            for zone in df_i['Zone'].unique():
                                temp_df = df_i[df_i['Zone'] == zone]
                                sum_data = temp_df.sum(numeric_only=True, min_count=1)
                                result_row = pd.Series(sum_data, index=temp_df.columns[1:])
                                result_row['Zone'] = zone
                                consolidated_rows.append(pd.DataFrame([result_row]))

                            new_df_i = pd.concat(consolidated_rows, ignore_index=True)
                            new_df_i['Level'] = new_df_i['Level'].round(2)
                            new_df_i = new_df_i[['Zone', 'Level', 'Comment']]

                            # MAIT = ME + PENELEC (only level values)
                            new_df_i.loc[new_df_i['Zone'] == 'ME', 'Zone'] = 'MAIT'
                            new_df_i = new_df_i[new_df_i['Zone'] != 'PENELEC']

                            new_df_i = new_df_i.rename(columns={'Zone': 'LocaleName', 'Level': 'PriceLevel', 'Comment': 'PriceComment'})

                            new_df_i['FlowMonth'] = df_time['Date'].iloc[0]
                            new_df_i = new_df_i[['FlowMonth', 'LocaleName', 'PriceLevel', 'PriceComment']]
                            res_per_year = pd.concat([res_per_year, new_df_i])

                    # deal with missing dates at the end
                    res_per_year = res_per_year.sort_values(by='FlowMonth', ascending=True)
                    df_time_total = df_time_total.sort_values(by='Date',ascending=True)
                    df_time_total['Date'] = pd.to_datetime(df_time_total['Date'])
                    if year != str(current_year):
                        all_months = pd.date_range(start=f'{year}-01-01', end=f'{year}-12-01', freq='MS')
                    else:
                        all_months = pd.date_range(start=df_time['Date'].iloc[0], end=f'{year}-12-01', freq='MS')

                    existing_months = df_time_total['Date'].unique()
                    existing_months = existing_months.strftime('%Y-%m-%d')
                    missing_months = all_months.difference(existing_months)
                    missing_months = missing_months.strftime('%Y-%m-%d')

                    new_rows = pd.DataFrame()
                    year_total = res_per_year.copy()

                    for i, j in enumerate(existing_months):
                        temp = year_total[year_total['FlowMonth'] == j]
                        next_month = existing_months[i + 1] if i + 1 < len(existing_months) else None
                        for missing_month in missing_months:
                            if next_month and missing_month > j and missing_month < next_month:
                                temp_copy = temp.copy()
                                temp_copy['FlowMonth'] = missing_month
                                new_rows = pd.concat([new_rows, temp_copy])
                            elif not next_month:
                                if missing_month > j:
                                    temp_copy = temp.copy()
                                    temp_copy['FlowMonth'] = missing_month
                                    new_rows = pd.concat([new_rows, temp_copy])

                    year_total = pd.concat([year_total, pd.DataFrame(new_rows)], ignore_index=True)
                    year_total = year_total.sort_values(by=['FlowMonth','LocaleName'], ascending=[True,True])

                    if year != str(current_year):
                        res_total = pd.concat([res_total, year_total])
                    else:
                        start_date = year_total.iloc[0]['FlowMonth']
                        res_total_before_date = res_total.loc[res_total['FlowMonth'] < start_date]
                        res_total = pd.concat([res_total_before_date, year_total])

        res_total['PriceUnit'] = 'MW-Year'
        res_total['PriceType'] = 'NITSRevenue'
        res_total['PriceComment'] = res_total['PriceComment'].astype(str)
        res_total['FlowMonth'] = pd.to_datetime(res_total['FlowMonth'])
        res_total = res_total[['FlowMonth','LocaleName','PriceLevel','PriceType','PriceUnit','PriceComment']]

        # Handle format issue for ComEd
        res_total.loc[res_total['LocaleName'] == 'ComEd', 'LocaleName'] = 'COMED'

        # Save processed data
        data_processed = {
            'FlowMonth': res_total['FlowMonth'].dt.strftime('%Y-%m-%d'),
            'LocaleName': res_total['LocaleName'],
            'PriceLevel': res_total['PriceLevel'],
            'PriceType': res_total['PriceType'],
            'PriceUnit': res_total['PriceUnit'],
            'PriceComment': res_total['PriceComment'].astype(float)
        }
        df_processed = pd.DataFrame(data_processed)

        # Data Check
        logging.info('Checking Data')
        warning_messages = data_check(df_processed, warning_messages)

        # Data Crosscheck
        conn,engine = dbop.db_connect('LoadStaging')
        df_database = pd.read_sql('select * from [dbo].[Load_PJMMonthlyPriceHist] where PriceType = \'NITSRevenue\'',conn)
        conn.close()
        df_database['FlowMonth'] = pd.to_datetime(df_database['FlowMonth']).dt.strftime('%Y-%m-%d')

        # Data Crosscheck
        df_merged = df_processed.merge(df_database, on=['FlowMonth', 'LocaleName', 'PriceType', 'PriceUnit'], how='outer',
                                       indicator=True, suffixes=('_output', '_db'))
        df_match = df_merged[df_merged['_merge'] == 'both'].copy()
        df_match['PriceLevel_diff'] = df_match['PriceLevel_output'] - df_match['PriceLevel_db']
        df_match['PriceComment_diff'] = df_match['PriceComment_output'] - df_match['PriceComment_db'].astype(float)

        df_mismatch = df_match[~((abs(df_match['PriceComment_diff']) < 1) & (abs(df_match['PriceLevel_diff']) < 1))]

        if not df_mismatch.empty:
            for index, row in df_mismatch.iterrows():
                missing_time = row['FlowMonth']
                locale_name = row['LocaleName']
                print(f'Find data mismatch in {missing_time} {locale_name}')
                warning_messages.append(
                    f'Data Mismatch Warning: Find data mismatch in {missing_time} {locale_name}')

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
            'PriceType': df_update['PriceType'],
            'PriceUnit': df_update['PriceUnit'],
            'PriceComment': df_update['PriceComment_output']
        }
        df_update = pd.DataFrame(data_update)

        os.makedirs(f"{base_path}/NITS_data/output_data", exist_ok=True)
        update_file_output_path = f"{base_path}/NITS_data/output_data/NITS_update.csv"
        df_update.to_csv(update_file_output_path, index=False)

        processed_file_output_path = f"{base_path}/NITS_data/output_data/NITS_processed.csv"
        df_processed.to_csv(processed_file_output_path, index=False)
        print("Final output is successfully saved!")

        # Save warning message
        if not warning_messages:
            warning_messages.append('No Warning in ETL Process')
        warning_dir = f"{base_path}/NITS_data"
        filename = f'NITS_etl_warnings.txt'
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
    logging.info('NITS ETL Process Complete')




