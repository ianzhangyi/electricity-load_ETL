"""
Script Purpose:
This script is designed to extract, transform, and load (ETL) data from the DUKE Ohio (DUKE_OH) energy dataset.
The primary objective is to ensure data integrity and consistency before loading it into the target databases.
The script fetches data from the DUKE Ohio energy data website (https://www.duke-energyohiocbp.com/Documents/LoadandOtherData.aspx)
and processes it to address various data quality issues.

Input Data:
1. Deration Factor
    Data Source: Downloaded directly from database [dbo].[Load_PJMHourlyDerationFactor] where LocaleName = 'DEOK'.
    Data Description: Provides hourly deration factors starting from 2015-06-01, used to derate hourly volume.
    Data Processing: Missing values exist between 2024-05-17 and 2024-05-19. These values are kept without further
    processing. Hourly volume data with missing deration factors are not derated.
    The processed data is saved for use in subsequent processes.



2. Hourly Volume & Customer Counts, and PIPP Hourly Volume  & Customer Counts
    Data Source: Downloaded from Load and Other Data (duke-energyohiocbp.com).
    Data Description: Provides hourly volume and customer counts for each year starting from 2012. The hourly volume
    data for PIPP starts from 2009. PIPP customer counts for PIPP is provided in monthly basis. Starting with
    October 1, 2018, PIPP Customer load is being classified as Switched usage rather than Standard Offer usage.
    Data Processing:
       1. Convert EST time to UTC and change the display time from hour end to hour beginning.
       2. Change the unit of volume from KW to MW, keeping three decimal places.
       3. Derate the volume using the deration factor. Since deration factors are only available after 2015-06-01 and
       have missing values between 2024-05-17 and 2024-05-19, the hourly volume for these date is not included in the
       output file.
       4. Aggregate customer counts to monthly customer account by taking average of daily data, round the result to
       integer.
       5. Deal with missing values on 2018-06-01  with the moving average of 7 prior and 7 post data points.

3. PLC and NSPL Volume

    Data Source: Downloaded from Load and Other Data (duke-energyohiocbp.com).
    Data Description: Provides daily PLC and NSPL volume in MW starting from 2012. The NSPL data provided is scaled,
    while PLC is not scaled.
    Data Processing:
        1. For PIPP data prior to 2016, fill all dates of a month with the first day's figure.
        2. According to the data source, NSPL data is scaled, while PLC data is not scaled.
        3. Deal with one missing value in June 2015 with the moving average of 7 prior and 7 post data points.


Output Data:

1. Hourly Volume
    Data Description: A combined table of hourly volume for residential, commercial & industrial customers for SSO,
    CRES, and PIPP. The hourly volume is in MW and is derated.

2. Customer Counts
    Data Description: An aggregated table from daily customer counts by taking average and rounding to integer.

3. PLC and NSPL Volume
    Data Description: A combined table of daily PLC and NSPL volume for SSO, CRES, and PIPP. The volume is in MW and
    only NSPL is scaled.

Usage:
1. Undate the base_path
2. Update the data_extract to select the way of loading data: download from website ('True') or use local data ('False')
3. If using local data, change the file names in main function to the file names of local files.
4. Put deration factor file under the base_path, and update the name of deration factor file.
5. Make sure ETL report template is in base_path
6. Run the script to process the data and report warnings

Automation:
1. Use  windows task scheduler for automation

"""


import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import urllib.parse
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from jinja2 import Environment, FileSystemLoader
import base64
import datetime
import numpy as np


# Fetch and parse the HTML content
def fetch_html_content(url):
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.content, 'html.parser')


# Find all links to Excel files
def find_excel_links(soup, base_url, keywords):
    links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.xls') or href.endswith('.xlsx'):
            if any(keyword in href for keyword in keywords):
                links.append(urllib.parse.urljoin(base_url, href))
    return links


# Download the file using the final URL
def download_file(final_url, new_file_path):
    response = requests.get(final_url, stream=True)
    response.raise_for_status()
    with open(new_file_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


# Handle redirections and get the final URL
def get_final_url(initial_url, keywords):
    session = requests.Session()
    response = session.get(initial_url, allow_redirects=True)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Check if it's redirecting to an Office Viewer link
    office_viewer_base = 'https://view.officeapps.live.com/op/view.aspx?src='
    if office_viewer_base in response.url:
        return urllib.parse.unquote(response.url.split('src=')[1])

    # Otherwise, look for the direct download link in the HTML
    for link in soup.find_all('a', href=True):
        if any(keyword in link['href'] for keyword in keywords):
            return urllib.parse.urljoin(initial_url, link['href'])

    # If no download link found, return the final URL after redirection
    return response.url


# Process the found links and download the files
def process_and_download_links(excel_links, keywords, download_path):
    downloaded_files = {}
    keyword_counts = {keyword: 0 for keyword in keywords}

    for link in excel_links:
        file_name = os.path.basename(link)
        keyword_in_file = None

        for keyword in keywords:
            if keyword in file_name:
                keyword_in_file = keyword
                break

        if keyword_in_file:
            keyword_counts[keyword_in_file] += 1
            new_file_name = f"{keyword_in_file}_duke_oh_{keyword_counts[keyword_in_file]}.xls" if file_name.endswith(
                '.xls') else f"{keyword_in_file}_duke_oh_{keyword_counts[keyword_in_file]}.xlsx"
            new_file_path = os.path.join(download_path, new_file_name)

            # Get the final URL after handling redirections
            final_url = get_final_url(link, keywords)

            # Validate the content type
            file_response = requests.get(final_url, stream=True)
            file_response.raise_for_status()
            content_type = file_response.headers['Content-Type']
            if content_type in ['application/vnd.ms-excel',
                                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']:
                # Download the file from the final URL
                download_file(final_url, new_file_path)
                print(f'Downloaded and renamed: {new_file_path}')
                downloaded_files[keyword_in_file] = new_file_path
            else:
                print(f'Warning: The URL {final_url} does not point to a valid Excel file. '
                      f'Content type is {content_type}. Skipping download.')

            # Print a warning if more than one file with the same keyword is found
            if keyword_counts[keyword_in_file] > 1:
                print(f'Warning: More than one file with the keyword "{keyword_in_file}" has been found and renamed.')

    # Print a warning if a keyword is not found
    for keyword, count in keyword_counts.items():
        if count == 0:
            print(f'Warning: No files found with the keyword "{keyword}".')

    return downloaded_files


def load_deration_factor(deration_factor_path):
    deration_factor = pd.read_csv(deration_factor_path)

    return deration_factor


def load_volume_data(file_path):
    file_sheets = pd.read_excel(file_path, sheet_name=None)
    for sheet_name in file_sheets:
        if sheet_name == 'Validation':
            continue

        initial_rows = pd.read_excel(file_path, sheet_name=sheet_name, nrows=5)
        if pd.notna(initial_rows.iloc[0]).sum() >= pd.notna(initial_rows.iloc[1]).sum():
            header_row = 1
        else:
            header_row = 2

        file_sheets[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)

    return file_sheets


def load_daily_data(file_path):
    file_sheets = pd.read_excel(file_path, sheet_name=None)
    daily_volume_sheets = {}
    for sheet_name in file_sheets:
        if 'PLC' in sheet_name:
            daily_volume_sheets['PLC'] = pd.read_excel(file_path, sheet_name=sheet_name, header=1)
        elif 'NSPL' in sheet_name:
            daily_volume_sheets["NSPL"] = pd.read_excel(file_path, sheet_name=sheet_name, header=1)

    return daily_volume_sheets


def load_pipp_data(pipp_file_path):
    pipp_sheets = {}
    for path_name in pipp_file_path:
        file_sheets = pd.read_excel(pipp_file_path[path_name], sheet_name=None)

        for sheet_name in file_sheets:
            if sheet_name == 'PIPP_RS':
                file_sheets[sheet_name] = pd.read_excel(pipp_file_path[path_name], sheet_name=sheet_name, header=8)
            elif sheet_name == 'Monthly':
                header_row = 1 if path_name == 'prior18' else 0
                file_sheets[sheet_name] = pd.read_excel(pipp_file_path[path_name], sheet_name=sheet_name,
                                                        header=header_row)
        pipp_sheets[path_name] = file_sheets
    return pipp_sheets


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


def process_deration_factor(deration_factor, edc_name):
    deration_factor['Datetime_beginning_utc'] = deration_factor['Datetime_beginning_utc'].astype(str)
    deration_factor['Datetime_beginning_utc'] = pd.to_datetime(deration_factor['Datetime_beginning_utc'])

    deration_factor_df = pd.DataFrame({
        'Datetime_beginning_utc': deration_factor['Datetime_beginning_utc'],
        'EDCName': edc_name,
        'DerationFactor': deration_factor['DerationFactor']
    })

    return deration_factor_df


def process_hourly_load_data(sheets, edc_name, deration_factor):
    df_volume_list = []
    df_customer_count_list = []

    # Check Excel data format to match the columns
    def check_df_type(df):
        if all(col in df.columns for col in ['DS', 'DM', 'DP', 'TS', 'OTHER']):
            customer_class_map = {
                'RES': ['RS'],
                'COM': ['DM', 'OTHER'],
                'IND': ['DS', 'DP', 'TS']
            }
            if '#RS' in df.columns:
                customer_count_class_map = {
                    'RES': ['#RS'],
                    'COM': ['#DM', '#OTHER'],
                    'IND': ['#DS', '#DP', '#TS']
                }
            else:
                customer_count_class_map = {
                    'RES': ['# RS'],
                    'COM': ['# DM', '# OTHER'],
                    'IND': ['# DS', '# DP', '# TS']
                }
        elif all(col in df.columns for col in ['DS + DP + TS', 'DM', 'OTHER']):
            customer_class_map = {
                'RES': ['RS'],
                'COM': ['DM', 'OTHER'],
                'IND': ['DS + DP + TS']
            }
            if '#RS' in df.columns:
                customer_count_class_map = {
                    'RES': ['#RS'],
                    'COM': ['#DM', '#OTHER'],
                    'IND': ['#DS + DP + TS']
                }
            else:
                customer_count_class_map = {
                    'RES': ['# RS'],
                    'COM': ['# DM', '# OTHER'],
                    'IND': ['# DS + DP + TS']
                }
        else:
            print('Warning: Unknown Dataframe Type')

        return customer_class_map, customer_count_class_map

    def adjust_hour(hour_ending):
        if isinstance(hour_ending, int):
            hour = hour_ending - 1
        else:
            hour = int(hour_ending[:2]) - 1
        return hour

    # Define a function to check for header rows
    def is_header(row):
        return list(row) == header

    for sheet_name, sheet_df in sheets.items():
        if sheet_name in ('Total Usage', 'Standard Offer Usage', 'Switched Usage'):
            sheet_df = sheet_df.copy()
            # Drop rows that are the same as the header
            header = list(sheet_df.columns)
            sheet_df = sheet_df[~sheet_df.apply(is_header, axis=1)]

            customer_class_map, customer_count_class_map = check_df_type(sheet_df)

            # Fill forward the 'REPORT DAY' values
            sheet_df['REPORT DAY'] = sheet_df['REPORT DAY'].ffill()
            # Remove rows where 'HOUR ENDING' is NaN
            sheet_df = sheet_df.dropna(subset=['HOUR ENDING'])

            # Convert 'REPORT DAY' and 'HOUR ENDING' columns to datetime format
            sheet_df['REPORT DAY'] = pd.to_datetime(sheet_df["REPORT DAY"].astype(str))

            # Adjust format of Hour Ending, and subtract one hour
            sheet_df['HOUR ENDING'] = sheet_df['HOUR ENDING'].apply(adjust_hour)

            # Combine date and hour, convert to UTC
            datetime_beginning_est = sheet_df['REPORT DAY'] + pd.to_timedelta(sheet_df['HOUR ENDING'], unit="h")
            sheet_df["Datetime_beginning_utc"] = datetime_beginning_est.dt.tz_localize('EST').dt.tz_convert('UTC')

            # Merge with deration_factor
            merged_df = sheet_df.merge(deration_factor, left_on='Datetime_beginning_utc',
                                       right_on='Datetime_beginning_utc')

            for target_customer_class in customer_class_map:
                merged_df['target_volume'] = merged_df[customer_class_map[target_customer_class]].sum(axis=1)

                processed_volume_data = {
                    'Datetime_beginning_utc': merged_df['Datetime_beginning_utc'],
                    'EDCName': edc_name,
                    'CustomerClass': target_customer_class,
                    'VolumeType': 'Wholesale_Derated',
                    'EGS_HourlyVolume': merged_df['target_volume'] / 1000 * (
                                1 - merged_df['DerationFactor']) if sheet_name == 'Switched Usage' else 0,
                    'Default_HourlyVolume': merged_df['target_volume'] / 1000 * (
                                1 - merged_df['DerationFactor']) if sheet_name == 'Standard Offer Usage' else 0,
                    'Eligible_HourlyVolume': merged_df['target_volume'] / 1000 * (
                                1 - merged_df['DerationFactor']) if sheet_name == 'Total Usage' else 0,
                    'VolumeComment': ''
                }
                df_volume_list.append(pd.DataFrame(processed_volume_data))

                # Process monthly customer counts
                daily_df = sheet_df.drop_duplicates(subset=['REPORT DAY']).copy()
                daily_df['target_customer_count'] = daily_df[customer_count_class_map[target_customer_class]].sum(
                    axis=1)
                processed_customer_count_data = {
                    'FlowMonth': daily_df['REPORT DAY'],
                    'EDCName': edc_name,
                    'CustomerClass': target_customer_class,
                    'VolumeType': 'CustomerCount',
                    'EGS_MonthlyVolume': daily_df['target_customer_count'] if sheet_name == 'Switched Usage' else 0,
                    'Default_MonthlyVolume': daily_df[
                        'target_customer_count'] if sheet_name == 'Standard Offer Usage' else 0,
                    'Eligible_MonthlyVolume': daily_df['target_customer_count'] if sheet_name == 'Total Usage' else 0,
                    'VolumeComment': ''
                }
                df_customer_count_list.append(pd.DataFrame(processed_customer_count_data))

    # Concat DataFrame across different sheets
    combined_volume_df = pd.concat(df_volume_list, ignore_index=True)
    combined_volume_df = combined_volume_df.groupby(['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType',
                                                     'VolumeComment'], as_index=False).sum()

    combined_customer_count_df = pd.concat(df_customer_count_list, ignore_index=True)
    combined_customer_count_df = combined_customer_count_df.groupby(['FlowMonth', 'EDCName', 'CustomerClass',
                                                                     'VolumeType', 'VolumeComment'],
                                                                    as_index=False).sum()

    # Take average of daily customer counts as monthly customer counts
    combined_customer_count_df['FlowMonth'] = combined_customer_count_df['FlowMonth'].dt.strftime('%Y-%m-%d')
    combined_customer_count_df = combined_customer_count_df.groupby(['FlowMonth', 'EDCName', 'CustomerClass',
                                                                     'VolumeType', 'VolumeComment'],
                                                                    as_index=False).mean()
    combined_customer_count_df[['EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume']] = \
        combined_customer_count_df[['EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume']].astype(int)
    return combined_volume_df, combined_customer_count_df


def process_pipp_data(pipp_sheets, edc_name, deration_factor):
    # Process Hourly Volume
    pipp_volume_list = []
    for pipp_sheets_name in pipp_sheets:

        sheets = pipp_sheets[pipp_sheets_name]
        pipp_volume_df = sheets['PIPP_RS']
        pipp_volume_df.columns = pipp_volume_df.columns.str.strip()
        pipp_volume_df['Year'] = pipp_volume_df['Year'].astype(int)
        pipp_volume_df['Month'] = pipp_volume_df['Month'].astype(int)
        pipp_volume_df['Day'] = pipp_volume_df['Day'].astype(int)
        pipp_volume_df['Hr Ending'] = pipp_volume_df['Hr Ending'].astype(int) - 1
        datetime_beginning_est = pd.to_datetime(pipp_volume_df[['Year', 'Month', 'Day', 'Hr Ending']].astype(str)
                                                .agg('-'.join, axis=1), format='%Y-%m-%d-%H')
        pipp_volume_df['Datetime_beginning_utc'] = datetime_beginning_est.dt.tz_localize('EST').dt.tz_convert('UTC')

        merged_volume_df = pipp_volume_df.merge(deration_factor, left_on='Datetime_beginning_utc',
                                                right_on='Datetime_beginning_utc')
        # Delete missing values in pipp data
        merged_volume_df = merged_volume_df[~(merged_volume_df['PIPP Total'] == 0)]

        for df_type in ["merged_volume_df_prior18", "merged_volume_df_post18"]:
            if df_type == 'merged_volume_df_prior18':
                df = merged_volume_df[merged_volume_df['Datetime_beginning_utc'] < '2018-10-01']
            else:
                df = merged_volume_df[merged_volume_df['Datetime_beginning_utc'] >= '2018-10-01']

            pipp_volume_data = {
                'Datetime_beginning_utc': df['Datetime_beginning_utc'],
                'EDCName': edc_name,
                'CustomerClass': 'PIPP',
                'VolumeType': 'Wholesale_Derated',
                'EGS_HourlyVolume': df['PIPP Total'] / 1000 * (
                            1 - df['DerationFactor']) if df_type == 'merged_volume_df_post18' else 0,
                'Default_HourlyVolume': df['PIPP Total'] / 1000 * (
                            1 - df['DerationFactor']) if df_type == 'merged_volume_df_prior18' else 0,
                'Eligible_HourlyVolume': df['PIPP Total'] / 1000 * (1 - df['DerationFactor']),
                'VolumeComment': ''
            }

            pipp_volume_list.append(pd.DataFrame(pipp_volume_data))

    pipp_volume_processed = pd.concat(pipp_volume_list, ignore_index=True)

    # Process Customer Counts
    pipp_customer_count_list = []
    for pipp_sheets_name in pipp_sheets:

        sheets = pipp_sheets[pipp_sheets_name]
        pipp_df = sheets['Monthly']
        pipp_df = pipp_df.dropna(subset=pipp_df.columns[0])
        pipp_df = pipp_df.copy()
        indices_to_drop = []
        for index, row in pipp_df.iterrows():
            # Check if the first column contains a year
            if not isinstance(row.iloc[0], str):
                year = row.iloc[0]
                indices_to_drop.append(index)
            else:
                pipp_df.loc[index, 'FlowMonth'] = pd.to_datetime(f'{year} {row.iloc[0]}', format='%Y %b').strftime(
                    '%Y-%m-01')
        pipp_df = pipp_df.drop(indices_to_drop)
        pipp_df = pipp_df.dropna(subset=['PIPP Customers'])

        for df_type in ["pipp_df_prior18", "pipp_df_post18"]:
            if df_type == 'pipp_df_prior18':
                df = pipp_df[pipp_df['FlowMonth'] < '2018-10-01']
            else:
                df = pipp_df[pipp_df['FlowMonth'] >= '2018-10-01']

            processed_pipp_customer_count = {
                'FlowMonth': df['FlowMonth'],
                'EDCName': edc_name,
                'CustomerClass': 'PIPP',
                'VolumeType': 'CustomerCount',
                'EGS_MonthlyVolume': df['PIPP Customers'].astype(int) if df_type == 'pipp_df_post18' else 0,
                'Default_MonthlyVolume': df['PIPP Customers'].astype(int) if df_type == 'pipp_df_prior18' else 0,
                'Eligible_MonthlyVolume': df['PIPP Customers'].astype(int),
                'VolumeComment': ''
            }

            pipp_customer_count_list.append(pd.DataFrame(processed_pipp_customer_count))
    pipp_customer_count_processed = pd.concat(pipp_customer_count_list, ignore_index=True)
    pipp_customer_count_processed = pipp_customer_count_processed.copy()
    pipp_customer_count_processed['FlowMonth'] = pd.to_datetime(pipp_customer_count_processed['FlowMonth'], format='%Y-%m-%d')
    # Expand monthly PIPP Customer Count to daily data
    def expand_to_daily(df):
        # List to store daily data
        daily_data = []

        for _, row in df.iterrows():
            # Get the first and last date of the month
            start_date = row['FlowMonth']
            end_date = (start_date + pd.offsets.MonthEnd(0))

            # Generate a date range for the entire month
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')

            # Create new rows for each day in the date range
            for date in date_range:
                daily_data.append({'FlowMonth': date.strftime('%Y-%m-%d'), 'EDCName': row['EDCName'], 'CustomerClass': row['CustomerClass'],
                                   'VolumeType': row['VolumeType'], 'EGS_MonthlyVolume': row['EGS_MonthlyVolume'],
                                   'Default_MonthlyVolume': row['Default_MonthlyVolume'],
                                   'Eligible_MonthlyVolume': row['Eligible_MonthlyVolume'],
                                   'VolumeComment': row['VolumeComment']})

        # Create a new DataFrame from the daily data
        df_daily = pd.DataFrame(daily_data)

        return df_daily

    pipp_customer_count_processed = expand_to_daily(pipp_customer_count_processed)
    return pipp_volume_processed, pipp_customer_count_processed


def combine_monthly_data(monthly_customer_count_list):
    combined_df = pd.concat(monthly_customer_count_list, ignore_index=True)
    # Subtract PIPP customer counts from RES customer counts
    res_volume = combined_df[combined_df['CustomerClass'] == 'RES']
    combined_df = combined_df[~(combined_df['CustomerClass'] == 'RES')]
    pipp_volume = combined_df[combined_df['CustomerClass'] == 'PIPP']
    merged_volume = res_volume.merge(pipp_volume, left_on='FlowMonth', right_on='FlowMonth', suffixes=('', '_pipp'))
    merged_volume['EGS_MonthlyVolume'] -= merged_volume['EGS_MonthlyVolume_pipp']
    merged_volume['Default_MonthlyVolume'] -= merged_volume['Default_MonthlyVolume_pipp']

    res_volume_processed = merged_volume[['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType',
                                          'EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume',
                                          'VolumeComment']]

    monthly_volume_processed = pd.concat([res_volume_processed, combined_df], ignore_index=True)
    monthly_volume_processed['Eligible_MonthlyVolume'] = monthly_volume_processed['EGS_MonthlyVolume'] + \
                                                         monthly_volume_processed['Default_MonthlyVolume']
    return monthly_volume_processed


def combine_hourly_data(hourly_load_list):
    combined_df = pd.concat(hourly_load_list, ignore_index=True)
    # Subtract PIPP customer counts from RES customer counts
    res_volume = combined_df[combined_df['CustomerClass'] == 'RES']
    combined_df = combined_df[~(combined_df['CustomerClass'] == 'RES')]
    pipp_volume = combined_df[combined_df['CustomerClass'] == 'PIPP']
    merged_volume = res_volume.merge(pipp_volume, left_on='Datetime_beginning_utc', right_on='Datetime_beginning_utc',
                                     suffixes=('', '_pipp'))
    merged_volume['EGS_HourlyVolume'] -= merged_volume['EGS_HourlyVolume_pipp']
    merged_volume['Default_HourlyVolume'] -= merged_volume['Default_HourlyVolume_pipp']
    merged_volume['Eligible_HourlyVolume'] -= merged_volume['Eligible_HourlyVolume_pipp']

    res_volume_processed = merged_volume[['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType',
                                          'EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume',
                                          'VolumeComment']]

    hourly_volume_processed = pd.concat([res_volume_processed, combined_df], ignore_index=True)
    hourly_volume_processed['Eligible_HourlyVolume'] = hourly_volume_processed['EGS_HourlyVolume'] + \
                                                       hourly_volume_processed['Default_HourlyVolume']
    return hourly_volume_processed


def handle_hourly_missing_data(hourly_volume_processed):
    # handle missing data in RES customer class
    res_df = hourly_volume_processed[(hourly_volume_processed['CustomerClass'] == 'RES')]
    pipp_df = hourly_volume_processed[(hourly_volume_processed['CustomerClass'] == 'PIPP')]
    ci_df = hourly_volume_processed[((hourly_volume_processed['CustomerClass'] != 'RES') & (hourly_volume_processed['CustomerClass'] != 'PIPP'))]

    df_list = [ci_df]
    # Handle missing values in res
    res_df = res_df.copy()
    res_df['Datetime_beginning_utc'] = pd.to_datetime(res_df['Datetime_beginning_utc'])
    complete_range = pd.date_range(start=res_df['Datetime_beginning_utc'].min(), end=res_df['Datetime_beginning_utc'].max(), freq='H')
    missing_dates_hours = complete_range[~complete_range.isin(res_df['Datetime_beginning_utc'])]
    missing_value_df = pd.DataFrame({'Datetime_beginning_utc': missing_dates_hours,
                                     'EDCName': 'OH_DUKE',
                                     'CustomerClass': 'RES',
                                     'VolumeType': 'Wholesale_Derated',
                                     'EGS_HourlyVolume': np.nan,
                                     'Default_HourlyVolume': np.nan,
                                     'Eligible_HourlyVolume': np.nan,
                                     'VolumeComment': ''})
    res_df_cleaned = pd.concat([missing_value_df, res_df], ignore_index=True).sort_values('Datetime_beginning_utc').reset_index(drop=True)

    # Fill in missing values with average of rolling window
    def calculate_rolling_avg(values):
        valid_values = values[~np.isnan(values)]
        if len(valid_values) > 0:
            return valid_values.mean()
        else:
            return np.nan

    # Apply the rolling window and fill na using a centered window for each column
    for column in ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']:
        rolling_avg_result = res_df_cleaned[column].rolling(window=7, center=True, min_periods=1).apply(lambda x: calculate_rolling_avg(x), raw=True)
        res_df_cleaned[column] = res_df_cleaned[column].combine_first(rolling_avg_result)

    df_list.append(res_df_cleaned)

    # Handle missing values in pipp
    pipp_df = pipp_df.copy()
    pipp_df['Datetime_beginning_utc'] = pd.to_datetime(pipp_df['Datetime_beginning_utc'])
    complete_range = pd.date_range(start=pipp_df['Datetime_beginning_utc'].min(), end=pipp_df['Datetime_beginning_utc'].max(), freq='H')
    missing_dates_hours = complete_range[~complete_range.isin(res_df['Datetime_beginning_utc'])]

    missing_value_df = pd.DataFrame({'Datetime_beginning_utc': missing_dates_hours,
                                     'EDCName': 'OH_DUKE',
                                     'CustomerClass': 'PIPP',
                                     'VolumeType': 'Wholesale_Derated',
                                     'EGS_HourlyVolume': np.nan,
                                     'Default_HourlyVolume': np.nan,
                                     'Eligible_HourlyVolume': np.nan,
                                     'VolumeComment': ''})
    pipp_df_cleaned = pd.concat([missing_value_df, pipp_df], ignore_index=True).sort_values('Datetime_beginning_utc').reset_index(drop=True)

    # Apply the rolling window and fill na using a centered window for each column
    for column in ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']:
        rolling_avg_result = pipp_df_cleaned[column].rolling(window=7, center=True, min_periods=1).apply(lambda x: calculate_rolling_avg(x), raw=True)
        pipp_df_cleaned[column] = pipp_df_cleaned[column].combine_first(rolling_avg_result)

    df_list.append(pipp_df_cleaned)

    # Combine hourly volume data
    hourly_volume_cleaned = pd.concat(df_list, ignore_index=True)
    hourly_volume_cleaned = hourly_volume_cleaned.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'], ignore_index=True)
    return hourly_volume_cleaned


def process_daily_volume_data(daily_load_sheet, edc_name):
    df_list = []
    for sheet_name in daily_load_sheet:
        volume_df = daily_load_sheet[sheet_name]
        volume_df['Date'] = pd.to_datetime(volume_df['Date']).dt.strftime('%Y-%m-%d')
        blended_df = pd.DataFrame({
            'FlowDate': volume_df['Date'],
            'EDCName': edc_name,
            'CustomerClass': 'Blended',
            'VolumeType': f'{sheet_name}_Scaled',
            'EGS_DailyVolume': volume_df[f'Switched {sheet_name}'].fillna(0).astype(float),
            'Default_DailyVolume': volume_df[f'SSO {sheet_name}'].fillna(0).astype(float),
            'Eligible_DailyVolume': volume_df[f'SSO {sheet_name}'].fillna(0).astype(float) + volume_df[f'Switched {sheet_name}'].fillna(0).astype(float),
            'VolumeComment': ''
        })

        df_list.append(blended_df)

        pipp_df = pd.DataFrame({
            'FlowDate': volume_df['Date'],
            'EDCName': edc_name,
            'CustomerClass': 'PIPP',
            'VolumeType': f'{sheet_name}_Scaled',
            'EGS_DailyVolume': 0,
            'Default_DailyVolume': volume_df[f'PIPP {sheet_name}'].fillna(0).astype(float),
            'Eligible_DailyVolume': volume_df[f'PIPP {sheet_name}'].fillna(0).astype(float),
            'VolumeComment': ''
        })
        df_list.append(pipp_df)
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df = combined_df.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'], ignore_index=True)

    return combined_df


def deal_with_monthly_abnormal(monthly_volume_processed):
    monthly_df = monthly_volume_processed.copy()
    for column_name in ['EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume']:
        for customer_class in ['COM', 'IND', 'PIPP', 'RES']:
            prior_value = monthly_df.loc[(monthly_df['FlowMonth'] == '2015-05-31') & (monthly_df['CustomerClass'] == customer_class), column_name].values[0]
            monthly_df.loc[(monthly_df['FlowMonth'] == '2015-06-01') & (monthly_df['CustomerClass'] == customer_class), column_name] = prior_value
            post_value = monthly_df.loc[(monthly_df['FlowMonth'] == '2015-06-03') & (monthly_df['CustomerClass'] == customer_class), column_name].values[0]
            monthly_df.loc[(monthly_df['FlowMonth'] == '2015-06-02') & (monthly_df['CustomerClass'] == customer_class), column_name] = post_value
    return monthly_df


def handle_daily_missing_data(daily_volume_processed):
    pipp_df = daily_volume_processed[(daily_volume_processed['CustomerClass'] == 'PIPP')]
    blended_df = daily_volume_processed[(daily_volume_processed['CustomerClass'] == 'Blended')]

    # Delete 0 entries in PIPP data
    pipp_df_cleaned = pipp_df[~(pipp_df['Eligible_DailyVolume'] == 0)]

    # Delete 0 entries in blended data
    blended_df_cleaned = blended_df[~(blended_df['Eligible_DailyVolume'] == 0)]
    df_list = [pipp_df_cleaned]
    for volume_type in ['PLC_Scaled', 'NSPL_Scaled']:
        df = blended_df_cleaned[blended_df_cleaned['VolumeType'] == volume_type]
        df = df.copy()
        # Ensure the date column is in datetime format
        df['FlowDate'] = pd.to_datetime(df['FlowDate'])
        df.set_index('FlowDate', inplace=True)

        complete_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='D')

        df = df.reindex(complete_range)

        # Define function to calculate average of 7 prior and 7 post data points
        def calculate_moving_average(series, window_size=7):
            return series.rolling(window=window_size, min_periods=1, center=True).mean()

        # Apply the moving average function to fill missing values
        for column in ['EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume']:
            df[column] = df[column].combine_first(calculate_moving_average(df[column], window_size=15))

        # Fill in the unchanged columns for NaN
        df['CustomerClass'] = 'Blended'
        df['VolumeType'] = f'{volume_type}'
        df['EDCName'] = 'OH_DUKE'

        # Reset index to have FlowDate as a column again
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'FlowDate'}, inplace=True)
        # Append df to df_list for combination
        df_list.append(df)

    daily_volume_cleaned = pd.concat(df_list, ignore_index=True)
    daily_volume_cleaned['FlowDate'] = pd.to_datetime(daily_volume_cleaned['FlowDate']).dt.strftime('%Y-%m-%d')
    daily_volume_cleaned = daily_volume_cleaned.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'],
                                                            ignore_index=True)
    return daily_volume_cleaned


def plot_monthly_data(df, output_dir):
    data = df.copy()
    data['Difference between Eligible and sum of EGS and Default'] = data['Eligible_MonthlyVolume'] \
                                                                     - data['Default_MonthlyVolume'] \
                                                                     - data['EGS_MonthlyVolume']
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
        plt.savefig(f'{output_dir}/duke_oh_CustomerCounts_{customer_class}_plot.png')
        plot_path[f'monthly_{customer_class}'] = f'{output_dir}/duke_oh_CustomerCounts_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_hourly_data(df, output_dir):
    data = df.copy()
    data['Difference between Eligible and sum of EGS and Default'] = (data['Eligible_HourlyVolume']
                                                                      - data['Default_HourlyVolume']
                                                                      - data['EGS_HourlyVolume']).astype(float).round(3)
    plot_path = {}
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    columns_to_plot = ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume',
                       'Difference between Eligible and sum of EGS and Default']
    customer_classes = data['CustomerClass'].unique()

    # Set larger font size for all text elements
    plt.rcParams.update({'font.size': 14})

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
        plt.savefig(f'{output_dir}/duke_oh_HourlyLoad_{customer_class}_plot.png')
        plot_path[f'hourly_{customer_class}'] = f'{output_dir}/duke_oh_HourlyLoad_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_daily_data(df, output_dir):
    data = df.copy()
    data['Difference Check'] = (data['Eligible_DailyVolume'] - data['Default_DailyVolume'] - data['EGS_DailyVolume']).round(0)

    plot_path = {}
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    columns_to_plot = ['EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume',
                       'Difference Check']
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
        plt.savefig(f'{output_dir}/duke_oh_PLC_NSPL_{customer_class}_plot.png')
        plot_path[f'daily_{customer_class}'] = f'{output_dir}/duke_oh_PLC_NSPL_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def save_plot_path(plot_path):
    report_plots_path = {
        'Monthly_Customer_Counts_RES': plot_path['monthly_RES'],
        'Monthly_Customer_Counts_COM': plot_path['monthly_COM'],
        'Monthly_Customer_Counts_IND': plot_path['monthly_IND'],
        'Monthly_Customer_Counts_PIPP': plot_path['monthly_PIPP'],
        'Hourly_Volume_RES': plot_path['hourly_RES'],
        'Hourly_Volume_COM': plot_path['hourly_COM'],
        'Hourly_Volume_IND': plot_path['hourly_IND'],
        'Hourly_Volume_PIPP': plot_path['hourly_PIPP'],
        'Daily_Volume_Blended': plot_path['daily_Blended'],
        'Daily_Volume_PIPP': plot_path['daily_PIPP'],
    }
    return report_plots_path


def generate_keystats(final_monthly_df, final_hourly_df, final_daily_df):
    monthly_columns = ['EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume']
    hourly_columns = ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']
    daily_columns = ['EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume']
    report_keystats_table = {
        'Monthly_Customer_Counts_RES': final_monthly_df[final_monthly_df['CustomerClass'] == 'RES'][
            monthly_columns].describe().T,
        'Monthly_Customer_Counts_COM': final_monthly_df[final_monthly_df['CustomerClass'] == 'COM'][monthly_columns].describe().T,
        'Monthly_Customer_Counts_IND': final_monthly_df[final_monthly_df['CustomerClass'] == 'IND'][monthly_columns].describe().T,
        'Monthly_Customer_Counts_PIPP': final_monthly_df[final_monthly_df['CustomerClass'] == 'PIPP'][
            monthly_columns].describe().T,
        'Hourly_Volume_RES': final_hourly_df[final_hourly_df['CustomerClass'] == 'RES'][hourly_columns].describe().T,
        'Hourly_Volume_COM': final_hourly_df[final_hourly_df['CustomerClass'] == 'COM'][hourly_columns].describe().T,
        'Hourly_Volume_IND': final_hourly_df[final_hourly_df['CustomerClass'] == 'IND'][hourly_columns].describe().T,
        'Hourly_Volume_PIPP': final_hourly_df[final_hourly_df['CustomerClass'] == 'PIPP'][hourly_columns].describe().T,
        'Daily_Volume_Blended': final_daily_df[final_daily_df['CustomerClass'] == 'Blended'][
            daily_columns].describe().T,
        'Daily_Volume_PIPP': final_daily_df[final_daily_df['CustomerClass'] == 'PIPP'][daily_columns].describe().T,
    }
    return report_keystats_table


def generate_report(etl_report_output_path, report_keystats_table, report_plots):
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
            'plot': encode_image_to_base64(report_plots[data_type])
        })

    date_today = datetime.datetime.today().strftime('%Y-%m-%d')

    # Data for the template
    report_data = {
        'report_title': 'DUKE Ohio ETL Report',
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


def main(base_path, data_extract=True, deration_factor_filename='DerationFactor_DEOK.csv'):
    # Load Data
    if data_extract:
        # Download data from duke_oh website
        print('Downloading data...')

        # Define the keywords to filter the filenames
        keywords = ['PLC_NSPL',
                    'Hourly_Loads_By_Class_2012',
                    'Hourly_Loads_By_Class_2013',
                    'Hourly_Loads_by_Class_2014',
                    'Hourly_Loads_by_Class_2015',
                    'Hourly_Loads_by_Class_2016',
                    'Hourly_Loads_by_Class_2017',
                    'Hourly_Loads_by_Class_2018',
                    'Hourly_Loads_by_Class_2019',
                    'Hourly_Loads_by_Class_2020',
                    'Hourly_Loads_by_Class_2021',
                    'Hourly_Loads_by_Class_2022',
                    'Hourly_Loads_by_Class_2023',
                    'Hourly_Loads_by_Class_2024',
                    '2009_2018_hourly_pipp',
                    'Hourly_PIPP']

        # Define the URL of the website
        keyword_dict = {'https://www.duke-energyohiocbp.com/Documents/LoadandOtherData.aspx': keywords}

        file_paths = {}
        for url in keyword_dict:
            soup = fetch_html_content(url)
            excel_links = find_excel_links(soup, url, keyword_dict[url])
            downloaded_path = process_and_download_links(excel_links, keyword_dict[url], base_path)
            file_paths.update(downloaded_path)

        # Update input data file path
        hourly_load_file_path = {
            '2012': file_paths['Hourly_Loads_By_Class_2012'],
            '2013': file_paths['Hourly_Loads_By_Class_2013'],
            '2014': file_paths['Hourly_Loads_by_Class_2014'],
            '2015': file_paths['Hourly_Loads_by_Class_2015'],
            '2016': file_paths['Hourly_Loads_by_Class_2016'],
            '2017': file_paths['Hourly_Loads_by_Class_2017'],
            '2018': file_paths['Hourly_Loads_by_Class_2018'],
            '2019': file_paths['Hourly_Loads_by_Class_2019'],
            '2020': file_paths['Hourly_Loads_by_Class_2020'],
            '2021': file_paths['Hourly_Loads_by_Class_2021'],
            '2022': file_paths['Hourly_Loads_by_Class_2022'],
            '2023': file_paths['Hourly_Loads_by_Class_2023'],
            '2024': file_paths['Hourly_Loads_by_Class_2024'],
        }

        daily_volume_file_path = file_paths['PLC_NSPL']
        PIPP_file_path = {
            'prior18': file_paths['2009_2018_hourly_pipp'],
            'post18': file_paths['Hourly_PIPP']
        }

    else:
        # Define local file path
        hourly_load_file_path = {
            '2012': f'{base_path}/Hourly_Loads_By_Class_2012_duke_oh_1.xlsx',
            '2013': f'{base_path}/Hourly_Loads_By_Class_2013_duke_oh_1.xlsx',
            '2014': f'{base_path}/Hourly_Loads_by_Class_2014_duke_oh_1.xlsx',
            '2015': f'{base_path}/Hourly_Loads_by_Class_2015_duke_oh_1.xlsx',
            '2016': f'{base_path}/Hourly_Loads_by_Class_2016_duke_oh_1.xlsx',
            '2017': f'{base_path}/Hourly_Loads_by_Class_2017_duke_oh_1.xlsx',
            '2018': f'{base_path}/Hourly_Loads_by_Class_2018_duke_oh_1.xlsx',
            '2019': f'{base_path}/Hourly_Loads_by_Class_2019_duke_oh_1.xlsx',
            '2020': f'{base_path}/Hourly_Loads_by_Class_2020_duke_oh_1.xlsx',
            '2021': f'{base_path}/Hourly_Loads_by_Class_2021_duke_oh_1.xlsx',
            '2022': f'{base_path}/Hourly_Loads_by_Class_2022_duke_oh_1.xlsx',
            '2023': f'{base_path}/Hourly_Loads_by_Class_2023_duke_oh_1.xlsx',
            '2024': f'{base_path}/Hourly_Loads_by_Class_2024_duke_oh_1.xlsx',
        }
        daily_volume_file_path = f'{base_path}/PLC_NSPL_duke_oh_1.xlsx'

        PIPP_file_path = {
            'prior18': f'{base_path}/2009_2018_hourly_pipp_duke_oh_1.xlsx',
            'post18': f'{base_path}/Hourly_PIPP_duke_oh_1.xlsx'
        }

    # Deration File paths
    deration_factor_path = f'{base_path}/{deration_factor_filename}'

    # Load data
    print('Loading data...')
    deration_factor = load_deration_factor(deration_factor_path)

    hourly_load_sheets = {}
    for year in hourly_load_file_path:
        hourly_load_sheets[year] = load_volume_data(hourly_load_file_path[year])

    daily_load_sheet = load_daily_data(daily_volume_file_path)
    pipp_sheets = load_pipp_data(PIPP_file_path)

    # Process data
    print('Processing data...')
    edc_name = "OH_DUKE"
    deration_factor_processed = process_deration_factor(deration_factor, edc_name)

    hourly_load_list = []
    monthly_customer_count_list = []
    for year in hourly_load_file_path:
        hourly_volume, monthly_volume = process_hourly_load_data(hourly_load_sheets[year], edc_name, deration_factor)
        hourly_load_list.append(hourly_volume)
        monthly_customer_count_list.append(monthly_volume)

    pipp_volume_processed, pipp_customer_count_processed = process_pipp_data(pipp_sheets, edc_name,
                                                                             deration_factor_processed)

    # Combine PIPP data with monthly and hourly data
    hourly_load_list.append(pipp_volume_processed)
    monthly_customer_count_list.append(pipp_customer_count_processed)

    # Combined monthly and hourly data with pipp data
    monthly_volume_processed = combine_monthly_data(monthly_customer_count_list)
    hourly_volume_processed = combine_hourly_data(hourly_load_list)

    # Handle hourly missing values
    hourly_volume_processed = handle_hourly_missing_data(hourly_volume_processed)

    # Sort combined dataframe for better presenting
    hourly_volume_processed = hourly_volume_processed.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'],
                                                                  ignore_index=True)
    monthly_volume_processed = monthly_volume_processed.sort_values(by=['FlowMonth', 'CustomerClass'],
                                                                    ignore_index=True)
    # Process daily volume data and Deal with missing values
    daily_volume_processed = process_daily_volume_data(daily_load_sheet, edc_name)
    daily_volume_processed = handle_daily_missing_data(daily_volume_processed)

    # Handle abnormal data in monthly customer count on 2015-06-01 and 2015-06-02
    monthly_volume_processed = deal_with_monthly_abnormal(monthly_volume_processed)

    # Output path
    output_path = f'{base_path}/output_data'
    # Create output directory if it does not exist
    os.makedirs(output_path, exist_ok=True)

    hourly_output_path = f'{output_path}/duke_oh_HourlyVolume_processed.xlsx'
    monthly_output_path = f'{output_path}/duke_oh_CustomerCount_processed.xlsx'
    daily_output_path = f'{output_path}/duke_oh_NSPL_PLC_processed.xlsx'

    etl_report_output_path = f'{output_path}/ETL_report'

    # Plot data for correction
    print('Saving plot...')
    plot_path = {}
    plot_path.update(plot_monthly_data(monthly_volume_processed, etl_report_output_path))
    plot_path.update(plot_hourly_data(hourly_volume_processed, etl_report_output_path))
    plot_path.update(plot_daily_data(daily_volume_processed, etl_report_output_path))

    # Generate ETL report
    report_plots_path = save_plot_path(plot_path)
    report_keystats_table = generate_keystats(monthly_volume_processed, hourly_volume_processed, daily_volume_processed)
    generate_report(etl_report_output_path, report_keystats_table, report_plots_path)

    # Check Continuity
    print('Checking continuity...')
    for customer_class in ['RES', 'COM', 'IND', 'PIPP']:
        check_continuity(hourly_volume_processed[hourly_volume_processed['CustomerClass'] == customer_class],
                         'Datetime_beginning_utc', 'H', f'{customer_class} hourly volume')
        check_continuity(monthly_volume_processed[monthly_volume_processed['CustomerClass'] == customer_class],
                         'FlowMonth', 'D', f'{customer_class} monthly volume')

    check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == 'PIPP') & (
            daily_volume_processed['VolumeType'] == 'NSPL_Scaled')], 'FlowDate', 'D', 'PIPP NSPL daily volume')
    check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == 'Blended') & (
            daily_volume_processed['VolumeType'] == 'NSPL_Scaled')], 'FlowDate', 'D', 'Blended NSPL daily volume')
    check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == 'PIPP') & (
            daily_volume_processed['VolumeType'] == 'PLC_Scaled')], 'FlowDate', 'D', 'PIPP PLC daily volume')
    check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == 'Blended') & (
            daily_volume_processed['VolumeType'] == 'PLC_Scaled')], 'FlowDate', 'D', 'Blended PLC daily volume')

    # Change PLC to unscaled
    daily_volume_processed.loc[daily_volume_processed['VolumeType'] == 'PLC_Scaled', 'VolumeType'] = 'PLC_unscaled'

    # Save processed data
    print('Saving data...')
    save_processed_data(hourly_volume_processed, hourly_output_path, 'hourly')
    save_processed_data(monthly_volume_processed, monthly_output_path, 'monthly')
    save_processed_data(daily_volume_processed, daily_output_path, 'daily')

if __name__ == "__main__":
    base_path = 'C:\\Users\\5DIntern3_2024\\Work\\DUKE_Ohio'
    data_extract = False
    deration_factor_filename = 'DerationFactor_DEOK.csv'
    main(base_path, data_extract, deration_factor_filename)
