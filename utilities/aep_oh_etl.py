"""
Script Purpose:
This script is designed to extract, transform, and load (ETL) data from the AEP Ohio (AEP_OH) energy dataset.
The primary objective is to ensure data integrity and consistency before loading it into the target databases.
The script fetches data from the AEP Ohio energy data website (https://aepohiocbp.com/index.cfm?s=dataRoom&p=monthly)
and processes it to address various data quality issues.

Input Data:
1. Deration Factor
    Data Source: Downloaded directly from database
    [dbo].[Load_PJMHourlyDerationFactor] where LocaleName = 'AEPOHIO_RESID_AGG'.

2. Hourly Volume for Residential, Commercial & Industrial Customers, and PIPP
    Data Source: Downloaded from AEP_OH website and updated before the 20th of each month.

3. Customer Counts for Residential, Commercial & Industrial Customers, and PIPP
    Data Source: Downloaded from AEP_OH website and updated before the 20th of each month.


4. PLC and NSPL Volume, and PIPP PLC and NSPL Volume
    Data Source: Downloaded from AEP_OH website and updated before the 20th of each month.


5. Unaccounted for Energy (UFE)
    Data Source: Downloaded from AEP_OH website and updated before the 20th of each month.

7. Government Aggregation
    Data Source: Downloaded from AEP_OH website and updated before the 20th of each month.

Output Data:

1. Hourly Volume
    Data Description: A combined table of hourly volume for residential, commercial & industrial customers for SSO,
    CRES, and PIPP. The hourly volume is in MW and is derated.

2. Customer Counts
    Data Description: A combined table of monthly customer counts for residential and commercial & industrial customers
    for SSO, CRES, and PIPP.

3. PLC and NSPL Volume
    Data Description: A combined table of daily PLC and NSPL volume for SSO, CRES, and PIPP. The volume is in MW and is
    scaled.

4. Unaccounted for Energy (UFE)
    Data Description: A table includes hourly UFE volume over customer classes.

6. Government Aggregation
    Data Description: A table including monthly volume of government aggregation with customer classes and entities
    specified.

Usage:
1. Undate the base_path
2. Update the data_extract to select the way of loading data: download from website ('True') or use local data ('False')
3. If using local data, change the file names in main function to the file names of local files.
4. Put deration factor file under the base_path, and update the name of deration factor file.
5. Make sure ETL report template is in base_path
6. Run the script to process the data and report warnings

Automation:
1. Use  windows task scheduler for automation
2. Set the trigger to 21th of each month. (AEP_OH will update data before 20th of each month)
"""

import pandas as pd
import warnings
import requests
from bs4 import BeautifulSoup
import os
import urllib.parse
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from jinja2 import Environment, FileSystemLoader
import base64
import datetime

# Ignore the warning on unreadable excel header
warnings.filterwarnings("ignore", category=UserWarning, message="Cannot parse header or footer so it will be ignored")

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
            new_file_name = f"{keyword_in_file}_aep_oh_{keyword_counts[keyword_in_file]}.xls" if file_name.endswith(
                '.xls') else f"{keyword_in_file}_aep_oh_{keyword_counts[keyword_in_file]}.xlsx"
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


def load_volume_data(cres_file_path, sso_file_path, pipp_file_path, deration_factor_path=None):
    # Load the raw data files
    cres_df = pd.read_excel(cres_file_path, header=4)
    sso_df = pd.read_excel(sso_file_path, header=4)
    pipp_sheets = pd.read_excel(pipp_file_path, sheet_name=None)

    for sheet_name in pipp_sheets:
        pipp_sheets[sheet_name] = pd.read_excel(pipp_file_path, sheet_name=sheet_name, header=5)

    deration_factor = pd.read_csv(deration_factor_path) if deration_factor_path else None

    return cres_df, sso_df, pipp_sheets, deration_factor


def load_UFE_data(UFE_path):
    # Load the raw data files
    UFE_df = pd.read_excel(UFE_path)
    return UFE_df


def load_GovtAggr_data(GovtAggr_file_path):
    # Load the raw data files
    GovtAggr_sheets = pd.read_excel(GovtAggr_file_path, sheet_name=None)

    return GovtAggr_sheets


def preprocess_hourly_data(cres_df, sso_df):
    # Convert 'Date' column to datetime format
    cres_df['DATE'] = pd.to_datetime(cres_df['DATE'].astype(str))
    sso_df['DATE'] = pd.to_datetime(sso_df['DATE'].astype(str))

    cres_df['HOUR'] = cres_df['HOUR'].astype(int).div(100) - 1
    sso_df['HOUR'] = sso_df['HOUR'].astype(int).div(100) - 1

    datetime_beginning_ept_cres = cres_df["DATE"] + pd.to_timedelta(cres_df["HOUR"], unit="h")
    cres_df["datetime_beginning_ept"] = datetime_beginning_ept_cres.dt.tz_localize(tz='America/New_York',
                                                                                   ambiguous='infer')
    cres_df["datetime_beginning_utc"] = cres_df["datetime_beginning_ept"].dt.tz_convert("UTC")

    datetime_beginning_ept_sso = sso_df["DATE"] + pd.to_timedelta(sso_df["HOUR"], unit="h")
    sso_df["datetime_beginning_ept"] = datetime_beginning_ept_sso.dt.tz_localize(tz='America/New_York',
                                                                                 ambiguous='infer')
    sso_df["datetime_beginning_utc"] = sso_df["datetime_beginning_ept"].dt.tz_convert("UTC")

    # Continuity Check
    check_continuity(cres_df, 'datetime_beginning_utc', 'H', 'CRES Hourly data')
    check_continuity(sso_df, 'datetime_beginning_utc', 'H', 'SSO Hourly data')

    return cres_df, sso_df


def preprocess_monthly_data(cres_df, sso_df):
    # Convert 'Date' and 'Hour_EPT' columns to datetime format
    cres_df['Year'] = cres_df['Year'].astype(str)
    cres_df['Month'] = cres_df['Month'].astype(str)
    sso_df['Year'] = sso_df['Year'].astype(str)
    sso_df['Month'] = sso_df['Month'].astype(str)

    # Extract month and year
    cres_df['FlowMonth'] = pd.to_datetime(cres_df['Year'] + '-' + cres_df['Month'] + '-01').dt.strftime('%Y-%m-%d')
    sso_df['FlowMonth'] = pd.to_datetime(sso_df['Year'] + '-' + sso_df['Month'] + '-01').dt.strftime('%Y-%m-%d')

    # Continuity Check
    check_continuity(cres_df, 'FlowMonth', 'M', 'CRES Monthly data')
    check_continuity(sso_df, 'FlowMonth', 'M', 'SSO Monthly data')

    return cres_df, sso_df


def preprocess_UFE_data(UFE_df):
    UFE_df['Date'] = pd.to_datetime(UFE_df['Date'].astype(str)).dt.strftime('%Y-%m-%d')
    UFE_df['Hour_EPT'] = UFE_df['Hour_EPT'].astype(int) - 1

    # Daylight saving modification: Modify the wrong duplicates in AEP_OH_UFE data
    daylight_saving_dates = ['2013-11-03', '2014-11-02']
    for date in daylight_saving_dates:
        idx = UFE_df[(UFE_df['Date'] == date) & (UFE_df['Hour_EPT'] == 2)].index
        if len(idx) > 0:
            # Update the first occurrence to 1
            UFE_df.loc[idx[0], 'Hour_EPT'] = 1

    # Daylight saving modification: Deal with missing rows in November
    date_with_missing = ['2015-11-01', '2016-11-06', '2017-11-05', '2018-11-04', '2019-11-03', '2020-11-01',
                         '2021-11-07', '2022-11-06', '2023-11-05']
    for date in date_with_missing:
        new_row = UFE_df[(UFE_df['Date'] == date) & (UFE_df['Hour_EPT'] == 1)].copy()
        UFE_df = pd.concat([UFE_df, new_row], ignore_index=True)
    UFE_df = UFE_df.sort_values(by=['Date', 'Hour_EPT']).reset_index(drop=True)

    # Daylight saving modification: Deal with extra rows in March
    date_to_drop = ['2016-03-13', '2017-03-12', '2018-03-11', '2019-03-10', '2020-03-08', '2021-03-14', '2022-03-13',
                    '2023-03-12', '2024-03-10']
    idx_to_drop = []
    for date in date_to_drop:
        idx_to_drop.append(UFE_df[(UFE_df['Date'] == date) & (UFE_df['Hour_EPT'] == 2)].index)

    for idx in idx_to_drop:
        UFE_df = UFE_df.drop(idx)
    UFE_df = UFE_df.reset_index(drop=True)

    datetime_beginning_ept_UFE = pd.to_datetime(UFE_df["Date"]) + pd.to_timedelta(UFE_df["Hour_EPT"], unit="h")

    UFE_df["Datetime_beginning_ept"] = datetime_beginning_ept_UFE.dt.tz_localize(tz='America/New_York',
                                                                                 ambiguous='infer')
    UFE_df["Datetime_beginning_utc"] = UFE_df["Datetime_beginning_ept"].dt.tz_convert("UTC")

    # Continuity Check
    check_continuity(UFE_df, 'Datetime_beginning_utc', 'H', 'UFE Hourly data')

    return UFE_df


def process_deration_factor(deration_factor, edc_name):
    deration_factor['Datetime_beginning_utc'] = deration_factor['Datetime_beginning_utc'].astype(str)
    deration_factor['Datetime_beginning_utc'] = pd.to_datetime(deration_factor['Datetime_beginning_utc'])

    deration_factor_df = pd.DataFrame({
        'Datetime_beginning_utc': deration_factor['Datetime_beginning_utc'],
        'EDCName': edc_name,
        'DerationFactor': deration_factor['DerationFactor']
    })

    # Continuity Check
    check_continuity(deration_factor_df, 'Datetime_beginning_utc', 'H', 'deration_factor Hourly data')

    return deration_factor_df


def process_hourly_cres_data(cres_df, edc_name, deration_factor):
    merged_df = cres_df.merge(deration_factor, left_on='datetime_beginning_utc', right_on='Datetime_beginning_utc')
    cres_com_ind = {
        'Datetime_beginning_utc': merged_df['datetime_beginning_utc'],
        'EDCName': edc_name,
        'CustomerClass': 'COM_&_IND',
        'VolumeType': 'Wholesale_Derated',
        'EGS_HourlyVolume': merged_df['C&I Hourly Load (kW)'] / 1000 * (1 - merged_df['DerationFactor']),
        'Default_HourlyVolume': 0,
        'Eligible_HourlyVolume': merged_df['C&I Hourly Load (kW)'] / 1000 * (1 - merged_df['DerationFactor']),
        'VolumeComment': ''
    }
    cres_res = {
        'Datetime_beginning_utc': merged_df['datetime_beginning_utc'],
        'EDCName': edc_name,
        'CustomerClass': 'RES',
        'VolumeType': 'Wholesale_Derated',
        'EGS_HourlyVolume': merged_df['Residential Hourly Load (kW)'] / 1000 * (1 - merged_df['DerationFactor']),
        'Default_HourlyVolume': 0,
        'Eligible_HourlyVolume': merged_df['Residential Hourly Load (kW)'] / 1000 * (1 - merged_df['DerationFactor']),
        'VolumeComment': ''
    }
    cres_df_com_ind = pd.DataFrame(cres_com_ind)
    cres_df_res = pd.DataFrame(cres_res)
    df_combined = pd.concat([cres_df_com_ind, cres_df_res], ignore_index=True)

    return df_combined


def process_hourly_sso_data(sso_df, edc_name, deration_factor):
    merged_df = sso_df.merge(deration_factor, left_on='datetime_beginning_utc', right_on='Datetime_beginning_utc')
    sso_com_ind = {
        'Datetime_beginning_utc': merged_df['datetime_beginning_utc'],
        'EDCName': edc_name,
        'CustomerClass': 'COM_&_IND',
        'VolumeType': 'Wholesale_Derated',
        'EGS_HourlyVolume': 0,
        'Default_HourlyVolume': merged_df['C&I Hourly Load (kW)'] / 1000 * (1 - merged_df['DerationFactor']),
        'Eligible_HourlyVolume': merged_df['C&I Hourly Load (kW)'] / 1000 * (1 - merged_df['DerationFactor']),
        'VolumeComment': ''
    }
    sso_res = {
        'Datetime_beginning_utc': merged_df['datetime_beginning_utc'],
        'EDCName': edc_name,
        'CustomerClass': 'RES',
        'VolumeType': 'Wholesale_Derated',
        'EGS_HourlyVolume': 0,
        'Default_HourlyVolume': merged_df['Residential Hourly Load (kW)'] / 1000 * (1 - merged_df['DerationFactor']),
        'Eligible_HourlyVolume': merged_df['Residential Hourly Load (kW)'] / 1000 * (1 - merged_df['DerationFactor']),
        'VolumeComment': ''
    }
    sso_df_com_ind = pd.DataFrame(sso_com_ind)
    sso_df_res = pd.DataFrame(sso_res)

    return pd.concat([sso_df_com_ind, sso_df_res], ignore_index=True)


def process_hourly_pipp_data(pipp_sheets, edc_name, deration_factor):
    pipp_df_list = []
    for sheet_name, sheet_df in pipp_sheets.items():
        # Convert 'Date' column to datetime format
        sheet_df['DATE'] = sheet_df['DATE'].astype(str)
        sheet_df['DATE'] = pd.to_datetime(sheet_df["DATE"])

        # Ensure 'Hour' columns are treated as int
        sheet_df['HOUR'] = sheet_df['HOUR'].astype(int)
        sheet_df['HOUR'] = sheet_df['HOUR'].apply(lambda x: x / 100 - 1 if x > 24 else x - 1)
        sheet_df['HOUR'] = sheet_df['HOUR'].astype(int)

        datetime_beginning_ept_pipp = sheet_df["DATE"] + pd.to_timedelta(sheet_df["HOUR"], unit="h")
        sheet_df["datetime_beginning_ept"] = datetime_beginning_ept_pipp.dt.tz_localize(tz='America/New_York',
                                                                                        # Eastern Prevailing Time
                                                                                        ambiguous='infer')
        sheet_df["datetime_beginning_utc"] = sheet_df["datetime_beginning_ept"].dt.tz_convert("UTC")

        merged_df = sheet_df.merge(deration_factor, left_on='datetime_beginning_utc', right_on='Datetime_beginning_utc')

        pipp_data = {
            'Datetime_beginning_utc': merged_df['datetime_beginning_utc'],
            'EDCName': edc_name,
            'CustomerClass': 'PIPP',
            'VolumeType': 'Wholesale_Derated',
            'EGS_HourlyVolume': 0,
            'Default_HourlyVolume': merged_df['PIPP Customers\nHourly Load (kW)'] / 1000 * (
                    1 - merged_df['DerationFactor']),
            'Eligible_HourlyVolume': merged_df['PIPP Customers\nHourly Load (kW)'] / 1000 * (
                    1 - merged_df['DerationFactor']),
            'VolumeComment': ''
        }
        pipp_df = pd.DataFrame(pipp_data, index=sheet_df.index)
        pipp_df_list.append(pipp_df)
    combined_df = pd.concat(pipp_df_list, ignore_index=True)
    # Continuity Check
    check_continuity(combined_df, 'Datetime_beginning_utc', 'H', 'PIPP Hourly data')

    return combined_df


def decompose_hourly_data(sso_hourly_processed, pipp_hourly_processed, cutoff_date='2016-06-01'):
    sso_hourly_processed = sso_hourly_processed.copy()

    sso_prior_16 = sso_hourly_processed[(sso_hourly_processed['CustomerClass'] == 'RES') &
                                         (sso_hourly_processed['Datetime_beginning_utc'] < cutoff_date)]
    sso_after_16 = sso_hourly_processed[~((sso_hourly_processed['CustomerClass'] == 'RES') &
                                           (sso_hourly_processed['Datetime_beginning_utc'] < cutoff_date))]

    merged_df = sso_prior_16.merge(pipp_hourly_processed, on=['Datetime_beginning_utc'], how='left',
                                   suffixes=('', '_pipp'))

    merged_df['Default_HourlyVolume'] -= merged_df['Default_HourlyVolume_pipp']
    merged_df['Eligible_HourlyVolume'] -= merged_df['Eligible_HourlyVolume_pipp']

    sso_prior_16 = merged_df[['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType', 'EGS_HourlyVolume',
                              'Default_HourlyVolume', 'Eligible_HourlyVolume', 'VolumeComment']]

    sso_hourly_processed = pd.concat([sso_after_16, sso_prior_16], ignore_index=True)

    return sso_hourly_processed



def process_monthly_cres_data(cres_df, edc_name):
    cres_com_ind = {
        'FlowMonth': cres_df['FlowMonth'],
        'EDCName': edc_name,
        'CustomerClass': 'COM_&_IND',
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': cres_df['C&I Customer Count'],
        'Default_MonthlyVolume': 0,
        'Eligible_MonthlyVolume': cres_df['C&I Customer Count'],
        'VolumeComment': ''
    }
    cres_res = {
        'FlowMonth': cres_df['FlowMonth'],
        'EDCName': edc_name,
        'CustomerClass': 'RES',
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': cres_df['Residential Customer Count'],
        'Default_MonthlyVolume': 0,
        'Eligible_MonthlyVolume': cres_df['Residential Customer Count'],
        'VolumeComment': ''
    }
    cres_df_com_ind = pd.DataFrame(cres_com_ind)
    cres_df_res = pd.DataFrame(cres_res)

    return pd.concat([cres_df_com_ind, cres_df_res], ignore_index=True)


def process_monthly_sso_data(sso_df, edc_name):
    sso_com_ind = {
        'FlowMonth': sso_df['FlowMonth'],
        'EDCName': edc_name,
        'CustomerClass': 'COM_&_IND',
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': 0,
        'Default_MonthlyVolume': sso_df['C&I Customer Count'],
        'Eligible_MonthlyVolume': sso_df['C&I Customer Count'],
        'VolumeComment': ''
    }
    sso_res = {
        'FlowMonth': sso_df['FlowMonth'],
        'EDCName': edc_name,
        'CustomerClass': 'RES',
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': 0,
        'Default_MonthlyVolume': sso_df['Residential Customer Count'],
        'Eligible_MonthlyVolume': sso_df['Residential Customer Count'],
        'VolumeComment': ''
    }
    sso_df_com_ind = pd.DataFrame(sso_com_ind)
    sso_df_res = pd.DataFrame(sso_res)

    return pd.concat([sso_df_com_ind, sso_df_res], ignore_index=True)


def process_monthly_pipp_data(pipp_sheets, edc_name):
    pipp_df_list = []
    for sheet_name, sheet_df in pipp_sheets.items():
        sheet_df['MONTH'] = pd.to_datetime(sheet_df['MONTH'])
        sheet_df['FlowMonth'] = sheet_df['MONTH'].dt.strftime('%Y-%m-01')
        pipp_data = {
            'FlowMonth': sheet_df['FlowMonth'],
            'EDCName': edc_name,
            'CustomerClass': 'PIPP',
            'VolumeType': 'CustomerCount',
            'EGS_MonthlyVolume': 0,
            'Default_MonthlyVolume': sheet_df['Active No. of Customers'],
            'Eligible_MonthlyVolume': sheet_df['Active No. of Customers'],
            'VolumeComment': ''
        }
        pipp_df = pd.DataFrame(pipp_data, index=sheet_df.index)
        pipp_df_list.append(pipp_df)

    combined_df = pd.concat(pipp_df_list, ignore_index=True)
    # Continuity Check
    check_continuity(combined_df, 'FlowMonth', 'M', 'PIPP Monthly data')

    return combined_df


def decompose_monthly_data(sso_monthly_processed, pipp_monthly_processed, cutoff_date='2016-06-01'):
    sso_monthly_processed = sso_monthly_processed.copy()
    sso_monthly_processed['FlowMonth'] = pd.to_datetime(sso_monthly_processed['FlowMonth']).dt.strftime('%Y-%m-%d')

    sso_prior_16 = sso_monthly_processed[(sso_monthly_processed['CustomerClass'] == 'RES') &
                                         (sso_monthly_processed['FlowMonth'] < cutoff_date)]
    sso_after_16 = sso_monthly_processed[~((sso_monthly_processed['CustomerClass'] == 'RES') &
                                           (sso_monthly_processed['FlowMonth'] < cutoff_date))]

    merged_df = sso_prior_16.merge(pipp_monthly_processed, on=['FlowMonth'], how='left',
                                   suffixes=('', '_pipp'))

    merged_df['Default_MonthlyVolume'] -= merged_df['Default_MonthlyVolume_pipp']
    merged_df['Eligible_MonthlyVolume'] -= merged_df['Eligible_MonthlyVolume_pipp']

    sso_prior_16 = merged_df[['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType', 'EGS_MonthlyVolume',
                              'Default_MonthlyVolume', 'Eligible_MonthlyVolume', 'VolumeComment']]

    sso_monthly_processed = pd.concat([sso_after_16, sso_prior_16], ignore_index=True)

    return sso_monthly_processed


def handle_PLC_missing_data(plc_df, nspl_df):
    merged_df = pd.merge(plc_df, nspl_df, on=['DATE'], how='outer', suffixes=('_PLC', '_NSPL'))
    merged_df = merged_df.sort_values(by=['DATE'], ignore_index=True)
    missing_df = merged_df[merged_df['AEP_OHIO_PLC'].isna()]

    for index, row in missing_df.iterrows():
        total_ratio =merged_df.iloc[index - 1]['AEP_OHIO_PLC']/merged_df.iloc[index - 1]['AEP_OHIO_NSPL']
        merged_df.loc[index, 'AEP_OHIO_PLC'] = total_ratio * merged_df.iloc[index]['AEP_OHIO_NSPL']

        sso_ratio = merged_df.iloc[index - 1]['SSO_PLC'] / merged_df.iloc[index - 1]['SSO_NSPL']
        merged_df.loc[index, 'SSO_PLC'] = sso_ratio * merged_df.iloc[index]['SSO_NSPL']

        merged_df.loc[index, 'CRES_PLC'] = merged_df.loc[index, 'AEP_OHIO_PLC'] - merged_df.loc[index, 'SSO_PLC']

    output_plc = pd.DataFrame({
        'DATE': merged_df['DATE'],
        'AEP_OHIO_PLC': merged_df['AEP_OHIO_PLC'],
        'SSO_PLC': merged_df['SSO_PLC'],
        'CRES_PLC': merged_df['CRES_PLC'],
        'PIPP_PLC': merged_df['PIPP_PLC']
    })
    return output_plc



def process_daily_PLC_data(plc_df, edc_name):
    # Ensure 'Date' columns are treated as string
    plc_df['DATE'] = plc_df['DATE'].astype(str)
    plc_df['DATE'] = pd.to_datetime(plc_df['DATE']).dt.strftime('%Y-%m-%d')

    output_df = {
        'FlowDate': plc_df['DATE'],
        'EDCName': edc_name,
        'CustomerClass': 'Blended',
        'VolumeType': 'PLC_Scaled',
        'EGS_DailyVolume': plc_df['CRES_PLC'],
        'Default_DailyVolume': plc_df['SSO_PLC'],
        'Eligible_DailyVolume': plc_df['SSO_PLC'] + plc_df['CRES_PLC'],
        'VolumeComment': ''}

    plc_df = pd.DataFrame(output_df)

    # Continuity Check
    check_continuity(plc_df, 'FlowDate', 'D', 'PLC Daily data')

    return plc_df


def process_daily_NSPL_data(nspl_df, edc_name):
    # Ensure 'Date' columns are treated as string
    nspl_df['DATE'] = nspl_df['DATE'].astype(str)
    nspl_df['DATE'] = pd.to_datetime(nspl_df['DATE']).dt.strftime('%Y-%m-%d')

    output_df = {
        'FlowDate': nspl_df['DATE'],
        'EDCName': edc_name,
        'CustomerClass': 'Blended',
        'VolumeType': 'NSPL_Scaled',
        'EGS_DailyVolume': nspl_df['CRES_NSPL'],
        'Default_DailyVolume': nspl_df['SSO_NSPL'],
        'Eligible_DailyVolume': nspl_df['SSO_NSPL'] + nspl_df['CRES_NSPL'],
        'VolumeComment': ''}

    nspl_df = pd.DataFrame(output_df)

    # Continuity Check
    check_continuity(nspl_df, 'FlowDate', 'D', 'NSPL Daily data')

    return nspl_df


def process_daily_PIPP_data(PIPP_daily_sheets, edc_name):
    sheet_df_1 = PIPP_daily_sheets['Prior to Jun 1 2016']

    sheet_df_1['MONTH'] = sheet_df_1['MONTH'].astype(str)
    sheet_df_1['MONTH'] = pd.to_datetime(sheet_df_1['MONTH']).dt.strftime('%Y-%m-%d')

    # Continuity Check
    check_continuity(sheet_df_1, 'MONTH', 'M', 'PIPP daily data sheet 1')

    daily_volume_df_prior16 = pd.DataFrame()
    for date, PLC_volume, NSPL_volume in zip(sheet_df_1['MONTH'], sheet_df_1['PLC in MW per day'],
                                             sheet_df_1['NSPL in MW per day']):
        # Create a date range for the month

        start_date = pd.to_datetime(date).replace(day=1)
        end_date = (start_date + pd.offsets.MonthEnd(1)).normalize()
        date_range = pd.date_range(start=start_date, end=end_date)

        # Create a dataframe with daily dates and corresponding volume
        pipp_PLC_data = pd.DataFrame({
            'FlowDate': date_range,
            'EDCName': edc_name,
            'CustomerClass': 'PIPP',
            'VolumeType': 'PLC_Scaled',
            'EGS_DailyVolume': 0,
            'Default_DailyVolume': PLC_volume,
            'Eligible_DailyVolume': PLC_volume,
            'VolumeComment': ''
        })
        pipp_NSPL_data = pd.DataFrame({
            'FlowDate': date_range,
            'EDCName': edc_name,
            'CustomerClass': 'PIPP',
            'VolumeType': 'NSPL_Scaled',
            'EGS_DailyVolume': 0,
            'Default_DailyVolume': NSPL_volume,
            'Eligible_DailyVolume': NSPL_volume,
            'VolumeComment': ''
        })
        daily_volume_df_prior16 = pd.concat([daily_volume_df_prior16, pipp_PLC_data, pipp_NSPL_data], ignore_index=True)

    # Process sheet 2 (2016 Forward)
    sheet_df_2 = PIPP_daily_sheets['Jun 1 2016 Forward']
    sheet_df_2['DATE'] = sheet_df_2['DATE'].astype(str)
    sheet_df_2['DATE'] = pd.to_datetime(sheet_df_2['DATE']).dt.strftime('%Y-%m-%d')

    # Continuity Check
    check_continuity(sheet_df_2, 'DATE', 'D', 'PIPP daily data sheet 2')

    # Create a dataframe with daily dates and corresponding volume
    pipp_PLC_df = pd.DataFrame({
        'FlowDate': sheet_df_2['DATE'],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'PLC_Scaled',
        'EGS_DailyVolume': 0,
        'Default_DailyVolume': sheet_df_2['PLC in MW per day'],
        'Eligible_DailyVolume': sheet_df_2['PLC in MW per day'],
        'VolumeComment': ''
    })
    pipp_NSPL_df = pd.DataFrame({
        'FlowDate': sheet_df_2['DATE'],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'NSPL_Scaled',
        'EGS_DailyVolume': 0,
        'Default_DailyVolume': sheet_df_2['NSPL in MW per day'],
        'Eligible_DailyVolume': sheet_df_2['NSPL in MW per day'],
        'VolumeComment': ''
    })

    output_df = pd.concat([daily_volume_df_prior16, pipp_PLC_df, pipp_NSPL_df], ignore_index=True)
    output_df['FlowDate'] = pd.to_datetime(output_df['FlowDate']).dt.strftime('%Y-%m-%d')

    return output_df

def decompose_daily_data(plc_daily_processed, nspl_daily_processed, pipp_daily_processed, cutoff_date='2016-06-01'):
    pipp_plc_df = pipp_daily_processed[(pipp_daily_processed['VolumeType'] == 'PLC_Scaled') &
                                        (pipp_daily_processed['FlowDate'] < cutoff_date)]
    pipp_nspl_df = pipp_daily_processed[(pipp_daily_processed['VolumeType'] == 'NSPL_Scaled') &
                                        (pipp_daily_processed['FlowDate'] < cutoff_date)]

    blended_plc_df = plc_daily_processed[(plc_daily_processed['VolumeType'] == 'PLC_Scaled') &
                                         (plc_daily_processed['FlowDate'] < cutoff_date)]
    blended_nspl_df = nspl_daily_processed[(nspl_daily_processed['VolumeType'] == 'NSPL_Scaled') &
                                           (nspl_daily_processed['FlowDate'] < cutoff_date)]

    volume_columns = ['EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume']

    merged_plc_df = blended_plc_df.merge(pipp_plc_df, on=['FlowDate', 'VolumeType'], how='left',
                                         suffixes=('_blended', '_pipp'))
    merged_nspl_df = blended_nspl_df.merge(pipp_nspl_df, on=['FlowDate', 'VolumeType'], how='left',
                                           suffixes=('_blended', '_pipp'))
    for column in volume_columns:
        merged_plc_df[f'{column}_blended'] -= merged_plc_df[f'{column}_pipp']
        merged_nspl_df[f'{column}_blended'] -= merged_nspl_df[f'{column}_pipp']

    plc_prior_16 = pd.DataFrame({
        'FlowDate': merged_plc_df['FlowDate'],
        'EDCName': merged_plc_df['EDCName_blended'],
        'CustomerClass': 'Blended',
        'VolumeType': 'PLC_Scaled',
        'EGS_DailyVolume': merged_plc_df['EGS_DailyVolume_blended'],
        'Default_DailyVolume': merged_plc_df['Default_DailyVolume_blended'],
        'Eligible_DailyVolume': merged_plc_df['Eligible_DailyVolume_blended'],
        'VolumeComment': ''})

    plc_after_16 = plc_daily_processed[(plc_daily_processed['VolumeType'] == 'PLC_Scaled') &
                                       (plc_daily_processed['FlowDate'] >= cutoff_date)]
    nspl_prior_16 = pd.DataFrame({
        'FlowDate': merged_nspl_df['FlowDate'],
        'EDCName': merged_nspl_df['EDCName_blended'],
        'CustomerClass': 'Blended',
        'VolumeType': 'NSPL_Scaled',
        'EGS_DailyVolume': merged_nspl_df['EGS_DailyVolume_blended'],
        'Default_DailyVolume': merged_nspl_df['Default_DailyVolume_blended'],
        'Eligible_DailyVolume': merged_nspl_df['Eligible_DailyVolume_blended'],
        'VolumeComment': ''})

    nspl_after_16 = nspl_daily_processed[(nspl_daily_processed['VolumeType'] == 'NSPL_Scaled') &
                                         (nspl_daily_processed['FlowDate'] >= cutoff_date)]
    plc_daily_decomposed = pd.concat([plc_after_16, plc_prior_16], ignore_index=True)
    nspl_daily_decomposed = pd.concat([nspl_after_16, nspl_prior_16], ignore_index=True)

    return plc_daily_decomposed, nspl_daily_decomposed

def process_UFE_data(UFE_df, final_hourly_df, edc_name):
    UFE_df = UFE_df.copy()
    final_hourly_df = final_hourly_df.copy()
    merged_df = UFE_df.merge(final_hourly_df, on=['Datetime_beginning_utc'], how='left', suffixes=('_ufe', ''))
    merged_df = merged_df.dropna(subset=['Eligible_HourlyVolume'])
    UFE_processed = pd.DataFrame({
        'Datetime_beginning_utc': merged_df['Datetime_beginning_utc'],
        'EDCName': edc_name,
        'CustomerClass': merged_df['CustomerClass'],
        'VolumeType': 'UFE_volume',
        'EGS_HourlyVolume': merged_df['EGS_HourlyVolume'] * (1 - 1/merged_df['UFE_Factor']),
        'Default_HourlyVolume': merged_df['Default_HourlyVolume'] * (1 - 1/merged_df['UFE_Factor']),
        'Eligible_HourlyVolume': merged_df['Eligible_HourlyVolume'] * (1 - 1/merged_df['UFE_Factor']),
        'VolumeComment': ''
    })

    return UFE_processed


def process_GovtAggr_data(GovtAggr_sheets, GovtAggr_file_path, edc_name):
    sheet_data = []
    for sheet in GovtAggr_sheets:

        header = 4 if sheet == 'Oct 2014 - Sep 2015' else 3
        # Read Excel sheet, drop the first and last rows
        GovtAggr_df = pd.read_excel(GovtAggr_file_path, sheet_name=sheet, header=header)
        GovtAggr_df = GovtAggr_df.iloc[1:-1, :]

        # Save EntityName, CustomerClass, and sheet_months
        GovtAggr_df['EntityName'] = GovtAggr_df.iloc[:, 0].ffill()
        GovtAggr_df['CustomerClass'] = GovtAggr_df.iloc[:, 1]

        # Drop useless columns
        GovtAggr_df.columns = GovtAggr_df.columns.astype(str)
        GovtAggr_df = GovtAggr_df.loc[:, ~GovtAggr_df.columns.str.contains('^Unnamed')]
        columns_to_drop = ['Total Billed kWh', 'Total Cust Cnt ']
        GovtAggr_df = GovtAggr_df.drop(columns=columns_to_drop, errors='ignore')
        sheet_months = GovtAggr_df.columns[:-2]

        # Fillin NaN with 0
        GovtAggr_df = GovtAggr_df.fillna(0)

        for _, row in GovtAggr_df.iterrows():

            entity_name = row['EntityName']
            customer_class = row['CustomerClass']

            for month in sheet_months:
                volume = row[month]
                flow_month = pd.to_datetime(str(month), format='%Y%m', errors='coerce').strftime("%Y-%m-01")

                sheet_data.append({
                    'FlowMonth': flow_month,
                    'EDCName': edc_name,
                    'CustomerClass': f'{customer_class}',
                    'VolumeType': f'volume_MW_{entity_name}',
                    'Volume': volume/1000,
                    'VolumeComment': ''
                })

    GovtAggr_processed_df = pd.DataFrame(sheet_data)

    # Make sure all Customer Classes are displayed in upper case
    GovtAggr_processed_df['CustomerClass'] = GovtAggr_processed_df['CustomerClass'].str.upper()

    # Drop duplicates and Report unmatch data
    GovtAggr_processed_df = GovtAggr_processed_df.drop_duplicates(keep='first')
    duplicate_rows = GovtAggr_processed_df[
        GovtAggr_processed_df.duplicated(subset=['FlowMonth', 'CustomerClass', 'VolumeType'])]
    if not duplicate_rows.empty:
        print('Warning: Found unmatched duplicates in GovtAggr file')
        print(duplicate_rows)

    GovtAggr_processed_df = GovtAggr_processed_df.sort_values(by=['FlowMonth', 'CustomerClass', 'VolumeType'],
                                                              ignore_index=True)

    # Continuity Check
    check_continuity(GovtAggr_processed_df, 'FlowMonth', 'M', 'GovtAggr data')

    return GovtAggr_processed_df


def combine_data(cres_df, sso_df, pipp_df, data_type):
    # Combine the CRES, SSO, PIPP and deration_factor data
    combined_df = pd.concat([cres_df, sso_df, pipp_df], ignore_index=True)

    if data_type == 'hourly':
        # Group by Datetime_beginning_utc, EDCName, CustomerClass, VolumeType, and VolumeComment
        combined_df = combined_df.groupby(['Datetime_beginning_utc', 'EDCName', 'CustomerClass',
                                           'VolumeType', 'VolumeComment'], as_index=False).sum()
    elif data_type == 'monthly':
        # Group by FlowMonth, EDCName, CustomerClass, VolumeType, and VolumeComment
        combined_df = combined_df.groupby(['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType', 'VolumeComment'],
                                          as_index=False).sum()
    elif data_type == 'daily':
        # Sort combined_df in FlowDate, CustomerClass, VolumeType
        combined_df = combined_df.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'], ignore_index=True)

    return combined_df


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
        plt.savefig(f'{output_dir}/aep_oh_CustomerCounts_{customer_class}_plot.png')
        plot_path[f'monthly_{customer_class}'] = f'{output_dir}/aep_oh_CustomerCounts_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_hourly_data(df, output_dir):
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
        plt.savefig(f'{output_dir}/aep_oh_HourlyLoad_{customer_class}_plot.png')
        plot_path[f'hourly_{customer_class}'] = f'{output_dir}/aep_oh_HourlyLoad_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_daily_data(df, output_dir):
    data = df.copy()
    data['Difference Check'] = (data['Eligible_DailyVolume'] - data['Default_DailyVolume'] - data['EGS_DailyVolume']).round(3)

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
        plt.savefig(f'{output_dir}/aep_oh_PLC_NSPL_{customer_class}_plot.png')
        plot_path[f'daily_{customer_class}'] = f'{output_dir}/aep_oh_PLC_NSPL_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_UFE_data(data, output_dir):
    data = data.copy()
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

    for customer_class in customer_classes:
        fig, axs = plt.subplots(len(columns_to_plot), 1, figsize=(20, 20), sharex=False)

        class_data = data[data['CustomerClass'] == customer_class]

        for col_idx, column in enumerate(columns_to_plot):
            axs[col_idx].plot(pd.to_datetime(class_data['Datetime_beginning_utc']), class_data[column])
            axs[col_idx].set_title(f'{column} - {customer_class}', fontsize=18)
            axs[col_idx].set_ylabel('UFE Volume', fontsize=16)

            axs[col_idx].xaxis.set_major_locator(mdates.MonthLocator(bymonth=6))
            axs[col_idx].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            axs[col_idx].xaxis.set_visible(True)

            if col_idx == len(columns_to_plot) - 1:
                axs[col_idx].set_xlabel('Datetime_beginning_utc', fontsize=16)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/aep_oh_UFE_{customer_class}_plot.png')
        plot_path[f'UFE_{customer_class}'] = f'{output_dir}/aep_oh_UFE_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_GovtAggr_data(df, output_dir):
    data = df.copy()
    data = data.groupby(['FlowMonth', 'EDCName', 'CustomerClass'], as_index=False).sum()
    plot_path = {}
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)
    customer_classes = data['CustomerClass'].unique()

    # Set larger font size for all text elements
    plt.rcParams.update({'font.size': 14})

    fig, axs = plt.subplots(len(customer_classes), 1, figsize=(20, 20), sharex=False)

    for col_idx, customer_class in enumerate(customer_classes):
        class_data = data[data['CustomerClass'] == customer_class]
        axs[col_idx].plot(pd.to_datetime(class_data['FlowMonth']), class_data['Volume'])
        axs[col_idx].set_title(f'Government Aggregation - {customer_class}', fontsize=18)
        axs[col_idx].set_ylabel('Volume', fontsize=16)

        axs[col_idx].xaxis.set_major_locator(mdates.MonthLocator(bymonth=6))
        axs[col_idx].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        axs[col_idx].xaxis.set_visible(True)

        if col_idx == len(customer_classes) - 1:
            axs[col_idx].set_xlabel('FlowMonth', fontsize=16)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/aep_oh_Government_Aggregation_plot.png')
    plot_path[f"GovtAggr"] = f'{output_dir}/aep_oh_Government_Aggregation_plot.png'
    plt.close(fig)

    return plot_path


def save_plot_path(plot_path):
    report_plots_path = {
        'Monthly_Customer_Counts_RES': plot_path['monthly_RES'],
        'Monthly_Customer_Counts_CI': plot_path['monthly_COM_&_IND'],
        'Monthly_Customer_Counts_PIPP': plot_path['monthly_PIPP'],
        'Hourly_Volume_RES': plot_path['hourly_RES'],
        'Hourly_Volume_CI': plot_path['hourly_COM_&_IND'],
        'Hourly_Volume_PIPP': plot_path['hourly_PIPP'],
        'Daily_Volume_Blended': plot_path['daily_Blended'],
        'Daily_Volume_PIPP': plot_path['daily_PIPP'],
        'UFE_RES': plot_path['UFE_RES'],
        'UFE_CI': plot_path['UFE_COM_&_IND'],
        'UFE_PIPP': plot_path['UFE_PIPP'],
        'GovtAggr': plot_path['GovtAggr']
    }
    return report_plots_path


def generate_keystats(final_monthly_df, final_hourly_df, final_daily_df, UFE_processed, govt_aggr_processed):
    monthly_columns = ['EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume']
    hourly_columns = ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']
    daily_columns = ['EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume']
    UFE_columns = ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']

    govt_aggr_df = govt_aggr_processed.copy()
    govt_aggr_df = govt_aggr_df.groupby(['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType'], as_index=False).sum()
    key_stats_govt_aggr = pd.DataFrame()
    for customerclass in govt_aggr_df['CustomerClass'].unique():
        describe_df = govt_aggr_df[govt_aggr_df['CustomerClass'] == customerclass].describe()
        describe_df.columns = [f'{customerclass}']
        key_stats_govt_aggr = pd.concat([key_stats_govt_aggr, describe_df], axis=1)
    report_keystats_table = {
        'Monthly_Customer_Counts_RES': final_monthly_df[final_monthly_df['CustomerClass'] == 'RES'][
            monthly_columns].describe().T,
        'Monthly_Customer_Counts_CI': final_monthly_df[final_monthly_df['CustomerClass'] == 'COM_&_IND'][
            monthly_columns].describe().T,
        'Monthly_Customer_Counts_PIPP': final_monthly_df[final_monthly_df['CustomerClass'] == 'PIPP'][
            monthly_columns].describe().T,
        'Hourly_Volume_RES': final_hourly_df[final_hourly_df['CustomerClass'] == 'RES'][hourly_columns].describe().T,
        'Hourly_Volume_CI': final_hourly_df[final_hourly_df['CustomerClass'] == 'COM_&_IND'][
            hourly_columns].describe().T,
        'Hourly_Volume_PIPP': final_hourly_df[final_hourly_df['CustomerClass'] == 'PIPP'][hourly_columns].describe().T,
        'Daily_Volume_Blended': final_daily_df[final_daily_df['CustomerClass'] == 'Blended'][
            daily_columns].describe().T,
        'Daily_Volume_PIPP': final_daily_df[final_daily_df['CustomerClass'] == 'PIPP'][daily_columns].describe().T,
        'UFE_RES': UFE_processed[UFE_processed['CustomerClass'] == 'RES'][UFE_columns].describe().T,
        'UFE_CI': UFE_processed[UFE_processed['CustomerClass'] == 'COM_&_IND'][UFE_columns].describe().T,
        'UFE_PIPP': UFE_processed[UFE_processed['CustomerClass'] == 'PIPP'][UFE_columns].describe().T,
        'GovtAggr': key_stats_govt_aggr.T
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
        'report_title': 'AEP Ohio ETL Report',
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

    elif data_type == 'GovtAggr':

        processed_df.to_excel(output_path, index=False)
        print(f"Data processing complete. The file '{output_path}' has been created.")


def main(base_path, data_extract=True, deration_factor_filename='DerationFactor_AEPOHIO_RESID_AGG.csv'):

    if data_extract:
        # Download data from aep_oh website
        print('Downloading data...')

        # Define the keywords to filter the filenames
        keywords = ['NSPL', 'PLC', 'SSO Hourly', 'CRES Hourly', 'SSO Customer Counts', 'CRES Customer Counts', 'UFE', 'Govt Aggr']
        keywords_pipp = ['PIPP NSPL-PLC', 'PIPP Hourly', 'PIPP Customer Counts']

        # Define the URL of the website
        keyword_dict = {'https://aepohiocbp.com/index.cfm?s=dataRoom&p=monthly': keywords,
                        'https://aepohiocbp.com/index.cfm?s=PIPPRFP&p=PIPPRFP': keywords_pipp}

        file_paths = {}
        for url in keyword_dict:
            soup = fetch_html_content(url)
            excel_links = find_excel_links(soup, url, keyword_dict[url])
            downloaded_path = process_and_download_links(excel_links, keyword_dict[url], base_path)
            file_paths.update(downloaded_path)

        # Update input data file path
        cres_hourly_file_path = file_paths['CRES Hourly']
        sso_hourly_file_path = file_paths['SSO Hourly']
        pipp_hourly_file_path = file_paths['PIPP Hourly']

        cres_monthly_file_path = file_paths['CRES Customer Counts']
        sso_monthly_file_path = file_paths['SSO Customer Counts']
        pipp_monthly_file_path = file_paths['PIPP Customer Counts']

        plc_daily_file_path = file_paths['PLC']
        nspl_daily_file_path = file_paths['NSPL']
        pipp_daily_file_path = file_paths['PIPP NSPL-PLC']

        ufe_file_path = file_paths['UFE']
        govt_aggr_file_path = file_paths['Govt Aggr']

    else:
        # Define local file path
        cres_hourly_file_path = f'{base_path}/CRES Hourly_aep_oh_1.xlsx'
        sso_hourly_file_path = f'{base_path}/SSO Hourly_aep_oh_1.xlsx'
        pipp_hourly_file_path = f'{base_path}/PIPP Hourly_aep_oh_1.xlsx'

        cres_monthly_file_path = f'{base_path}/CRES Customer Counts_aep_oh_1.xlsx'
        sso_monthly_file_path = f'{base_path}/SSO Customer Counts_aep_oh_1.xlsx'
        pipp_monthly_file_path = f'{base_path}/PIPP Customer Counts_aep_oh_1.xlsx'

        plc_daily_file_path = f'{base_path}/PLC_aep_oh_1.xls'
        nspl_daily_file_path = f'{base_path}/NSPL_aep_oh_1.xls'
        pipp_daily_file_path = f'{base_path}/PIPP NSPL-PLC_aep_oh_1.xls'
        ufe_file_path = f'{base_path}/UFE_aep_oh_1.xlsx'
        govt_aggr_file_path = f'{base_path}/Govt Aggr_aep_oh_1.xls'

    deration_factor_path = f'{base_path}/{deration_factor_filename}'

    # Output path
    output_path = f'{base_path}/output_data'
    # Create output directory if it does not exist
    os.makedirs(output_path, exist_ok=True)

    hourly_output_path = f'{output_path}/aep_oh_HourlyVolume_processed.xlsx'
    monthly_output_path = f'{output_path}/aep_oh_CustomerCount_processed.xlsx'
    daily_output_path = f'{output_path}/aep_oh_NSPL_PLC_processed.xlsx'
    ufe_output_path = f'{output_path}/aep_oh_UFE_processed.xlsx'

    govt_aggr_output_path = f'{output_path}/aep_oh_GovtAggr_processed.xlsx'
    etl_report_output_path = f'{output_path}/ETL_report'

    # Load data
    print('Loading data...')
    cres_hourly_df, sso_hourly_df, pipp_hourly_sheets, deration_factor = load_volume_data(cres_hourly_file_path,
                                                                                          sso_hourly_file_path,
                                                                                          pipp_hourly_file_path,
                                                                                          deration_factor_path)

    deration_factor = deration_factor[deration_factor['LocaleName'] == 'AEPOHIO_RESID_AGG']

    cres_monthly_df, sso_monthly_df, pipp_monthly_sheets, _ = load_volume_data(cres_monthly_file_path,
                                                                               sso_monthly_file_path,
                                                                               pipp_monthly_file_path)

    plc_df, nspl_df, pipp_daily_sheets, _ = load_volume_data(plc_daily_file_path, nspl_daily_file_path,
                                                             pipp_daily_file_path)

    ufe_df = load_UFE_data(ufe_file_path)
    govt_aggr_sheets = load_GovtAggr_data(govt_aggr_file_path)

    print('Processing data...')
    # Preprocess data
    cres_hourly_df, sso_hourly_df = preprocess_hourly_data(cres_hourly_df, sso_hourly_df)
    cres_monthly_df, sso_monthly_df = preprocess_monthly_data(cres_monthly_df, sso_monthly_df)
    ufe_df = preprocess_UFE_data(ufe_df)
    plc_df = handle_PLC_missing_data(plc_df, nspl_df)
    # Process data
    edc_name = "OH_AEP"
    deration_factor_processed = process_deration_factor(deration_factor, edc_name)
    cres_hourly_processed = process_hourly_cres_data(cres_hourly_df, edc_name, deration_factor_processed)
    sso_hourly_processed = process_hourly_sso_data(sso_hourly_df, edc_name, deration_factor_processed)
    pipp_hourly_processed = process_hourly_pipp_data(pipp_hourly_sheets, edc_name, deration_factor_processed)
    sso_hourly_processed = decompose_hourly_data(sso_hourly_processed, pipp_hourly_processed, '2016-06-01')

    cres_monthly_processed = process_monthly_cres_data(cres_monthly_df, edc_name)
    sso_monthly_processed = process_monthly_sso_data(sso_monthly_df, edc_name)
    pipp_monthly_processed = process_monthly_pipp_data(pipp_monthly_sheets, edc_name)
    sso_monthly_processed = decompose_monthly_data(sso_monthly_processed, pipp_monthly_processed, '2016-06-01')

    plc_daily_processed = process_daily_PLC_data(plc_df, edc_name)
    nspl_daily_processed = process_daily_NSPL_data(nspl_df, edc_name)
    pipp_daily_processed = process_daily_PIPP_data(pipp_daily_sheets, edc_name)
    plc_daily_processed, nspl_daily_processed = decompose_daily_data(plc_daily_processed, nspl_daily_processed,
                                                                     pipp_daily_processed, '2016-06-01')

    govt_aggr_processed = process_GovtAggr_data(govt_aggr_sheets, govt_aggr_file_path, edc_name)

    # Combine all dataframes
    final_hourly_df = combine_data(cres_hourly_processed, sso_hourly_processed, pipp_hourly_processed, 'hourly')
    final_monthly_df = combine_data(cres_monthly_processed, sso_monthly_processed, pipp_monthly_processed, 'monthly')
    final_daily_df = combine_data(plc_daily_processed, nspl_daily_processed, pipp_daily_processed, 'daily')

    # Calculate ufe data
    ufe_processed = process_UFE_data(ufe_df, final_hourly_df, edc_name)

    # Plot data for correction
    print('Saving plots...')
    plot_path = {}
    plot_path.update(plot_monthly_data(final_monthly_df, etl_report_output_path))
    plot_path.update(plot_hourly_data(final_hourly_df, etl_report_output_path))
    plot_path.update(plot_daily_data(final_daily_df, etl_report_output_path))
    plot_path.update(plot_UFE_data(ufe_processed, etl_report_output_path))
    plot_path.update(plot_GovtAggr_data(govt_aggr_processed, etl_report_output_path))

    # Generate ETL report
    report_plots_path = save_plot_path(plot_path)
    report_keystats_table = generate_keystats(final_monthly_df, final_hourly_df, final_daily_df, ufe_processed,
                                              govt_aggr_processed)
    generate_report(etl_report_output_path, report_keystats_table, report_plots_path)

    # Save processed data
    print('Saving data...')
    save_processed_data(final_hourly_df, hourly_output_path, 'hourly')
    save_processed_data(final_monthly_df, monthly_output_path, 'monthly')
    save_processed_data(final_daily_df, daily_output_path, 'daily')
    save_processed_data(ufe_processed, ufe_output_path, 'UFE')
    save_processed_data(govt_aggr_processed, govt_aggr_output_path, 'GovtAggr')

if __name__ == "__main__":
    base_path = 'C:\\Users\\5DIntern3_2024\\Work\\AEP_Ohio'
    data_extract = False
    deration_factor_filename = 'DerationFactor_AEPOHIO_RESID_AGG.csv'
    main(base_path, data_extract, deration_factor_filename)
