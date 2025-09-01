import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import urllib.parse
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from jinja2 import Environment, FileSystemLoader
from dateutil import parser
import base64
import datetime

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
            # Considering OH_FE always keeps 2 copy for each file, skip the second search result.
            if keyword_counts[keyword_in_file] != 2:
                new_file_name = f"{keyword_in_file}_fe_oh_{keyword_counts[keyword_in_file]}.xls" if file_name.endswith(
                    '.xls') else f"{keyword_in_file}_fe_oh_{keyword_counts[keyword_in_file]}.xlsx"
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


def load_hourly_volume_data(hourly_volume_file_path):
    file_sheets = pd.read_excel(hourly_volume_file_path, sheet_name=None)

    for sheet_name in file_sheets:
        if sheet_name == 'PIPP Load 2019+':
            df = pd.read_excel(hourly_volume_file_path, sheet_name=sheet_name, header=[5, 6])
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
            file_sheets[sheet_name] = df

        elif sheet_name == 'PIPP 08.2023+':
            file_sheets[sheet_name] = pd.read_excel(hourly_volume_file_path, sheet_name=sheet_name, header=1)

        elif sheet_name == 'OH Hrly Load 073122':
            df = pd.read_excel(hourly_volume_file_path, sheet_name=sheet_name, header=[8, 9])
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
            file_sheets[sheet_name] = df

        elif sheet_name == 'OH Hrly Load 08.2022+':
            df = pd.read_excel(hourly_volume_file_path, sheet_name=sheet_name, header=[6, 7])
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
            file_sheets[sheet_name] = df

    return file_sheets


def load_ufe_data(ufe_file_path):
    file_sheets = pd.read_excel(ufe_file_path, sheet_name=None)
    for sheet_name in file_sheets:
        if sheet_name == 'UFE Factors 2019+':
            file_sheets[sheet_name] = pd.read_excel(ufe_file_path, sheet_name=sheet_name, header=6)

        elif sheet_name == 'UFE August 2023+':
            file_sheets[sheet_name] = pd.read_excel(ufe_file_path, sheet_name=sheet_name, header=0)

    return file_sheets


def load_monthly_data(monthly_customer_count_file_path):
    file_sheets = pd.read_excel(monthly_customer_count_file_path, sheet_name=None)

    for sheet_name in file_sheets:
        if sheet_name == 'Shopping':
            df = pd.read_excel(monthly_customer_count_file_path, sheet_name=sheet_name, header=[0,1, 2])
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
            file_sheets[sheet_name] = df

        elif sheet_name == 'PIPP Count':
            file_sheets[sheet_name] = pd.read_excel(monthly_customer_count_file_path, sheet_name=sheet_name, header=3)
    return file_sheets


def load_daily_data(daily_volume_file_path):
    file_sheets = pd.read_excel(daily_volume_file_path, sheet_name=None)

    for sheet_name in file_sheets:
        if sheet_name == 'PLC 2019+':
            df = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name, header=[2, 3])
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
            file_sheets[sheet_name] = df

        elif sheet_name == 'NSPL 2019+':
            df = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name, header=[2, 3])
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
            file_sheets[sheet_name] = df

        elif sheet_name == 'PIPP PLC 2019+':
            file_sheets[sheet_name] = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name, header=4)

        elif sheet_name == 'PIPP NSPL 2019+':
            file_sheets[sheet_name] = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name, header=4)

        elif sheet_name == 'PLC 11.2022+':
            file_sheets[sheet_name] = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name, header=2)

        elif sheet_name == 'NSPL 11.2022+':
            file_sheets[sheet_name] = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name, header=2)

        elif sheet_name == 'PIPP PLC 11.2022+':
            file_sheets[sheet_name] = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name, header=3)

        elif sheet_name == 'PIPP NSPL 11.2022+':
            file_sheets[sheet_name] = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name, header=3)

    return file_sheets


def process_deration_factor(deration_factor, edc_name):
    deration_factor['Datetime_beginning_utc'] = deration_factor['Datetime_beginning_utc'].astype(str)
    deration_factor['Datetime_beginning_utc'] = pd.to_datetime(deration_factor['Datetime_beginning_utc'])

    deration_factor_df = pd.DataFrame({
        'Datetime_beginning_utc': deration_factor['Datetime_beginning_utc'],
        'EDCName': edc_name,
        'DerationFactor': deration_factor['DerationFactor']
    })

    return deration_factor_df


def process_ufe_data(ufe_sheets):

    ufe_df_prior_23 = ufe_sheets['UFE Factors 2019+'].copy()
    ufe_df_prior_23['date'] = pd.to_datetime(ufe_df_prior_23['Date'].str.split(' ').str[0], errors='coerce')
    ufe_df_prior_23.loc[ufe_df_prior_23['Hour'].notna(), 'date'] = pd.to_datetime(
        ufe_df_prior_23.loc[ufe_df_prior_23['Hour'].notna(), 'Date'])

    # Hard Code to fix data format issue on 2023-03-13 01:00 EPT
    ufe_df_prior_23.loc[(ufe_df_prior_23['Date'] == '11/6/2022  01:00 d'), 'Hour'] = 1
    ufe_df_prior_23.loc[(ufe_df_prior_23['Date'] == '11/6/2022  01:00 s'), 'Hour'] = 2
    ufe_df_prior_23.loc[(ufe_df_prior_23['Date'] == '03/12/2023 03:00d'), 'Hour'] = 2

    ufe_df_prior_23.loc[ufe_df_prior_23['Hour'].isna(), 'Hour'] = \
        ufe_df_prior_23.loc[ufe_df_prior_23['Hour'].isna(), 'Date'].str.split(' ').str[1].str[:2].astype(int)

    # Convert Hour Ending to Hour Beginning
    ufe_df_prior_23['Hour'] = ufe_df_prior_23['Hour'].astype(int) - 1

    # Identify the duplicate hours and reformat it
    indices_24 = ufe_df_prior_23[ufe_df_prior_23['Hour'] == 24].index

    for index_24 in indices_24:
        if index_24 >= 22:
            ufe_df_prior_23.loc[index_24 - 22:index_24, 'Hour'] -= 1

    # Based on observation, UFE data should be in EST
    datetime_beginning_ept = pd.to_datetime(ufe_df_prior_23['date']) + pd.to_timedelta(ufe_df_prior_23['Hour'],
                                                                                       unit="h")
    ufe_df_prior_23["datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York',
                                                                                        ambiguous='infer')
    ufe_df_prior_23["Datetime_beginning_utc"] = ufe_df_prior_23["datetime_beginning_ept"].dt.tz_convert('UTC')

    ufe_df_list = []
    ufe_data = {
        'Datetime_beginning_utc': ufe_df_prior_23["Datetime_beginning_utc"],
        'UFE MW': ufe_df_prior_23['UFE MW'],
        'UFE Factor': ufe_df_prior_23['UFE Factor']
    }
    ufe_df_list.append(pd.DataFrame(ufe_data))

    # Process post 23 UFE data
    ufe_df_post_23 = ufe_sheets['UFE August 2023+'].copy()
    ufe_df_post_23['date'] = ufe_df_post_23['DATE'].str.split(' ').str[0]
    ufe_df_post_23['hour'] = ufe_df_post_23['DATE'].str.split(' ').str[1].str[:2].astype(int) - 1

    # Hard code to fix date format issue on 2023-11-05, 2023-03-10
    ufe_df_post_23.loc[(ufe_df_post_23['DATE'] == '2023-11-05 01::00s'), 'hour'] = 1
    ufe_df_post_23.loc[(ufe_df_post_23['DATE'] == '2024-03-10 03:00d'), 'hour'] = 1

    def parse_mixed_date_formats(date_str):
        try:
            return pd.to_datetime(date_str, format='%Y-%m-%d')
        except (ValueError, TypeError):
            try:
                return pd.to_datetime(date_str, format='%m/%d/%Y')
            except (ValueError, TypeError):
                return pd.to_datetime(date_str, errors='coerce')

    ufe_df_post_23['date'] = ufe_df_post_23['date'].apply(parse_mixed_date_formats)

    datetime_beginning_ept = pd.to_datetime(ufe_df_post_23['date']) + pd.to_timedelta(ufe_df_post_23['hour'],
                                                                                      unit="h")
    ufe_df_post_23["datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York',
                                                                                        ambiguous='infer')
    ufe_df_post_23["Datetime_beginning_utc"] = ufe_df_post_23["datetime_beginning_ept"].dt.tz_convert('UTC')
    ufe_data = {
        'Datetime_beginning_utc': ufe_df_post_23["Datetime_beginning_utc"],
        'UFE MW': ufe_df_post_23['UFE (MWH)'],
        'UFE Factor': ufe_df_post_23['UFE FACTOR']
    }
    ufe_df_list.append(pd.DataFrame(ufe_data))
    ufe_df_processed = pd.concat(ufe_df_list, ignore_index=True)

    return ufe_df_processed


def process_hourly_volume(hourly_volume_sheets, ufe_df_processed, deration_factor_processed, edc_name):
    # Process prior 2022 data
    hourly_df_prior22 = hourly_volume_sheets['OH Hrly Load 073122'].copy()

    # Fix the wrong date input for 3/13/2022
    fix_index = hourly_df_prior22.loc[(hourly_df_prior22['Unnamed: 0_level_0_Date'] == '3/13/2022') & (hourly_df_prior22['Unnamed: 1_level_0_Hour Ending'] == 1),:].index[1]
    hourly_df_prior22.loc[fix_index, "Unnamed: 0_level_0_Date"] = '3/14/2022'

    hourly_df_prior22['Unnamed: 0_level_0_Date'] = pd.to_datetime(
        hourly_df_prior22['Unnamed: 0_level_0_Date'].astype(str))
    hourly_df_prior22['Unnamed: 1_level_0_Hour Ending'] = hourly_df_prior22['Unnamed: 1_level_0_Hour Ending'].astype(
        int) - 1

    # Identify the duplicate hours and reformat it
    indices_24 = hourly_df_prior22[hourly_df_prior22['Unnamed: 1_level_0_Hour Ending'] == 24].index

    for index_24 in indices_24:
        if index_24 >= 24:
            hourly_df_prior22.loc[index_24 - 22:index_24, 'Unnamed: 1_level_0_Hour Ending'] -= 1

    datetime_beginning_ept = hourly_df_prior22['Unnamed: 0_level_0_Date'] + pd.to_timedelta(
        hourly_df_prior22['Unnamed: 1_level_0_Hour Ending'], unit="h")
    hourly_df_prior22["datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York',
                                                                                        ambiguous='infer')
    hourly_df_prior22["Datetime_beginning_utc"] = hourly_df_prior22["datetime_beginning_ept"].dt.tz_convert("UTC")

    # Merge with deration factor and ufe
    hourly_df_prior22 = hourly_df_prior22.merge(deration_factor_processed, left_on='Datetime_beginning_utc',
                                                right_on='Datetime_beginning_utc')
    hourly_df_prior22 = hourly_df_prior22.merge(ufe_df_processed, left_on='Datetime_beginning_utc',
                                                right_on='Datetime_beginning_utc')

    hourly_volume_list = []
    ufe_volume_list = []
    for customer_class in ['Commerical', 'Industrial', 'Residential']:
        hourly_volume_data = {
            'Datetime_beginning_utc': hourly_df_prior22['Datetime_beginning_utc'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class[:3].upper()}',
            'VolumeType': 'Wholesale_Derated',
            'EGS_HourlyVolume': (hourly_df_prior22[f'CEI-Shopped_{customer_class}'] + hourly_df_prior22[
                f'OE-Shopped_{customer_class}'] + hourly_df_prior22[f'TE-Shopped_{customer_class}']) / 1000 * (
                                            1 - hourly_df_prior22['DerationFactor']) * hourly_df_prior22['UFE Factor'],
            'Default_HourlyVolume': (hourly_df_prior22[f'CEI-Non-Shopped_{customer_class}'] + hourly_df_prior22[
                f'OE-Non-Shopped_{customer_class}'] + hourly_df_prior22[f'TE-Non-Shopped_{customer_class}']) / 1000 * (
                                                1 - hourly_df_prior22['DerationFactor']) * hourly_df_prior22[
                                        'UFE Factor'],
            'Eligible_HourlyVolume': (hourly_df_prior22[f'CEI-Shopped_{customer_class}'] + hourly_df_prior22[
                f'OE-Shopped_{customer_class}'] + hourly_df_prior22[f'TE-Shopped_{customer_class}'] + hourly_df_prior22[
                                          f'CEI-Non-Shopped_{customer_class}'] + hourly_df_prior22[
                                          f'OE-Non-Shopped_{customer_class}'] + hourly_df_prior22[
                                          f'TE-Non-Shopped_{customer_class}']) / 1000 * (
                                                 1 - hourly_df_prior22['DerationFactor']) * hourly_df_prior22[
                                         'UFE Factor'],
            'VolumeComment': ''
        }
        hourly_volume_list.append(pd.DataFrame(hourly_volume_data))

        ufe_volume_data = {
            'Datetime_beginning_utc': hourly_df_prior22['Datetime_beginning_utc'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class[:3].upper()}',
            'VolumeType': 'UFE',
            'EGS_HourlyVolume': (hourly_df_prior22[f'CEI-Shopped_{customer_class}'] + hourly_df_prior22[
                f'OE-Shopped_{customer_class}'] + hourly_df_prior22[f'TE-Shopped_{customer_class}']) / 1000 * (
                                            1 - hourly_df_prior22['DerationFactor']) * (
                                            hourly_df_prior22['UFE Factor'] - 1),
            'Default_HourlyVolume': (hourly_df_prior22[f'CEI-Non-Shopped_{customer_class}'] + hourly_df_prior22[
                f'OE-Non-Shopped_{customer_class}'] + hourly_df_prior22[f'TE-Non-Shopped_{customer_class}']) / 1000 * (
                                                1 - hourly_df_prior22['DerationFactor']) * (
                                                hourly_df_prior22['UFE Factor'] - 1),
            'Eligible_HourlyVolume': (hourly_df_prior22[f'CEI-Shopped_{customer_class}'] + hourly_df_prior22[
                f'OE-Shopped_{customer_class}'] + hourly_df_prior22[f'TE-Shopped_{customer_class}'] + hourly_df_prior22[
                                          f'CEI-Non-Shopped_{customer_class}'] + hourly_df_prior22[
                                          f'OE-Non-Shopped_{customer_class}'] + hourly_df_prior22[
                                          f'TE-Non-Shopped_{customer_class}']) / 1000 * (
                                                 1 - hourly_df_prior22['DerationFactor']) * (
                                                 hourly_df_prior22['UFE Factor'] - 1),
            'VolumeComment': ''
        }
        ufe_volume_list.append(pd.DataFrame(ufe_volume_data))

    # Process post 2022 data
    hourly_df_post22 = hourly_volume_sheets['OH Hrly Load 08.2022+'].copy()
    hourly_df_post22['date'] = hourly_df_post22['Unnamed: 0_level_0_Date/Hour'].str.split(' ').str[0]
    hourly_df_post22['hour'] = hourly_df_post22['Unnamed: 0_level_0_Date/Hour'].str.split(' ').str[1]

    # Hard Code to fix data format issue on 2023-03-13 01:00 EPT
    hourly_df_post22.loc[(hourly_df_post22['Unnamed: 0_level_0_Date/Hour'] == '03/13/2023  01:00'), 'hour'] = '01:00'
    # Hard Code to fix wrong daylight saving duplicates issue on 11/06/2022 01:00
    hourly_df_post22.loc[(hourly_df_post22['Unnamed: 0_level_0_Date/Hour'] == '11/06/2022 01:00 s'), 'hour'] = '02:00'
    # Hard Code to fix wrong daylight saving duplicates issue on 03/12/2023 03:00
    hourly_df_post22.loc[(hourly_df_post22['Unnamed: 0_level_0_Date/Hour'] == '03/12/2023 03:00d'), 'hour'] = '02:00'
    # Hard Code to fix wrong daylight saving duplicates issue on 11/05/2023 01:00
    hourly_df_post22.loc[(hourly_df_post22['Unnamed: 0_level_0_Date/Hour'] == '2023-11-05 01::00s'), 'hour'] = '02:00'
    # Hard Code to fix wrong daylight saving duplicates issue on 10/03/2024 02:00
    hourly_df_post22.loc[(hourly_df_post22['Unnamed: 0_level_0_Date/Hour'] == '2024-03-10 03:00d'), 'hour'] = '02:00'

    hourly_df_post22['hour_beginning'] = hourly_df_post22['hour'].str[:2].astype(int) - 1

    def parse_mixed_date_formats(date_str):
        try:
            return pd.to_datetime(date_str, format='%Y-%m-%d')
        except (ValueError, TypeError):
            try:
                return pd.to_datetime(date_str, format='%m/%d/%Y')
            except (ValueError, TypeError):
                return pd.to_datetime(date_str, errors='coerce')

    hourly_df_post22['date'] = hourly_df_post22['date'].apply(parse_mixed_date_formats)

    datetime_beginning_ept = hourly_df_post22['date'] + pd.to_timedelta(hourly_df_post22['hour_beginning'], unit="h")
    hourly_df_post22["datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York',
                                                                                       ambiguous='infer')
    hourly_df_post22["Datetime_beginning_utc"] = hourly_df_post22["datetime_beginning_ept"].dt.tz_convert("UTC")

    # Merge with deration factor and ufe
    hourly_df_post22 = hourly_df_post22.merge(deration_factor_processed, left_on='Datetime_beginning_utc',
                                              right_on='Datetime_beginning_utc')
    hourly_df_post22 = hourly_df_post22.merge(ufe_df_processed, left_on='Datetime_beginning_utc',
                                              right_on='Datetime_beginning_utc')

    for customer_class in ['Commerical', 'Industrial', 'Residential']:
        hourly_volume_data = {
            'Datetime_beginning_utc': hourly_df_post22['Datetime_beginning_utc'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class[:3].upper()}',
            'VolumeType': 'Wholesale_Derated',
            'EGS_HourlyVolume': (hourly_df_post22[f'CEI-Shopped_{customer_class}'] + hourly_df_post22[
                f'OE-Shopped_{customer_class}'] + hourly_df_post22[f'TE-Shopped_{customer_class}']) / 1000 * (
                                            1 - hourly_df_post22['DerationFactor']) * hourly_df_post22['UFE Factor'],
            'Default_HourlyVolume': (hourly_df_post22[f'CEI-Non-Shopped_{customer_class}'] + hourly_df_post22[
                f'OE-Non-Shopped_{customer_class}'] + hourly_df_post22[f'TE-Non-Shopped_{customer_class}']) / 1000 * (
                                                1 - hourly_df_post22['DerationFactor']) * hourly_df_post22[
                                        'UFE Factor'],
            'Eligible_HourlyVolume': (hourly_df_post22[f'CEI-Shopped_{customer_class}'] + hourly_df_post22[
                f'OE-Shopped_{customer_class}'] + hourly_df_post22[f'TE-Shopped_{customer_class}'] + hourly_df_post22[
                                          f'CEI-Non-Shopped_{customer_class}'] + hourly_df_post22[
                                          f'OE-Non-Shopped_{customer_class}'] + hourly_df_post22[
                                          f'TE-Non-Shopped_{customer_class}']) / 1000 * (
                                                 1 - hourly_df_post22['DerationFactor']) * hourly_df_post22[
                                         'UFE Factor'],
            'VolumeComment': ''
        }
        hourly_volume_list.append(pd.DataFrame(hourly_volume_data))

        ufe_volume_data = {
            'Datetime_beginning_utc': hourly_df_post22['Datetime_beginning_utc'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class[:3].upper()}',
            'VolumeType': 'UFE',
            'EGS_HourlyVolume': (hourly_df_post22[f'CEI-Shopped_{customer_class}'] + hourly_df_post22[
                f'OE-Shopped_{customer_class}'] + hourly_df_post22[f'TE-Shopped_{customer_class}']) / 1000 * (
                                            1 - hourly_df_post22['DerationFactor']) * (
                                            hourly_df_post22['UFE Factor'] - 1),
            'Default_HourlyVolume': (hourly_df_post22[f'CEI-Non-Shopped_{customer_class}'] + hourly_df_post22[
                f'OE-Non-Shopped_{customer_class}'] + hourly_df_post22[f'TE-Non-Shopped_{customer_class}']) / 1000 * (
                                                1 - hourly_df_post22['DerationFactor']) * (
                                                hourly_df_post22['UFE Factor'] - 1),
            'Eligible_HourlyVolume': (hourly_df_post22[f'CEI-Shopped_{customer_class}'] + hourly_df_post22[
                f'OE-Shopped_{customer_class}'] + hourly_df_post22[f'TE-Shopped_{customer_class}'] + hourly_df_post22[
                                          f'CEI-Non-Shopped_{customer_class}'] + hourly_df_post22[
                                          f'OE-Non-Shopped_{customer_class}'] + hourly_df_post22[
                                          f'TE-Non-Shopped_{customer_class}']) / 1000 * (
                                                 1 - hourly_df_post22['DerationFactor']) * (
                                                 hourly_df_post22['UFE Factor'] - 1),
            'VolumeComment': ''
        }
        ufe_volume_list.append(pd.DataFrame(ufe_volume_data))

    # Process PIPP data
    pipp_df_prior23 = hourly_volume_sheets['PIPP Load 2019+'].copy()
    # Deal with data overlapping

    pipp_df_prior23['VALUE_Unnamed: 0_level_1'] = pipp_df_prior23['VALUE_Unnamed: 0_level_1'].astype(str)
    # Fix the wrong date input for 3/8/2020
    fix_index = pipp_df_prior23.loc[(pipp_df_prior23['VALUE_Unnamed: 0_level_1'] == '2020-03-08 00:00:00') & (pipp_df_prior23['VALUE_Unnamed: 1_level_1'] == 1),:].index[1]
    pipp_df_prior23.loc[fix_index, "VALUE_Unnamed: 0_level_1"] = '2020-03-09 00:00:00'
    # Fix the wrong date input for '3/10/2019'
    fix_index = pipp_df_prior23.loc[(pipp_df_prior23['VALUE_Unnamed: 0_level_1'] == '2019-03-10 00:00:00') & (pipp_df_prior23['VALUE_Unnamed: 1_level_1'] == 1),:].index[1]
    pipp_df_prior23.loc[fix_index, "VALUE_Unnamed: 0_level_1"] = '2019-03-11 00:00:00'
    # Fix the wrong date input for '03/04/2021'
    fix_index = pipp_df_prior23.loc[(pipp_df_prior23['VALUE_Unnamed: 0_level_1'] == '2021-03-14 00:00:00') & (pipp_df_prior23['VALUE_Unnamed: 1_level_1'] == 1),:].index[1]
    pipp_df_prior23.loc[fix_index, "VALUE_Unnamed: 0_level_1"] = '2021-03-15 00:00:00'
    # Fix the wrong date input for '03/04/2021'
    fix_index = pipp_df_prior23.loc[(pipp_df_prior23['VALUE_Unnamed: 0_level_1'] == '2022-03-13 00:00:00') & (pipp_df_prior23['VALUE_Unnamed: 1_level_1'] == 1),:].index[1]
    pipp_df_prior23.loc[fix_index, "VALUE_Unnamed: 0_level_1"] = '2022-03-14 00:00:00'

    pipp_df_prior23['date_str'] = pipp_df_prior23['VALUE_Unnamed: 0_level_1'].str.split(' ').str[0]

    def parse_mixed_date_formats(date_str):
        try:
            return pd.to_datetime(date_str, format='%Y-%m-%d')
        except (ValueError, TypeError):
            try:
                return pd.to_datetime(date_str, format='%m/%d/%Y')
            except (ValueError, TypeError):
                return pd.to_datetime(date_str, errors='coerce')

    pipp_df_prior23['date'] = pipp_df_prior23['date_str'].apply(parse_mixed_date_formats)

    pipp_df_prior23['hour'] = pipp_df_prior23['VALUE_Unnamed: 1_level_1']
    pipp_df_prior23.loc[pipp_df_prior23['hour'].notna(), 'hour'] = pipp_df_prior23.loc[pipp_df_prior23['hour'].notna(), 'hour'].astype(int)

    # Hard Code to fix data format issue on 11/06/2022 01:00 and 03/13/2023 01:00
    pipp_df_prior23.loc[pipp_df_prior23['VALUE_Unnamed: 0_level_1'] == '11/06/2022  01:00 d', 'hour'] = 1
    pipp_df_prior23.loc[pipp_df_prior23['VALUE_Unnamed: 0_level_1'] == '11/06/2022  01:00 s', 'hour'] = 2
    pipp_df_prior23.loc[pipp_df_prior23['VALUE_Unnamed: 0_level_1'] == '03/13/2023  01:00', 'hour'] = 1

    # Hard code to fix wrong daylight saving issue on 03/12/2023 03:00
    pipp_df_prior23.loc[pipp_df_prior23['VALUE_Unnamed: 0_level_1'] == '03/12/2023 03:00d', 'hour'] = 2

    pipp_df_prior23.loc[pipp_df_prior23['hour'].isna(), 'hour'] = \
    pipp_df_prior23.loc[pipp_df_prior23['hour'].isna(), 'VALUE_Unnamed: 0_level_1'].str.split(' ').str[1].str[:2].astype(int)

    pipp_df_prior23['hour'] = pipp_df_prior23['hour'].astype(int) - 1

    datetime_beginning_ept = pd.to_datetime(pipp_df_prior23['date']) + pd.to_timedelta(pipp_df_prior23['hour'],
                                                                                       unit="h")
    pipp_df_prior23["datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York',
                                                                                      ambiguous='infer')
    pipp_df_prior23["Datetime_beginning_utc"] = pipp_df_prior23["datetime_beginning_ept"].dt.tz_convert("UTC")
    # Deal with data overlap
    pipp_df_prior23 = pipp_df_prior23[pipp_df_prior23['date'] <= '05/31/2023']

    # Merge with deration factor and ufe
    pipp_df_prior23 = pipp_df_prior23.merge(deration_factor_processed, left_on='Datetime_beginning_utc',
                                            right_on='Datetime_beginning_utc')
    pipp_df_prior23 = pipp_df_prior23.merge(ufe_df_processed, left_on='Datetime_beginning_utc',
                                            right_on='Datetime_beginning_utc')

    hourly_pipp_data = {
        'Datetime_beginning_utc': pipp_df_prior23["Datetime_beginning_utc"],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'Wholesale_Derated',
        'EGS_HourlyVolume': 0,
        'Default_HourlyVolume': pipp_df_prior23['Total_PIP'] / 1000 * (1 - pipp_df_prior23['DerationFactor']) *
                                pipp_df_prior23['UFE Factor'],
        'Eligible_HourlyVolume': pipp_df_prior23['Total_PIP'] / 1000 * (1 - pipp_df_prior23['DerationFactor']) *
                                 pipp_df_prior23['UFE Factor'],
        'VolumeComment': ''
    }
    hourly_volume_list.append(pd.DataFrame(hourly_pipp_data))

    ufe_pipp_data = {
        'Datetime_beginning_utc': pipp_df_prior23["Datetime_beginning_utc"],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'UFE',
        'EGS_HourlyVolume': 0,
        'Default_HourlyVolume': pipp_df_prior23['Total_PIP'] / 1000 * (1 - pipp_df_prior23['DerationFactor']) * (
                    pipp_df_prior23['UFE Factor'] - 1),
        'Eligible_HourlyVolume': pipp_df_prior23['Total_PIP'] / 1000 * (1 - pipp_df_prior23['DerationFactor']) * (
                    pipp_df_prior23['UFE Factor'] - 1),
        'VolumeComment': ''
    }
    ufe_volume_list.append(pd.DataFrame(ufe_pipp_data))

    pipp_df_post23 = hourly_volume_sheets['PIPP 08.2023+'].copy()

    pipp_df_post23['date'] = pipp_df_post23['DATE/TIME'].str.split(' ').str[0]
    pipp_df_post23['hour'] = pipp_df_post23['DATE/TIME'].str.split(' ').str[1].str[:2]

    # Hard code to fix the wrong duplicate daylight saving hour at 2023-11-05 01:00
    pipp_df_post23.loc[pipp_df_post23['DATE/TIME'] == '2023-11-05 01::00s', 'hour'] = 2
    # Hard code to fix the wrong duplicate daylight saving hour at 2024-03-10 02:00
    pipp_df_post23.loc[pipp_df_post23['DATE/TIME'] == '2024-03-10 03:00d', 'hour'] = 2

    pipp_df_post23['hour'] = pipp_df_post23['hour'].astype(int) - 1

    def parse_mixed_date_formats(date_str):
        try:
            return pd.to_datetime(date_str, format='%Y-%m-%d')
        except (ValueError, TypeError):
            try:
                return pd.to_datetime(date_str, format='%m/%d/%Y')
            except (ValueError, TypeError):
                return pd.to_datetime(date_str, errors='coerce')

    pipp_df_post23['date'] = pipp_df_post23['date'].apply(parse_mixed_date_formats)

    datetime_beginning_ept = pipp_df_post23['date'] + pd.to_timedelta(pipp_df_post23['hour'], unit="h")
    pipp_df_post23["datetime_beginning_ept"] = datetime_beginning_ept.dt.tz_localize(tz='America/New_York',
                                                                                     ambiguous='infer')
    pipp_df_post23["Datetime_beginning_utc"] = pipp_df_post23["datetime_beginning_ept"].dt.tz_convert("UTC")

    # Merge with deration factor and ufe
    pipp_df_post23 = pipp_df_post23.merge(deration_factor_processed, left_on='Datetime_beginning_utc',
                                          right_on='Datetime_beginning_utc')
    pipp_df_post23 = pipp_df_post23.merge(ufe_df_processed, left_on='Datetime_beginning_utc',
                                          right_on='Datetime_beginning_utc')

    hourly_pipp_data = {
        'Datetime_beginning_utc': pipp_df_post23["Datetime_beginning_utc"],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'Wholesale_Derated',
        'EGS_HourlyVolume': 0,
        'Default_HourlyVolume': (pipp_df_post23['CE'] + pipp_df_post23['OE'] + pipp_df_post23['TE']) / 1000 * (
                    1 - pipp_df_post23['DerationFactor']) * pipp_df_post23['UFE Factor'],
        'Eligible_HourlyVolume': (pipp_df_post23['CE'] + pipp_df_post23['OE'] + pipp_df_post23['TE']) / 1000 * (
                    1 - pipp_df_post23['DerationFactor']) * pipp_df_post23['UFE Factor'],
        'VolumeComment': ''
    }
    hourly_volume_list.append(pd.DataFrame(hourly_pipp_data))

    ufe_pipp_data = {
        'Datetime_beginning_utc': pipp_df_post23["Datetime_beginning_utc"],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'UFE',
        'EGS_HourlyVolume': 0,
        'Default_HourlyVolume': (pipp_df_post23['CE'] + pipp_df_post23['OE'] + pipp_df_post23['TE']) / 1000 * (
                    1 - pipp_df_post23['DerationFactor']) * (pipp_df_post23['UFE Factor'] - 1),
        'Eligible_HourlyVolume': (pipp_df_post23['CE'] + pipp_df_post23['OE'] + pipp_df_post23['TE']) / 1000 * (
                    1 - pipp_df_post23['DerationFactor']) * (pipp_df_post23['UFE Factor'] - 1),
        'VolumeComment': ''
    }
    ufe_volume_list.append(pd.DataFrame(ufe_pipp_data))

    hourly_volume_processed = pd.concat(hourly_volume_list, ignore_index=True)
    hourly_volume_processed = hourly_volume_processed.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'],
                                                                  ignore_index=True)

    ufe_volume_processed = pd.concat(ufe_volume_list, ignore_index=True)
    ufe_volume_processed = ufe_volume_processed.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'],
                                                            ignore_index=True)
    return hourly_volume_processed, ufe_volume_processed


def process_monthly_customer_count(monthly_sheets, edc_name):
    # Process monthly data
    monthly_customer_counts_df = monthly_sheets['Shopping'].copy()
    monthly_customer_counts_df['FlowMonth'] = pd.to_datetime(
        monthly_customer_counts_df[['Unnamed: 0_level_0_Unnamed: 0_level_1_Year', 'EDC_Class_Month']].astype(str).agg(
            '-'.join, axis=1), format='%Y-%m').dt.strftime('%Y-%m-01')

    monthly_customer_count_list = []
    for customer_class in ['COMM', 'IND', 'RES']:
        monthly_customer_count_data = {
            'FlowMonth': monthly_customer_counts_df['FlowMonth'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class[:3].upper()}',
            'VolumeType': 'CustomerCount',
            'EGS_MonthlyVolume': (monthly_customer_counts_df[f'CEI_{customer_class}_Shopping Customers'] +
                                  monthly_customer_counts_df[f'OE_{customer_class}_Shopping Customers'] +
                                  monthly_customer_counts_df[f'TE_{customer_class}_Shopping Customers']).round(0),
            'Default_MonthlyVolume': (monthly_customer_counts_df[f'CEI_{customer_class}_Customers'] +
                                      monthly_customer_counts_df[f'OE_{customer_class}_Customers'] +
                                      monthly_customer_counts_df[f'TE_{customer_class}_Customers']).round(0) - (
                                                 monthly_customer_counts_df[
                                                     f'CEI_{customer_class}_Shopping Customers'] +
                                                 monthly_customer_counts_df[f'OE_{customer_class}_Shopping Customers'] +
                                                 monthly_customer_counts_df[
                                                     f'TE_{customer_class}_Shopping Customers']).round(0),
            'Eligible_MonthlyVolume': (
                        monthly_customer_counts_df[f'CEI_{customer_class}_Customers'] + monthly_customer_counts_df[
                    f'OE_{customer_class}_Customers'] + monthly_customer_counts_df[
                            f'TE_{customer_class}_Customers']).round(0),
            'VolumeComment': ''
        }
        monthly_customer_count_list.append(pd.DataFrame(monthly_customer_count_data))

    # Process PIPP data
    pipp_customer_counts_df = monthly_sheets['PIPP Count'].copy()

    def parse_date(date_str):
        # Try to parse using the month-year format
        try:
            return pd.to_datetime(date_str, format='%b-%y', errors='raise')
        except ValueError:
            pass
        # If the first format fails, try the day-month-year format
        try:
            return pd.to_datetime(date_str, format='%m/%d/%Y', errors='raise')
        except ValueError:
            pass
        # Fall back to a more flexible parser
        try:
            return parser.parse(date_str)
        except (ValueError, TypeError):
            return pd.NaT

    # Fix the format issue of 2023-09 data
    pipp_customer_counts_df.loc[pipp_customer_counts_df['Unnamed: 0'] == 'Sept-23', 'Unnamed: 0'] = 'Sep-23'
    pipp_customer_counts_df['FlowMonth'] = pipp_customer_counts_df['Unnamed: 0'].apply(parse_date).dt.strftime(
        '%Y-%m-01')

    monthly_customer_count_data = {
        'FlowMonth': pipp_customer_counts_df['FlowMonth'],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': 0,
        'Default_MonthlyVolume': pipp_customer_counts_df['Total'].round(0),
        'Eligible_MonthlyVolume': pipp_customer_counts_df['Total'].round(0),
        'VolumeComment': ''
    }
    monthly_customer_count_list.append(pd.DataFrame(monthly_customer_count_data))

    monthly_customer_count_df_processed = pd.concat(monthly_customer_count_list, ignore_index=True)
    monthly_customer_count_df_processed = monthly_customer_count_df_processed.sort_values(
        by=['FlowMonth', 'CustomerClass'], ignore_index=True)

    return monthly_customer_count_df_processed


def process_daily_volume(daily_volume_sheets, edc_name):
    daily_volume_list = []

    # Process plc data
    plc_prior22 = daily_volume_sheets['PLC 2019+'].copy()
    plc_prior22['FlowDate'] = pd.to_datetime(plc_prior22['Unnamed: 0_level_0_Unnamed: 0_level_1']).dt.strftime(
        '%Y-%m-%d')
    for customer_class in ['COM', 'IND', 'RES']:
        daily_data = {
            'FlowDate': plc_prior22['FlowDate'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class}',
            'VolumeType': 'PLC_Scaled',
            'EGS_DailyVolume': plc_prior22[f'Shopped_{customer_class}'] / 1000,
            'Default_DailyVolume': plc_prior22[f'NonShopped_{customer_class}'] / 1000,
            'Eligible_DailyVolume': (plc_prior22[f'Shopped_{customer_class}'] + plc_prior22[
                f'NonShopped_{customer_class}']) / 1000,
            'VolumeComment': ''
        }
        daily_volume_list.append(pd.DataFrame(daily_data))

    plc_post22 = daily_volume_sheets['PLC 11.2022+'].copy()
    plc_post22['FlowDate'] = pd.to_datetime(plc_post22['Date']).dt.strftime('%Y-%m-%d')
    for customer_class in ['COM', 'IND', 'RES']:
        daily_data = {
            'FlowDate': plc_post22['FlowDate'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class}',
            'VolumeType': 'PLC_Scaled',
            'EGS_DailyVolume': (plc_post22[f'Shopping - CE - {customer_class} - kWh'] + plc_post22[
                f'Shopping - OE - {customer_class} - kWh'] + plc_post22[
                                    f'Shopping - TE - {customer_class} - kWh']) / 1000,
            'Default_DailyVolume': (plc_post22[f'Non Shopping - CE - {customer_class} - kWh'] + plc_post22[
                f'Non Shopping - OE - {customer_class} - kWh'] + plc_post22[
                                        f'Non Shopping - TE - {customer_class} - kWh']) / 1000,
            'Eligible_DailyVolume': (plc_post22[f'Shopping - CE - {customer_class} - kWh'] + plc_post22[
                f'Shopping - OE - {customer_class} - kWh'] + plc_post22[f'Shopping - TE - {customer_class} - kWh'] +
                                     plc_post22[f'Non Shopping - CE - {customer_class} - kWh'] + plc_post22[
                                         f'Non Shopping - OE - {customer_class} - kWh'] + plc_post22[
                                         f'Non Shopping - TE - {customer_class} - kWh']) / 1000,
            'VolumeComment': ''
        }
        daily_volume_list.append(pd.DataFrame(daily_data))

    # Process plc-pipp data
    plc_pipp_prior22 = daily_volume_sheets['PIPP PLC 2019+'].copy()
    plc_pipp_prior22['FlowDate'] = pd.to_datetime(plc_pipp_prior22['Unnamed: 0']).dt.strftime('%Y-%m-%d')
    daily_data = {
        'FlowDate': plc_pipp_prior22['FlowDate'],
        'EDCName': edc_name,
        'CustomerClass': f'PIPP',
        'VolumeType': 'PLC_Scaled',
        'EGS_DailyVolume': 0,
        'Default_DailyVolume': plc_pipp_prior22['PIPP'] / 1000,
        'Eligible_DailyVolume': plc_pipp_prior22['PIPP'] / 1000,
        'VolumeComment': ''
    }
    daily_volume_list.append(pd.DataFrame(daily_data))

    plc_pipp_post22 = daily_volume_sheets['PIPP PLC 11.2022+'].copy()
    plc_pipp_post22['FlowDate'] = pd.to_datetime(plc_pipp_post22['DATE']).dt.strftime('%Y-%m-%d')
    daily_data = {
        'FlowDate': plc_pipp_post22['FlowDate'],
        'EDCName': edc_name,
        'CustomerClass': f'PIPP',
        'VolumeType': 'PLC_Scaled',
        'EGS_DailyVolume': 0,
        'Default_DailyVolume': (plc_pipp_post22['CE'] + plc_pipp_post22['OE'] + plc_pipp_post22['TE']) / 1000,
        'Eligible_DailyVolume': (plc_pipp_post22['CE'] + plc_pipp_post22['OE'] + plc_pipp_post22['TE']) / 1000,
        'VolumeComment': ''
    }
    daily_volume_list.append(pd.DataFrame(daily_data))

    # Process nspl data
    nspl_prior22 = daily_volume_sheets['NSPL 2019+'].copy()
    nspl_prior22['FlowDate'] = pd.to_datetime(nspl_prior22['Unnamed: 0_level_0_Unnamed: 0_level_1']).dt.strftime(
        '%Y-%m-%d')
    for customer_class in ['COM', 'IND', 'RES']:
        daily_data = {
            'FlowDate': nspl_prior22['FlowDate'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class}',
            'VolumeType': 'NSPL_Scaled',
            'EGS_DailyVolume': nspl_prior22[f'Shopped_{customer_class}'] / 1000,
            'Default_DailyVolume': nspl_prior22[f'NonShopped_{customer_class}'] / 1000,
            'Eligible_DailyVolume': (nspl_prior22[f'Shopped_{customer_class}'] + nspl_prior22[
                f'NonShopped_{customer_class}']) / 1000,
            'VolumeComment': ''
        }
        daily_volume_list.append(pd.DataFrame(daily_data))

    nspl_post22 = daily_volume_sheets['NSPL 11.2022+'].copy()
    nspl_post22['FlowDate'] = pd.to_datetime(nspl_post22['Date']).dt.strftime('%Y-%m-%d')
    for customer_class in ['COM', 'IND', 'RES']:
        daily_data = {
            'FlowDate': nspl_post22['FlowDate'],
            'EDCName': edc_name,
            'CustomerClass': f'{customer_class}',
            'VolumeType': 'NSPL_Scaled',
            'EGS_DailyVolume': (nspl_post22[f'Shopping - CE - {customer_class} - kWh'] + nspl_post22[
                f'Shopping - OE - {customer_class} - kWh'] + nspl_post22[
                                    f'Shopping - TE - {customer_class} - kWh']) / 1000,
            'Default_DailyVolume': (nspl_post22[f'Non Shopping - CE - {customer_class} - kWh'] + nspl_post22[
                f'Non Shopping - OE - {customer_class} - kWh'] + nspl_post22[
                                        f'Non Shopping - TE - {customer_class} - kWh']) / 1000,
            'Eligible_DailyVolume': (nspl_post22[f'Shopping - CE - {customer_class} - kWh'] + nspl_post22[
                f'Shopping - OE - {customer_class} - kWh'] + nspl_post22[f'Shopping - TE - {customer_class} - kWh'] +
                                     nspl_post22[f'Non Shopping - CE - {customer_class} - kWh'] + nspl_post22[
                                         f'Non Shopping - OE - {customer_class} - kWh'] + nspl_post22[
                                         f'Non Shopping - TE - {customer_class} - kWh']) / 1000,
            'VolumeComment': ''
        }
        daily_volume_list.append(pd.DataFrame(daily_data))

    # Process nspl-pipp data
    nspl_pipp_prior22 = daily_volume_sheets['PIPP NSPL 2019+'].copy()
    nspl_pipp_prior22['FlowDate'] = pd.to_datetime(nspl_pipp_prior22['Unnamed: 0']).dt.strftime('%Y-%m-%d')
    daily_data = {
        'FlowDate': nspl_pipp_prior22['FlowDate'],
        'EDCName': edc_name,
        'CustomerClass': f'PIPP',
        'VolumeType': 'NSPL_Scaled',
        'EGS_DailyVolume': 0,
        'Default_DailyVolume': nspl_pipp_prior22['PIPP'] / 1000,
        'Eligible_DailyVolume': nspl_pipp_prior22['PIPP'] / 1000,
        'VolumeComment': ''
    }
    daily_volume_list.append(pd.DataFrame(daily_data))

    nspl_pipp_post22 = daily_volume_sheets['PIPP NSPL 11.2022+'].copy()
    nspl_pipp_post22['FlowDate'] = pd.to_datetime(nspl_pipp_post22['DATE']).dt.strftime('%Y-%m-%d')
    daily_data = {
        'FlowDate': nspl_pipp_post22['FlowDate'],
        'EDCName': edc_name,
        'CustomerClass': f'PIPP',
        'VolumeType': 'NSPL_Scaled',
        'EGS_DailyVolume': 0,
        'Default_DailyVolume': (nspl_pipp_post22['CE'] + nspl_pipp_post22['OE'] + nspl_pipp_post22['TE']) / 1000,
        'Eligible_DailyVolume': (nspl_pipp_post22['CE'] + nspl_pipp_post22['OE'] + nspl_pipp_post22['TE']) / 1000,
        'VolumeComment': ''
    }
    daily_volume_list.append(pd.DataFrame(daily_data))
    daily_volume_df_processed = pd.concat(daily_volume_list, ignore_index=True)


    # Handle missing daily volume data on '2023-03-12' and '2024-03-10'
    for volume_type in ['PLC', 'NSPL']:
        for customer_class in ['COM', 'IND', 'RES', 'PIPP']:
            for missing_date in ['3/12/2023', '3/10/2024']:

                missing_date = pd.to_datetime(missing_date)
                pre_date = missing_date - pd.Timedelta(days=1)
                post_date = missing_date + pd.Timedelta(days=1)
                egs_dailyvolume = (daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == pre_date.strftime('%Y-%m-%d'))
                                                                 & (daily_volume_df_processed['CustomerClass'] == customer_class)
                                                                 & (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), 'EGS_DailyVolume'].values[0] +
                                   daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == post_date.strftime('%Y-%m-%d'))
                                                                 & (daily_volume_df_processed['CustomerClass'] == customer_class)
                                                                 & (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), 'EGS_DailyVolume'].values[0])/2

                default_dailyvolume = (daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == pre_date.strftime('%Y-%m-%d'))
                                                                     & (daily_volume_df_processed['CustomerClass'] == customer_class)
                                                                     & (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), 'Default_DailyVolume'].values[0] +
                                       daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == post_date.strftime('%Y-%m-%d'))
                                                                     & (daily_volume_df_processed['CustomerClass'] == customer_class)
                                                                     & (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), 'Default_DailyVolume'].values[0])/2
                eligible_dailyvolume = (daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == pre_date.strftime('%Y-%m-%d'))
                                                                      & (daily_volume_df_processed['CustomerClass'] == customer_class)
                                                                      & (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), 'Eligible_DailyVolume'].values[0] +
                                        daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == post_date.strftime('%Y-%m-%d'))
                                                                      & (daily_volume_df_processed['CustomerClass'] == customer_class)
                                                                      & (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), 'Eligible_DailyVolume'].values[0])/2
                missing_daily_data = {
                    'FlowDate': missing_date.strftime('%Y-%m-%d'),
                    'EDCName': edc_name,
                    'CustomerClass': f'{customer_class}',
                    'VolumeType': f'{volume_type}_Scaled',
                    'EGS_DailyVolume': egs_dailyvolume,
                    'Default_DailyVolume': default_dailyvolume,
                    'Eligible_DailyVolume': eligible_dailyvolume,
                    'VolumeComment': ''
                }
                daily_volume_list.append(pd.DataFrame([missing_daily_data]))

    daily_volume_df_processed = pd.concat(daily_volume_list, ignore_index=True)
    daily_volume_df_processed = daily_volume_df_processed.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'],
                                                                      ignore_index=True)

    # Handle abnormal data on 2023-11-05 for PIPP customer
    for volume_type in ['PLC', 'NSPL']:
        for column_name in ['Default_DailyVolume', 'Eligible_DailyVolume']:
            daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == '2023-11-05') &
                                          (daily_volume_df_processed['CustomerClass'] == 'PIPP') &
                                          (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), column_name] = \
                daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == '2023-11-04') &
                                              (daily_volume_df_processed['CustomerClass'] == 'PIPP') &
                                              (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), column_name].values[0]/2 + \
                daily_volume_df_processed.loc[(daily_volume_df_processed['FlowDate'] == '2023-11-06') &
                                              (daily_volume_df_processed['CustomerClass'] == 'PIPP') &
                                              (daily_volume_df_processed['VolumeType'] == f'{volume_type}_Scaled'), column_name].values[0]/2

    return daily_volume_df_processed


def plot_monthly_data(df, output_dir):
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
        plt.savefig(f'{output_dir}/fe_oh_CustomerCounts_{customer_class}_plot.png')
        plot_path[f'monthly_{customer_class}'] = f'{output_dir}/fe_oh_CustomerCounts_{customer_class}_plot.png'
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
        plt.savefig(f'{output_dir}/fe_oh_HourlyLoad_{customer_class}_plot.png')
        plot_path[f'hourly_{customer_class}'] = f'{output_dir}/fe_oh_HourlyLoad_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_daily_data(df, output_dir):
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
        plt.savefig(f'{output_dir}/fe_oh_PLC_NSPL_{customer_class}_plot.png')
        plot_path[f'daily_{customer_class}'] = f'{output_dir}/fe_oh_PLC_NSPL_{customer_class}_plot.png'
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
        plt.savefig(f'{output_dir}/fe_oh_UFE_{customer_class}_plot.png')
        plot_path[f'UFE_{customer_class}'] = f'{output_dir}/fe_oh_UFE_{customer_class}_plot.png'
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

    elif data_type == 'ufe':

        # Reorder columns
        column_order = ['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType', 'EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume', 'VolumeComment']
        processed_df = processed_df[column_order]
        # Set a copy to avoid warning
        processed_df = processed_df.copy()

        # Convert output Datetime_beginning_utc to string to avoid timezone error
        processed_df['Datetime_beginning_utc'] = processed_df['Datetime_beginning_utc'].astype(str)

        processed_df.to_excel(output_path, index=False, float_format='%.3f')  # Accuracy
        print(f"Data processing complete. The file '{output_path}' has been created.")


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
        'Daily_Volume_RES': plot_path['daily_RES'],
        'Daily_Volume_COM': plot_path['daily_COM'],
        'Daily_Volume_IND': plot_path['daily_IND'],
        'Daily_Volume_PIPP': plot_path['daily_PIPP'],
        'UFE_RES': plot_path['UFE_RES'],
        'UFE_COM': plot_path['UFE_COM'],
        'UFE_IND': plot_path['UFE_IND'],
        'UFE_PIPP': plot_path['UFE_PIPP'],
    }
    return report_plots_path


def generate_keystats(monthly_df, hourly_df, daily_df, ufe_df):
    monthly_columns = ['EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume']
    hourly_columns = ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']
    daily_columns = ['EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume']
    UFE_columns = ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']

    report_keystats_table = {
        'Monthly_Customer_Counts_RES': monthly_df[monthly_df['CustomerClass'] == 'RES'][monthly_columns].describe().T,
        'Monthly_Customer_Counts_COM': monthly_df[monthly_df['CustomerClass'] == 'COM'][monthly_columns].describe().T,
        'Monthly_Customer_Counts_IND': monthly_df[monthly_df['CustomerClass'] == 'IND'][monthly_columns].describe().T,
        'Monthly_Customer_Counts_PIPP': monthly_df[monthly_df['CustomerClass'] == 'PIPP'][monthly_columns].describe().T,

        'Hourly_Volume_RES': hourly_df[hourly_df['CustomerClass'] == 'RES'][hourly_columns].describe().T,
        'Hourly_Volume_COM': hourly_df[hourly_df['CustomerClass'] == 'COM'][hourly_columns].describe().T,
        'Hourly_Volume_IND': hourly_df[hourly_df['CustomerClass'] == 'IND'][hourly_columns].describe().T,
        'Hourly_Volume_PIPP': hourly_df[hourly_df['CustomerClass'] == 'PIPP'][hourly_columns].describe().T,

        'Daily_Volume_RES': daily_df[daily_df['CustomerClass'] == 'RES'][daily_columns].describe().T,
        'Daily_Volume_COM': daily_df[daily_df['CustomerClass'] == 'COM'][daily_columns].describe().T,
        'Daily_Volume_IND': daily_df[daily_df['CustomerClass'] == 'IND'][daily_columns].describe().T,
        'Daily_Volume_PIPP': daily_df[daily_df['CustomerClass'] == 'PIPP'][daily_columns].describe().T,

        'UFE_RES': ufe_df[ufe_df['CustomerClass'] == 'RES'][UFE_columns].describe().T,
        'UFE_COM': ufe_df[ufe_df['CustomerClass'] == 'COM'][UFE_columns].describe().T,
        'UFE_IND': ufe_df[ufe_df['CustomerClass'] == 'IND'][UFE_columns].describe().T,
        'UFE_PIPP': ufe_df[ufe_df['CustomerClass'] == 'PIPP'][UFE_columns].describe().T,
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
        'report_title': 'FE Ohio ETL Report',
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


def main(base_path, data_extract=True, deration_factor_filename='DerationFactor_ATSI.csv'):

    if data_extract:
        # Download data from duke_oh website
        print('Downloading data...')

        # Define the keywords to filter the filenames
        keywords = ['Cap_Trans',# PLC & NSPL
                    'DZSF',# Scaling Factor
                    'OH_Hourly_Load_by_Class',
                    'Shopping%20Stats',# Monthly Customer Counts
                    'UFE']

        # Define the URL of the website
        keyword_dict = {'https://www.firstenergycbp.com/Documents/LoadandOtherData.aspx': keywords}

        file_paths = {}
        for url in keyword_dict:
            soup = fetch_html_content(url)
            excel_links = find_excel_links(soup, url, keyword_dict[url])
            downloaded_path = process_and_download_links(excel_links, keyword_dict[url], base_path)
            file_paths.update(downloaded_path)

        # Update input data file path
        daily_volume_file_path = file_paths['Cap_Trans']
        scaling_factor_file_path = file_paths['DZSF']
        hourly_volume_file_path = file_paths['OH_Hourly_Load_by_Class']
        monthly_customer_count_file_path = file_paths['Shopping%20Stats']
        ufe_file_path = file_paths['UFE']

    else:
        # Define local file path
        daily_volume_file_path = f'{base_path}/Cap_Trans_fe_oh_1.xls'
        scaling_factor_file_path = f'{base_path}/DZSF_fe_oh_1.xls'
        hourly_volume_file_path = f'{base_path}/OH_Hourly_Load_by_Class_fe_oh_1.xlsx'
        monthly_customer_count_file_path = f'{base_path}/Shopping%20Stats_fe_oh_1.xls'
        ufe_file_path = f'{base_path}/UFE_fe_oh_1.xls'


    # Deration File paths
    deration_factor_path = f'{base_path}/{deration_factor_filename}'

    # Load data
    print('Loading data...')
    deration_factor = load_deration_factor(deration_factor_path)

    hourly_volume_sheets = load_hourly_volume_data(hourly_volume_file_path)
    ufe_sheets = load_ufe_data(ufe_file_path)
    monthly_sheets = load_monthly_data(monthly_customer_count_file_path)
    daily_volume_sheets = load_daily_data(daily_volume_file_path)

    # Process data
    print('Processing data...')
    edc_name = "OH_FE"
    deration_factor_processed = process_deration_factor(deration_factor, edc_name)
    ufe_df_processed = process_ufe_data(ufe_sheets)
    monthly_customer_count_processed = process_monthly_customer_count(monthly_sheets, edc_name)
    daily_volume_processed = process_daily_volume(daily_volume_sheets, edc_name)
    hourly_volume_processed, ufe_processed = process_hourly_volume(hourly_volume_sheets, ufe_df_processed, deration_factor_processed, edc_name)

    # Cutoff monthly data by 2010-01-01
    monthly_customer_count_processed = monthly_customer_count_processed[monthly_customer_count_processed['FlowMonth'] >= '2010-01-01']

    # Fix abnormal data for PIPP monthly customer counts in Feb-2021
    mid_customer_count = (monthly_customer_count_processed.loc[(monthly_customer_count_processed['FlowMonth'] == '2021-01-01')
                                                               & (monthly_customer_count_processed['CustomerClass'] == 'PIPP'),
                                                               'Eligible_MonthlyVolume'].values[0]/2 +
                          monthly_customer_count_processed.loc[(monthly_customer_count_processed['FlowMonth'] == '2021-03-01')
                                                               & (monthly_customer_count_processed['CustomerClass'] == 'PIPP'),
                                                               'Eligible_MonthlyVolume'].values[0]/2).round(0)
    for volume_type in ['Default_MonthlyVolume', 'Eligible_MonthlyVolume']:
        monthly_customer_count_processed.loc[(monthly_customer_count_processed['FlowMonth'] == '2021-02-01')
                                             & (monthly_customer_count_processed['CustomerClass'] == 'PIPP'),
                                             volume_type] = mid_customer_count

    # Output path
    output_path = f'{base_path}/output_data'
    # Create output directory if it does not exist
    os.makedirs(output_path, exist_ok=True)

    hourly_output_path = f'{output_path}/fe_oh_HourlyVolume_processed.xlsx'
    monthly_output_path = f'{output_path}/fe_oh_CustomerCount_processed.xlsx'
    daily_output_path = f'{output_path}/fe_oh_NSPL_PLC_processed.xlsx'
    ufe_output_path = f'{output_path}/fe_oh_customer_UFE_processed.xlsx'

    etl_report_output_path = f'{output_path}/ETL_report'

    # Plot data for correction
    print('Saving plot...')
    plot_path = {}
    plot_path.update(plot_monthly_data(monthly_customer_count_processed, etl_report_output_path))
    plot_path.update(plot_hourly_data(hourly_volume_processed, etl_report_output_path))
    plot_path.update(plot_daily_data(daily_volume_processed, etl_report_output_path))
    plot_path.update(plot_UFE_data(ufe_processed, etl_report_output_path))

    # # Generate ETL report
    report_plots_path = save_plot_path(plot_path)
    report_keystats_table = generate_keystats(monthly_customer_count_processed, hourly_volume_processed, daily_volume_processed, ufe_processed)
    generate_report(etl_report_output_path, report_keystats_table, report_plots_path)

    # Check Continuity
    print('Checking continuity...')
    for customer_class in ['RES', 'COM', 'IND', 'PIPP']:
        check_continuity(hourly_volume_processed[hourly_volume_processed['CustomerClass'] == customer_class],
                         'Datetime_beginning_utc', 'H', f'{customer_class} hourly volume')
        check_continuity(ufe_processed[ufe_processed['CustomerClass'] == customer_class],
                         'Datetime_beginning_utc', 'H', f'{customer_class} UFE volume')
        check_continuity(monthly_customer_count_processed[monthly_customer_count_processed['CustomerClass'] == customer_class],
                         'FlowMonth', 'M', f'{customer_class} monthly volume')
        check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == customer_class) & (
                daily_volume_processed['VolumeType'] == 'NSPL_Scaled')], 'FlowDate', 'D', f'{customer_class} NSPL daily volume')
        check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == customer_class) & (
                daily_volume_processed['VolumeType'] == 'PLC_Scaled')], 'FlowDate', 'D', f'{customer_class} PLC daily volume')

    # Change Volume type of PLC and NSPL from scaled to unscaled
    daily_volume_processed.loc[daily_volume_processed['VolumeType'] == 'NSPL_Scaled', 'VolumeType'] = 'NSPL_unscaled'
    daily_volume_processed.loc[daily_volume_processed['VolumeType'] == 'PLC_Scaled', 'VolumeType'] = 'PLC_unscaled'

    # Save processed data
    print('Saving data...')
    save_processed_data(hourly_volume_processed, hourly_output_path, 'hourly')
    save_processed_data(monthly_customer_count_processed, monthly_output_path, 'monthly')
    save_processed_data(daily_volume_processed, daily_output_path, 'daily')
    save_processed_data(ufe_processed, ufe_output_path, 'ufe')

if __name__ == "__main__":
    base_path = 'C:\\Users\\5DIntern3_2024\\Work\\FE_Ohio'
    data_extract = False
    deration_factor_filename = 'DerationFactor_ATSI.csv'
    main(base_path, data_extract, deration_factor_filename)

