
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
            new_file_name = f"{keyword_in_file}_aes_oh_{keyword_counts[keyword_in_file]}.xls" if file_name.endswith(
                '.xls') else f"{keyword_in_file}_aes_oh_{keyword_counts[keyword_in_file]}.xlsx"
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
    volume_sheets = ['Total', 'NonShop', 'Shop', 'PIPP']
    customercount_sheets = ['Count_Total', 'Count_NonShop', 'Count_Shop', 'Count_PIPP']
    PLC_NSPL_sheets = ['Capcity PLC & DZSF', 'Transmssn NSPL & DZSF']

    file_sheets = pd.read_excel(file_path, sheet_name=None)
    for sheet_name in file_sheets:
        if sheet_name in volume_sheets:
            file_sheets[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name, header=9)
        if sheet_name in customercount_sheets:
            file_sheets[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name, header=9)
        if sheet_name in PLC_NSPL_sheets:
            file_sheets[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name, header=3)
    return file_sheets


def load_pipp_data(pipp_file_path):

    file_sheets = pd.read_excel(pipp_file_path, sheet_name=None)

    for sheet_name in file_sheets:
        if sheet_name == 'PIPP_RS':
            file_sheets[sheet_name] = pd.read_excel(pipp_file_path, sheet_name=sheet_name, header=8)
        elif sheet_name == 'Monthly':
            file_sheets[sheet_name] = pd.read_excel(pipp_file_path, sheet_name=sheet_name, header=1)

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


def process_load_data(sheets, edc_name, deration_factor):
    hourly_volume_list = []
    monthly_customer_count_list = []
    daily_volume_list = []
    ufe_list = []
    res_ufe_factor = []

    for sheet_name, sheet_df in sheets.items():
        # Process daily volume data
        if sheet_name in ('NonShop', 'Shop', 'PIPP'):
            # Convert datetime to hour ending utc
            sheet_df = sheet_df.copy()
            sheet_df = sheet_df.dropna(subset=['Total'])
            sheet_df['Hr Ending'] = sheet_df['Hr Ending'].astype(int)
            sheet_df['Hr Ending'] -= 1
            datetime_beginning_est = pd.to_datetime(sheet_df[['Year', 'Month', 'Day ', 'Hr Ending']].astype(str).agg('-'.join, axis=1), format='%Y-%m-%d-%H')
            sheet_df["Datetime_beginning_utc"] = datetime_beginning_est.dt.tz_localize('EST').dt.tz_convert('UTC')

            merged_df = sheet_df.merge(deration_factor, left_on='Datetime_beginning_utc', right_on='Datetime_beginning_utc')
            if sheet_name != 'PIPP':
                for customer_class in ['Commercial', 'Industrial', 'Residential']:
                    hourly_volume_data = {
                        'Datetime_beginning_utc': merged_df["Datetime_beginning_utc"],
                        'EDCName': edc_name,
                        'CustomerClass': f'{customer_class[:3].upper()}',
                        'VolumeType': 'Wholesale_Derated',
                        'EGS_HourlyVolume': merged_df[customer_class] / 1000 * merged_df[f'{customer_class}.1'] * (1 - merged_df['DerationFactor']) if sheet_name == 'Shop' else 0,
                        'Default_HourlyVolume': merged_df[customer_class] / 1000 * merged_df[f'{customer_class}.1'] * (1 - merged_df['DerationFactor']) if sheet_name == 'NonShop' else 0,
                        'Eligible_HourlyVolume': merged_df[customer_class] / 1000 * merged_df[f'{customer_class}.1'] * (1 - merged_df['DerationFactor']),
                        'VolumeComment': ''
                    }
                    hourly_volume_list.append(pd.DataFrame(hourly_volume_data))

                    ufe_data = {
                        'Datetime_beginning_utc': merged_df["Datetime_beginning_utc"],
                        'EDCName': edc_name,
                        'CustomerClass': f'{customer_class[:3].upper()}',
                        'VolumeType': 'UFE',
                        'EGS_HourlyVolume': merged_df[customer_class] / 1000 * (merged_df[f'{customer_class}.1'] - 1) * (1 - merged_df['DerationFactor']) if sheet_name == 'Shop' else 0,
                        'Default_HourlyVolume': merged_df[customer_class] / 1000 * (merged_df[f'{customer_class}.1'] - 1) * (1 - merged_df['DerationFactor']) if sheet_name == 'NonShop' else 0,
                        'Eligible_HourlyVolume': merged_df[customer_class] / 1000 * (merged_df[f'{customer_class}.1'] - 1) * (1 - merged_df['DerationFactor']),
                        'VolumeComment': ''
                    }
                    ufe_list.append(pd.DataFrame(ufe_data))

                    # Save residual ufe factors for pipp processing
                    if (customer_class == 'Residential' and sheet_name == 'Shop'):
                        res_ufe_factor_data = {
                            'Datetime_beginning_utc': merged_df["Datetime_beginning_utc"],
                            'CustomerClass': 'RES',
                            'VolumeType': 'UFE_factor',
                            'ufe_factor':  merged_df[f'{customer_class}.1']

                        }
                        res_ufe_factor.append(pd.DataFrame(res_ufe_factor_data))

            else:
                # Only include one customer class: RES for PIPP data
                merged_df = merged_df[merged_df['Datetime_beginning_utc'] >= '2017-06-01 04:00:00+00:00']
                hourly_volume_data = {
                    'Datetime_beginning_utc': merged_df["Datetime_beginning_utc"],
                    'EDCName': edc_name,
                    'CustomerClass': 'PIPP',
                    'VolumeType': 'Wholesale_Derated',
                    'EGS_HourlyVolume': 0,
                    'Default_HourlyVolume': merged_df['Total'] / 1000 * merged_df['Total.1'] * (1 - merged_df['DerationFactor']),
                    'Eligible_HourlyVolume': merged_df['Total'] / 1000 * merged_df['Total.1'] * (1 - merged_df['DerationFactor']),
                    'VolumeComment': ''
                }
                hourly_volume_list.append(pd.DataFrame(hourly_volume_data))

                ufe_data = {
                    'Datetime_beginning_utc': merged_df["Datetime_beginning_utc"],
                    'EDCName': edc_name,
                    'CustomerClass': 'PIPP',
                    'VolumeType': 'UFE',
                    'EGS_HourlyVolume': 0,
                    'Default_HourlyVolume': merged_df['Total'] / 1000 * (merged_df['Total.1'] - 1) * (1 - merged_df['DerationFactor']),
                    'Eligible_HourlyVolume': merged_df['Total'] / 1000 * (merged_df['Total.1'] - 1) * (1 - merged_df['DerationFactor']),
                    'VolumeComment': ''
                }
                ufe_list.append(pd.DataFrame(ufe_data))

        # Process monthly customer count data
        if sheet_name in ('Count_NonShop', 'Count_Shop', 'Count_PIPP'):
            # Take average of daily data to get monthly customer counts
            sheet_df = sheet_df.copy()
            sheet_df = sheet_df.dropna(subset=['Total'])
            sheet_df['FlowMonth'] = pd.to_datetime(sheet_df[['Year', 'Month', 'Day ']].astype(str).agg('-'.join, axis=1), format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
            sheet_df = sheet_df.drop(columns=['Year', 'Month', 'Day '])
            sheet_df = sheet_df.groupby(['FlowMonth'], as_index=False).mean()
            if sheet_name != 'Count_PIPP':
                for customer_class in ['Commercial', 'Industrial', 'Residential']:
                    monthly_customer_count_data = {
                        'FlowMonth': sheet_df['FlowMonth'],
                        'EDCName': edc_name,
                        'CustomerClass': f'{customer_class[:3].upper()}',
                        'VolumeType': 'CustomerCount',
                        'EGS_MonthlyVolume': sheet_df[customer_class].round(0) if sheet_name == 'Count_Shop' else 0,
                        'Default_MonthlyVolume': sheet_df[customer_class].round(0) if sheet_name == 'Count_NonShop' else 0,
                        'Eligible_MonthlyVolume': sheet_df[customer_class].round(0),
                        'VolumeComment': ''
                    }
                    monthly_customer_count_list.append(pd.DataFrame(monthly_customer_count_data))
            else:
                sheet_df = sheet_df[sheet_df['FlowMonth'] >= '2017-06-01']
                monthly_customer_count_data = {
                    'FlowMonth': sheet_df['FlowMonth'],
                    'EDCName': edc_name,
                    'CustomerClass': 'PIPP',
                    'VolumeType': 'CustomerCount',
                    'EGS_MonthlyVolume': 0,
                    'Default_MonthlyVolume': sheet_df['Total'].round(0),
                    'Eligible_MonthlyVolume': sheet_df['Total'].round(0),
                    'VolumeComment': ''
                }
                monthly_customer_count_list.append(pd.DataFrame(monthly_customer_count_data))


        # Process PLC & NSPL data
        if sheet_name in ('Capcity PLC & DZSF', 'Transmssn NSPL & DZSF'):
            # Take average of daily data to get monthly customer counts
            sheet_df = sheet_df.copy()
            sheet_df = sheet_df.dropna(subset=['Total'])
            sheet_df['DATE'] = pd.to_datetime(sheet_df['DATE']).dt.strftime('%Y-%m-%d')

            # Drop columns where the first row is NA to select scaling factor with positional arguments
            sheet_df = sheet_df.dropna(axis=1, subset=[0])

            # Define volume type
            volume_type = 'PLC' if 'PLC' in sheet_name else 'NSPL'

            for customer_class in ['COM', 'IND', 'RES']:
                daily_data = {
                    'FlowDate': sheet_df['DATE'],
                    'EDCName': edc_name,
                    'CustomerClass': f'{customer_class}',
                    'VolumeType': f'{volume_type}_Scaled',
                    'EGS_DailyVolume': sheet_df[f'{customer_class}.1'] * sheet_df.iloc[:, -1] / 1000,
                    'Default_DailyVolume': sheet_df[customer_class] * sheet_df.iloc[:, -1] / 1000,
                    'Eligible_DailyVolume': (sheet_df[f'{customer_class}.1'] + sheet_df[customer_class]) * sheet_df.iloc[:, -1] / 1000,
                    'VolumeComment': ''
                }
                daily_volume_list.append(pd.DataFrame(daily_data))

            # Identify whether PIPP data included
            if 'RES.2' in sheet_df.columns:
                pipp_data = {
                    'FlowDate': sheet_df['DATE'],
                    'EDCName': edc_name,
                    'CustomerClass': 'PIPP',
                    'VolumeType': f'{volume_type}_Scaled',
                    'EGS_DailyVolume': 0,
                    'Default_DailyVolume': sheet_df['RES.2'] * sheet_df.iloc[:, -1] / 1000,
                    'Eligible_DailyVolume': sheet_df['RES.2'] * sheet_df.iloc[:, -1] / 1000,
                    'VolumeComment': ''
                }
                pipp_df = pd.DataFrame(pipp_data)
                pipp_df = pipp_df[pipp_df['FlowDate'] >= '2017-06-01']

                daily_volume_list.append(pipp_df)

    # Combine dataframe and groupby target columns
    hourly_volume_df = pd.concat(hourly_volume_list, ignore_index=True)
    hourly_volume_df = hourly_volume_df.groupby(['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType', 'VolumeComment'], as_index=False).sum()

    monthly_customer_count_df = pd.concat(monthly_customer_count_list, ignore_index=True)
    monthly_customer_count_df = monthly_customer_count_df.groupby(['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType', 'VolumeComment'], as_index=False).sum()

    daily_volume_df = pd.concat(daily_volume_list, ignore_index=True)
    daily_volume_df = daily_volume_df.groupby(['FlowDate', 'EDCName', 'CustomerClass', 'VolumeType', 'VolumeComment'], as_index=False).sum()

    ufe_df = pd.concat(ufe_list, ignore_index=True)
    ufe_df = ufe_df.groupby(['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType', 'VolumeComment'], as_index=False).sum()

    res_ufe_factor_df = pd.concat(res_ufe_factor, ignore_index=True)

    return hourly_volume_df, monthly_customer_count_df, daily_volume_df, ufe_df, res_ufe_factor_df


def combine_processed_data(data_list, data_type):
    if data_type == 'hourly':
        processed_df = pd.concat(data_list, ignore_index=True)
        processed_df = processed_df.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'], ignore_index=True)

    if data_type == 'monthly':
        processed_df = pd.concat(data_list, ignore_index=True)
        processed_df = processed_df.sort_values(by=['FlowMonth', 'CustomerClass'], ignore_index=True)

    if data_type == 'daily':
        processed_df = pd.concat(data_list, ignore_index=True)
        processed_df = processed_df.sort_values(by=['FlowDate', 'CustomerClass'], ignore_index=True)

    return processed_df


def process_pipp_data(pipp_sheets, deration_factor, ufe_factor_processed, edc_name):
    # Process Hourly Volume
    pipp_volume_df = pipp_sheets['PIPP_RS']
    pipp_volume_df['Year'] = pipp_volume_df['Year'].astype(int)
    pipp_volume_df['Month'] = pipp_volume_df['Month'].astype(int)
    pipp_volume_df['Day'] = pipp_volume_df['Day '].astype(int)
    pipp_volume_df['Hr Ending'] = pipp_volume_df['Hr Ending'].astype(int) - 1
    datetime_beginning_est = pd.to_datetime(pipp_volume_df[['Year', 'Month', 'Day', 'Hr Ending']].astype(str).agg('-'.join, axis=1), format='%Y-%m-%d-%H')
    pipp_volume_df['Datetime_beginning_utc'] = datetime_beginning_est.dt.tz_localize('EST').dt.tz_convert('UTC')

    merged_volume_df = pipp_volume_df.merge(deration_factor, left_on='Datetime_beginning_utc', right_on='Datetime_beginning_utc')
    merged_volume_df = merged_volume_df.merge(ufe_factor_processed, left_on='Datetime_beginning_utc', right_on='Datetime_beginning_utc')
    merged_volume_df = merged_volume_df.copy()
    merged_volume_df['PIPP Total'] = merged_volume_df['PIPP Total'].astype(float)

    pipp_hourly_list = []
    pipp_hourly_data = {
        'Datetime_beginning_utc': merged_volume_df['Datetime_beginning_utc'],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'Wholesale_Derated',
        'EGS_HourlyVolume': 0,
        'Default_HourlyVolume': merged_volume_df['PIPP Total'] / 1000 * (1 - merged_volume_df['DerationFactor']) * merged_volume_df['ufe_factor'],
        'Eligible_HourlyVolume': merged_volume_df['PIPP Total'] / 1000 * (1 - merged_volume_df['DerationFactor']) * merged_volume_df['ufe_factor'],
        'VolumeComment': ''
    }
    pipp_hourly_list.append(pd.DataFrame(pipp_hourly_data))
    pipp_hourly_df = pd.concat(pipp_hourly_list, ignore_index=True)

    # Save ufe data
    pipp_ufe_list = []
    pipp_ufe_data = {
        'Datetime_beginning_utc': merged_volume_df['Datetime_beginning_utc'],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'UFE',
        'EGS_HourlyVolume': 0,
        'Default_HourlyVolume': merged_volume_df['PIPP Total'] / 1000 * (1 - merged_volume_df['DerationFactor']) * (merged_volume_df['ufe_factor'] - 1),
        'Eligible_HourlyVolume': merged_volume_df['PIPP Total'] / 1000 * (1 - merged_volume_df['DerationFactor']) * (merged_volume_df['ufe_factor'] - 1),
        'VolumeComment': ''
    }
    pipp_ufe_list.append(pd.DataFrame(pipp_ufe_data))
    pipp_ufe_df = pd.concat(pipp_ufe_list, ignore_index=True)

    # Process Customer Counts
    pipp_customer_count_df = pipp_sheets['Monthly']
    pipp_customer_count_df = pipp_customer_count_df.dropna(subset=pipp_customer_count_df.columns[0])
    pipp_customer_count_df = pipp_customer_count_df.copy()
    indices_to_drop = []
    for index, row in pipp_customer_count_df.iterrows():
        # Check if the first column contains a year
        if not isinstance(row.iloc[0], str):
            year = row.iloc[0]
            indices_to_drop.append(index)
        else:
            pipp_customer_count_df.loc[index, 'FlowMonth'] = pd.to_datetime(f'{year} {row.iloc[0]}', format='%Y %b').strftime('%Y-%m-01')
    pipp_customer_count_df = pipp_customer_count_df.drop(indices_to_drop)
    pipp_customer_count_df = pipp_customer_count_df.dropna(subset=['PIPP Customers'])
    pipp_customer_count_df = pipp_customer_count_df[pipp_customer_count_df['FlowMonth'] < '2017-06-01']
    pipp_monthly_list = []
    pipp_monthly_data = {
        'FlowMonth': pipp_customer_count_df['FlowMonth'],
        'EDCName': edc_name,
        'CustomerClass': 'PIPP',
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': 0,
        'Default_MonthlyVolume': pipp_customer_count_df['PIPP Customers'].astype(int),
        'Eligible_MonthlyVolume': pipp_customer_count_df['PIPP Customers'].astype(int),
        'VolumeComment': ''
    }
    pipp_monthly_list.append(pd.DataFrame(pipp_monthly_data))
    pipp_monthly_df = pd.concat(pipp_monthly_list, ignore_index=True)
    pipp_monthly_df = pipp_monthly_df.copy()
    pipp_monthly_df['FlowMonth'] = pd.to_datetime(pipp_monthly_df['FlowMonth'], format='%Y-%m-%d')
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

    pipp_monthly_df = expand_to_daily(pipp_monthly_df)

    return pipp_monthly_df, pipp_hourly_df, pipp_ufe_df


def subtract_pipp_data(hourly_volume_processed, monthly_customer_count_processed, ufe_processed):
    # Process Hourly data
    hourly_df = hourly_volume_processed[hourly_volume_processed['Datetime_beginning_utc'] <= '2017-06-01 03:00:00+00:00']
    hourly_df_list = [hourly_volume_processed[hourly_volume_processed['Datetime_beginning_utc'] > '2017-06-01 03:00:00+00:00']]

    hourly_df_res = hourly_df[hourly_df['CustomerClass'] == 'RES']
    hourly_df_pipp = hourly_df[hourly_df['CustomerClass'] == 'PIPP']
    hourly_df_non_res = hourly_df[hourly_df['CustomerClass'] != 'RES']
    hourly_df_list.append(hourly_df_non_res)

    hourly_df_merged = hourly_df_res.merge(hourly_df_pipp, left_on='Datetime_beginning_utc', right_on='Datetime_beginning_utc', suffixes=('', '_pipp'))

    hourly_df_merged['EGS_HourlyVolume'] -= hourly_df_merged['EGS_HourlyVolume_pipp']
    hourly_df_merged['Default_HourlyVolume'] -= hourly_df_merged['Default_HourlyVolume_pipp']
    hourly_df_merged['Eligible_HourlyVolume'] -= hourly_df_merged['Eligible_HourlyVolume_pipp']

    hourly_df_res_cleaned = hourly_df_merged[['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType',
                                              'EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume',
                                              'VolumeComment']]
    hourly_df_list.append(hourly_df_res_cleaned)
    hourly_volume_cleaned = pd.concat(hourly_df_list, ignore_index=True)
    hourly_volume_cleaned = hourly_volume_cleaned.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'], ignore_index=True)

    # Process monthly data
    monthly_df = monthly_customer_count_processed[monthly_customer_count_processed['FlowMonth'] < '2017-06-01']
    monthly_df_list = [monthly_customer_count_processed[monthly_customer_count_processed['FlowMonth'] >= '2017-06-01']]

    monthly_df_res = monthly_df[monthly_df['CustomerClass'] == 'RES']
    monthly_df_pipp = monthly_df[monthly_df['CustomerClass'] == 'PIPP']
    monthly_df_non_res = monthly_df[monthly_df['CustomerClass'] != 'RES']
    monthly_df_list.append(monthly_df_non_res)

    monthly_df_merged = monthly_df_res.merge(monthly_df_pipp, left_on='FlowMonth', right_on='FlowMonth', suffixes=('', '_pipp'))

    monthly_df_merged['EGS_MonthlyVolume'] -= monthly_df_merged['EGS_MonthlyVolume_pipp']
    monthly_df_merged['Default_MonthlyVolume'] -= monthly_df_merged['Default_MonthlyVolume_pipp']
    monthly_df_merged['Eligible_MonthlyVolume'] -= monthly_df_merged['Eligible_MonthlyVolume_pipp']

    monthly_df_res_cleaned = monthly_df_merged[['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType',
                                                'EGS_MonthlyVolume', 'Default_MonthlyVolume',
                                                'Eligible_MonthlyVolume', 'VolumeComment']]
    monthly_df_list.append(monthly_df_res_cleaned)
    monthly_volume_cleaned = pd.concat(monthly_df_list, ignore_index=True)
    monthly_volume_cleaned = monthly_volume_cleaned.sort_values(by=['FlowMonth', 'CustomerClass'], ignore_index=True)
    # Cut off monthly customer count prior to 2013 for consistency
    monthly_volume_cleaned = monthly_volume_cleaned[monthly_volume_cleaned['FlowMonth'] >= '2013-01-01']

    # Process ufe data
    ufe_df = ufe_processed[ufe_processed['Datetime_beginning_utc'] <= '2017-06-01 03:00:00+00:00']
    ufe_df_list = [ufe_processed[ufe_processed['Datetime_beginning_utc'] > '2017-06-01 03:00:00+00:00']]

    ufe_df_res = ufe_df[ufe_df['CustomerClass'] == 'RES']
    ufe_df_pipp = ufe_df[ufe_df['CustomerClass'] == 'PIPP']
    ufe_df_non_res = ufe_df[ufe_df['CustomerClass'] != 'RES']
    ufe_df_list.append(ufe_df_non_res)

    ufe_df_merged = ufe_df_res.merge(ufe_df_pipp, left_on='Datetime_beginning_utc', right_on='Datetime_beginning_utc', suffixes=('', '_pipp'))

    ufe_df_merged['EGS_HourlyVolume'] -= ufe_df_merged['EGS_HourlyVolume_pipp']
    ufe_df_merged['Default_HourlyVolume'] -= ufe_df_merged['Default_HourlyVolume_pipp']
    ufe_df_merged['Eligible_HourlyVolume'] -= ufe_df_merged['Eligible_HourlyVolume_pipp']

    ufe_df_res_cleaned = ufe_df_merged[['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType',
                                        'EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume',
                                        'VolumeComment']]
    ufe_df_list.append(ufe_df_res_cleaned)
    ufe_volume_cleaned = pd.concat(ufe_df_list, ignore_index=True)
    ufe_volume_cleaned = ufe_volume_cleaned.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'], ignore_index=True)

    return hourly_volume_cleaned, monthly_volume_cleaned, ufe_volume_cleaned


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
        plt.savefig(f'{output_dir}/aes_oh_CustomerCounts_{customer_class}_plot.png')
        plot_path[f'monthly_{customer_class}'] = f'{output_dir}/aes_oh_CustomerCounts_{customer_class}_plot.png'
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
        plt.savefig(f'{output_dir}/aes_oh_HourlyLoad_{customer_class}_plot.png')
        plot_path[f'hourly_{customer_class}'] = f'{output_dir}/aes_oh_HourlyLoad_{customer_class}_plot.png'
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
        plt.savefig(f'{output_dir}/aes_oh_PLC_NSPL_{customer_class}_plot.png')
        plot_path[f'daily_{customer_class}'] = f'{output_dir}/aes_oh_PLC_NSPL_{customer_class}_plot.png'
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
        plt.savefig(f'{output_dir}/aes_oh_UFE_{customer_class}_plot.png')
        plot_path[f'UFE_{customer_class}'] = f'{output_dir}/aes_oh_UFE_{customer_class}_plot.png'
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
        'report_title': 'AES Ohio ETL Report',
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


def correct_abnormal_data(hourly_volume_processed):
    raw_df = hourly_volume_processed.copy()
    customer_class_list = raw_df['CustomerClass'].unique()
    hourly_volume_cleaned_list = []
    for customer_class in customer_class_list:
        hourly_volume_processed = raw_df[raw_df['CustomerClass'] == customer_class]
        # Deal with first half of missing data
        start_abnormal = '2024-02-28 05:00:00'
        end_abnormal = '2024-02-29 04:00:00'

        def calculate_average_first_half(row, df, column_name):
            time = row['Datetime_beginning_utc']
            val_48h_ago = df.loc[df['Datetime_beginning_utc'] == (time - pd.Timedelta(hours=48)), column_name]
            val_24h_ago = df.loc[df['Datetime_beginning_utc'] == (time - pd.Timedelta(hours=24)), column_name]
            val_48h_after = df.loc[df['Datetime_beginning_utc'] == (time + pd.Timedelta(hours=48)), column_name]

            # Ensure we have values for all required times
            if not (val_48h_ago.empty or val_24h_ago.empty or val_48h_after.empty):
                return (val_48h_ago.values[0] + val_24h_ago.values[0] + val_48h_after.values[0]) / 3
            else:
                return row[column_name]

        # Apply the function to the abnormal period
        for column_name in ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']:
            hourly_volume_processed.loc[(hourly_volume_processed['Datetime_beginning_utc'] >= start_abnormal) & (
                        hourly_volume_processed['Datetime_beginning_utc'] <= end_abnormal), column_name] = \
            hourly_volume_processed.loc[(hourly_volume_processed['Datetime_beginning_utc'] >= start_abnormal) & (
                        hourly_volume_processed['Datetime_beginning_utc'] <= end_abnormal)].apply(
                calculate_average_first_half, df=hourly_volume_processed, column_name=column_name, axis=1)

        # Deal with second half of missing data
        start_abnormal = '2024-02-29 05:00:00'
        end_abnormal = '2024-03-01 04:00:00'

        def calculate_average_second_half(row, df, column_name):
            time = row['Datetime_beginning_utc']
            val_48h_ago = df.loc[df['Datetime_beginning_utc'] == (time - pd.Timedelta(hours=48)), column_name]
            val_24h_after = df.loc[df['Datetime_beginning_utc'] == (time + pd.Timedelta(hours=24)), column_name]
            val_48h_after = df.loc[df['Datetime_beginning_utc'] == (time + pd.Timedelta(hours=48)), column_name]

            # Ensure we have values for all required times
            if not (val_48h_ago.empty or val_48h_after.empty or val_24h_after.empty):
                return (val_48h_ago.values[0] + val_48h_after.values[0] + val_24h_after.values[0]) / 3
            else:
                return row[column_name]

        # Apply the function to the abnormal period
        for column_name in ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']:
            hourly_volume_processed.loc[(hourly_volume_processed['Datetime_beginning_utc'] >= start_abnormal) & (
                        hourly_volume_processed['Datetime_beginning_utc'] <= end_abnormal), column_name] = \
            hourly_volume_processed.loc[(hourly_volume_processed['Datetime_beginning_utc'] >= start_abnormal) & (
                        hourly_volume_processed['Datetime_beginning_utc'] <= end_abnormal)].apply(
                calculate_average_second_half, df=hourly_volume_processed, column_name=column_name, axis=1)
        hourly_volume_cleaned_list.append(hourly_volume_processed)

    # Combine dataframe and sort values
    hourly_volume_processed = pd.concat(hourly_volume_cleaned_list, ignore_index=True)

    # Deal with the last 4 abnormal records for PIPP
    start_abnormal = '2024-03-21 01:00:00'
    end_abnormal = '2024-03-21 04:00:00'
    column_to_fix = ['EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume']

    def fix_abnormal_pipp(row, df, column_name):
        time = row['Datetime_beginning_utc']
        val_24h_ago = df.loc[(df['Datetime_beginning_utc'] == (time - pd.Timedelta(hours=24))) & (df['CustomerClass'] == 'PIPP'), column_name]
        val_48h_ago = df.loc[(df['Datetime_beginning_utc'] == (time - pd.Timedelta(hours=48))) & (df['CustomerClass'] == 'PIPP'), column_name]
        val_72h_ago = df.loc[(df['Datetime_beginning_utc'] == (time - pd.Timedelta(hours=72))) & (df['CustomerClass'] == 'PIPP'), column_name]
        return (val_24h_ago.values[0] + val_48h_ago.values[0] + val_72h_ago.values[0]) / 3

    for column_name in column_to_fix:
        hourly_volume_processed.loc[(hourly_volume_processed['Datetime_beginning_utc'] >= start_abnormal) &
                                    (hourly_volume_processed['Datetime_beginning_utc'] <= end_abnormal) &
                                    (hourly_volume_processed['CustomerClass'] == 'PIPP'), column_name] = \
            hourly_volume_processed.loc[(hourly_volume_processed['Datetime_beginning_utc'] >= start_abnormal) &
                                        (hourly_volume_processed['Datetime_beginning_utc'] <= end_abnormal) &
                                        (hourly_volume_processed['CustomerClass'] == 'PIPP')].apply(
                fix_abnormal_pipp, df=hourly_volume_processed, column_name=column_name, axis=1)

    hourly_volume_processed = hourly_volume_processed.sort_values(by=['Datetime_beginning_utc', 'CustomerClass'],
                                                                  ignore_index=True)
    return hourly_volume_processed


def main(base_path, data_extract=True, deration_factor_filename='DerationFactor_DAY.csv'):

    if data_extract:
        # Download data from duke_oh website
        print('Downloading data...')

        # Define the keywords to filter the filenames
        keywords = ['2011_AES_Load_Data',
                    '2012_AES_Load_Data',
                    '2013_AES_Load_Data',
                    '2014_AES_Load_Data',
                    '2015_AES_Load_Data',
                    '2016_AES_Load_Data',
                    '2017_AES_Load_Data',
                    '2018_AES_Load_Data',
                    '2019_AES_Load_Data',
                    '2020_AES_Load_Data',
                    '2021_DPL_Load_Data',
                    '2022_DPL_Load_Data',
                    '2023_DPL_Load_Data',
                    '2024_DPL_Load_Data',
                    '2013_2017_PIPP']

        # Define the URL of the website
        keyword_dict = {'https://www.aes-ohioauction.com/LoadData.aspx': keywords}

        file_paths = {}
        for url in keyword_dict:
            soup = fetch_html_content(url)
            excel_links = find_excel_links(soup, url, keyword_dict[url])
            downloaded_path = process_and_download_links(excel_links, keyword_dict[url], base_path)
            file_paths.update(downloaded_path)

        # Update input data file path
        hourly_load_file_path = {
            '2011': file_paths['2011_AES_Load_Data'],
            '2012': file_paths['2012_AES_Load_Data'],
            '2013': file_paths['2013_AES_Load_Data'],
            '2014': file_paths['2014_AES_Load_Data'],
            '2015': file_paths['2015_AES_Load_Data'],
            '2016': file_paths['2016_AES_Load_Data'],
            '2017': file_paths['2017_AES_Load_Data'],
            '2018': file_paths['2018_AES_Load_Data'],
            '2019': file_paths['2019_AES_Load_Data'],
            '2020': file_paths['2020_AES_Load_Data'],
            '2021': file_paths['2021_DPL_Load_Data'],
            '2022': file_paths['2022_DPL_Load_Data'],
            '2023': file_paths['2023_DPL_Load_Data'],
            '2024': file_paths['2024_DPL_Load_Data']
        }
        PIPP_file_path = file_paths['2013_2017_PIPP']

    else:
        # Define local file path
        hourly_load_file_path = {
            '2011': f'{base_path}/2011_AES_Load_Data_aes_oh_1.xls',
            '2012': f'{base_path}/2012_AES_Load_Data_aes_oh_1.xls',
            '2013': f'{base_path}/2013_AES_Load_Data_aes_oh_1.xls',
            '2014': f'{base_path}/2014_AES_Load_Data_aes_oh_1.xls',
            '2015': f'{base_path}/2015_AES_Load_Data_aes_oh_1.xls',
            '2016': f'{base_path}/2016_AES_Load_Data_aes_oh_1.xls',
            '2017': f'{base_path}/2017_AES_Load_Data_aes_oh_1.xlsx',
            '2018': f'{base_path}/2018_AES_Load_Data_aes_oh_1.xlsx',
            '2019': f'{base_path}/2019_AES_Load_Data_aes_oh_1.xlsx',
            '2020': f'{base_path}/2020_AES_Load_Data_aes_oh_1.xlsx',
            '2021': f'{base_path}/2021_DPL_Load_Data_aes_oh_1.xlsx',
            '2022': f'{base_path}/2022_DPL_Load_Data_aes_oh_1.xlsx',
            '2023': f'{base_path}/2023_DPL_Load_Data_aes_oh_1.xlsx',
            '2024': f'{base_path}/2024_DPL_Load_Data_aes_oh_1.xlsx',
        }
        PIPP_file_path = f'{base_path}/2013_2017_PIPP_aes_oh_1.xlsx'

    # Deration File paths
    deration_factor_path = f'{base_path}/{deration_factor_filename}'

    # Load data
    print('Loading data...')
    deration_factor = load_deration_factor(deration_factor_path)

    hourly_load_sheets = {}
    for year in hourly_load_file_path:
        hourly_load_sheets[year] = load_volume_data(hourly_load_file_path[year])

    # Load PIPP data
    pipp_data = load_pipp_data(PIPP_file_path)

    # Process data
    print('Processing data...')
    edc_name = "OH_AES"
    deration_factor_processed = process_deration_factor(deration_factor, edc_name)

    hourly_volume_list = []
    monthly_customer_count_list = []
    daily_volume_list = []
    ufe_list = []
    res_ufe_factor_list = []
    for year in hourly_load_file_path:
        hourly_volume_df, monthly_customer_count_df, daily_volume_df, ufe_df, res_ufe_factor_df = process_load_data(hourly_load_sheets[year], edc_name, deration_factor_processed)

        hourly_volume_list.append(hourly_volume_df)
        monthly_customer_count_list.append(monthly_customer_count_df)
        daily_volume_list.append(daily_volume_df)
        ufe_list.append(ufe_df)
        res_ufe_factor_list.append(res_ufe_factor_df)

    # Combine and sort yearly data list
    daily_volume_processed = combine_processed_data(daily_volume_list, 'daily')
    ufe_factor_processed = combine_processed_data(res_ufe_factor_list, 'hourly')

    # Process pipp data
    pipp_monthly_df, pipp_hourly_df, pipp_ufe_df = process_pipp_data(pipp_data, deration_factor_processed, ufe_factor_processed, edc_name)
    hourly_volume_list.append(pipp_hourly_df)
    monthly_customer_count_list.append(pipp_monthly_df)
    ufe_list.append(pipp_ufe_df)

    # Combine and sort yearly data list
    hourly_volume_processed = combine_processed_data(hourly_volume_list, 'hourly')
    monthly_customer_count_processed = combine_processed_data(monthly_customer_count_list, 'monthly')
    ufe_processed = combine_processed_data(ufe_list, 'hourly')

    # Subtract PIPP from RES for data prior to 2017-06-01
    hourly_volume_processed, monthly_customer_count_processed, ufe_processed = subtract_pipp_data(hourly_volume_processed, monthly_customer_count_processed, ufe_processed)

    # Hard Code to correct abnormal hourly volume data on 2024-02-28 and 2024-02-29 (expect to be corrected by AES later)
    hourly_volume_processed = correct_abnormal_data(hourly_volume_processed)

    # Output path
    output_path = f'{base_path}/output_data'
    # Create output directory if it does not exist
    os.makedirs(output_path, exist_ok=True)

    hourly_output_path = f'{output_path}/aes_oh_HourlyVolume_processed.xlsx'
    monthly_output_path = f'{output_path}/aes_oh_CustomerCount_processed.xlsx'
    daily_output_path = f'{output_path}/aes_oh_NSPL_PLC_processed.xlsx'
    ufe_output_path = f'{output_path}/aes_oh_customer_UFE_processed.xlsx'

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
                         'FlowMonth', 'D', f'{customer_class} monthly volume')
        check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == customer_class) & (
                daily_volume_processed['VolumeType'] == 'PLC_Scaled')], 'FlowDate', 'D', f'{customer_class} NSPL daily volume')
        check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == customer_class) & (
                daily_volume_processed['VolumeType'] == 'PLC_Scaled')], 'FlowDate', 'D', f'{customer_class} PLC daily volume')

    # Save processed data
    print('Saving data...')
    save_processed_data(hourly_volume_processed, hourly_output_path, 'hourly')
    save_processed_data(monthly_customer_count_processed, monthly_output_path, 'monthly')
    save_processed_data(daily_volume_processed, daily_output_path, 'daily')
    save_processed_data(ufe_processed, ufe_output_path, 'ufe')

if __name__ == "__main__":
    base_path = 'C:\\Users\\5DIntern3_2024\\Work\\AES_Ohio'
    data_extract = False
    deration_factor_filename = 'DerationFactor_DAY.csv'
    main(base_path, data_extract, deration_factor_filename)

