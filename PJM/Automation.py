import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from zipfile import ZipFile
import datetime
from datetime import datetime
import regex as re
import zipfile
import glob
import shutil
import random
import stat


def find_data_url(base_url, year, keyword, is_current_year, is_rrr=False, is_5cps=False, is_scaling=False):
    """
    Find the url links to the files from different years.

    Parameters:
    - base_url (str): The URL of the website where you can see updates of the data files.
    - year (int): The year of the data during this time of downloading.
    - keyword (str): The name of the data type.
    - is_current_year(boolean): If it is downloading the current year data (True for current year).
    - is_5cps(boolean): If it is downloading data for 5CoincidentPeaks.
    - is_scaling(boolean): If it is downloading data for 5CoincidentPeaks.

    Returns:
    - list: A list contains desired links of data files (in str).
    """
    current_year = datetime.now().year
    page = requests.get(base_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    links = soup.find_all('a')

    relevant_links = []

    # Current year data in monthly data files; historical data in annually zip files; find different patterns for each
    if is_current_year:
        pattern = re.compile(rf"{keyword}(-\w+)?-{year}\.ashx", re.IGNORECASE)
        relevant_links = [link['href'] for link in links if 'href' in link.attrs and pattern.search(link['href'])]
    else:
        pattern = re.compile(rf"{keyword}-{year}.ashx", re.IGNORECASE)
        relevant_links = [link['href'] for link in links if 'href' in link.attrs and pattern.search(link['href'])]

    if is_rrr:
        if year == 2023:
            pattern = re.compile(rf"/{year}\.ashx$", re.IGNORECASE)
            relevant_links = [link['href'] for link in links if link.get('href') and pattern.search(link['href'])]

    if is_5cps:
        pattern = re.compile(rf"summer-{year}{keyword}.ashx", re.IGNORECASE)
        relevant_links = [link['href'] for link in links if 'href' in link.attrs and pattern.search(link['href'])]

    if is_scaling:
        pattern = re.compile(rf"({year}-{keyword})", re.IGNORECASE)
        relevant_links = [link['href'] for link in links if 'href' in link.attrs and pattern.search(link['href'])]

    return relevant_links

# Note this downloading function is for data in excel/zip files
def download_files(directory, link, is_current_year=None, is_rrr=False, is_scaling=False):
    """
    Downloaded files both in current and past years with the full links and rename downloaded files.

    Parameters:
    - Directory (str): The base directory where the downloaded data are stored.
    - link (str): The URL of the data files.
    - is_current_year(boolean): If it is downloading the current year data (True for current year).
    - is_rrr (boolean): If it is downloading data for ReactiveRevenueRequirements.
    - is_scaling(boolean): If it is downloading data for ScalingFactor

    Returns:
    - None: If the downloading task fails, print messages to show and exit
    - List: If the downloading task succeeds, a list of downloaded file links in str
    """
    base_url = 'https://www.pjm.com'
    if not link.startswith('http'):
        link = base_url + link

    os.makedirs(directory, exist_ok=True)
    filename = "default_filename"  # Default filename as a fallback

    if is_current_year is not None:
        if is_rrr:
            match = re.search(r'(\w+)-(\d{4})\.ashx', link) or re.search(r'(\d{4})\.ashx', link)
        else:
            match = re.search(r'(\w+)-(\d{4})\.ashx', link)

        if match:
            if is_current_year:
                month_year = match.group(0).replace('.ashx', '')
                month, year = month_year.split('-')
                filename = f"{month[:3]}-{year}.xlsx" if not is_rrr else f"{month[:3]}-{year}.xls"
            else:
                year = match.group(1) if is_rrr and len(match.groups()) == 1 else match.group(2)
                filename = f"{year}.zip"
        else:
            print("Date information not found in URL")
            return

    elif is_scaling:
        match = re.search(r'(\d{4}-\d{4})', link)
        if match:
            year = match.group(0)
            if year <= str(2015):
                filename = f"{year}.xls"
            else:
                filename = f"{year}.xlsx"
        else:
            print("Year range not found in URL")
            return

    # Full path where the file will be saved
    save_path = os.path.join(directory, filename)
    response = requests.get(link)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded file saved to {save_path}")
    else:
        print(f"Failed to download file: status code {response.status_code}")

    return [save_path]

# Note this downloading function is for data in pdf files
def download_pdf_files(directory, link, is_NSPL=False, is_5cps=False, is_NITS=False, is_current_year=False):
    """
    Downloaded pdf files with the full links and rename downloaded files.

    Parameters:
    - directory (str): The base directory where the downloaded data are stored.
    - link (str): The URL of the data files.
    - is_NSPL (boolean): If it is downloading data for NSPL.
    - is_5cps (boolean): If it is downloading data for 5ConincidentPeaks.
    - is_NITS (boolean): If it is downloading data for NITS.
    - is_current_year (boolean): If it is downloading the current year data (True for current year).

    Returns:
    - None: If the downloading task fails, print messages to show and exit
    - List: If the downloading task succeeds, a list of downloaded file links in str
    """
    base_url = 'https://www.pjm.com'
    if not link.startswith('http'):
        link = base_url + link
    os.makedirs(directory, exist_ok=True)

    downloaded_pdf_paths = []

    if is_NSPL:
        match = re.search(r'-(\d{4})\.ashx', link)
        if match:
            year = match.group(1)
            filename = f"NSPL-{year}.pdf"
            save_path = os.path.join(directory, filename)
            response = requests.get(link)
            if response.status_code == 200:
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"Downloaded file saved to {save_path}")
                downloaded_pdf_paths.append(save_path)
            else:
                print(f"Failed to download file: status code {response.status_code}")
        else:
            print("Year not found in URL")
            return

    if is_5cps:
        match = re.search(r'summer-(\d{4})-peaks', link)
        if match:
            year = match.group(1)
            filename = f"5CPS-{year}.pdf"
            save_path = os.path.join(directory, filename)
            response = requests.get(link)
            if response.status_code == 200:
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"Downloaded file saved to {save_path}")
                downloaded_pdf_paths.append(save_path)
            else:
                print(f"Failed to download file: status code {response.status_code}")
        else:
            print("Year not found in URL")
            return

    if is_NITS:
        match = re.search(r'(\w+)-(\d{4})\.ashx', link) if is_current_year else re.search(r'-(\d{4})\.ashx', link)
        if match:
            year = match.group(2) if is_current_year else match.group(1)
            month = match.group(1)[:3] if is_current_year else ''
            filename = f"NITS-{month}-{year}.pdf" if month else f"NITS-{year}.pdf"
            save_path = os.path.join(directory, filename)
            response = requests.get(link)
            if response.status_code == 200:
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"Downloaded file saved to {save_path}")
                downloaded_pdf_paths.append(save_path)
            else:
                print(f"Failed to download file: status code {response.status_code}")
        else:
            print("Year not found in URL")
            return

    return downloaded_pdf_paths

def unzip_files(zip_path, extract_to, is_rrr=False):
    """
    Unzip the data files in previous years

    Parameters:
    - zip_path (str) The path of the folder containing zip files.
    - extract_to (str): The path of the folder where the unzip files are saved.
    - is_rrr (boolean): If it is downloading data for ReactiveRevenueRequirements.

    Returns:
    - None: If downloading and renaming tasks fails, print messages to show and exit.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Get the list of file names, want to preserve the formats of the filenames while edit some details
        zip_info = zip_ref.infolist()

        for zip_info in zip_info:
            original_filename = zip_info.filename
            if is_rrr:
                match = re.search(r'reactive-revenue-requirements-table-([a-zA-Z]+)-(\d{4})\.xls', original_filename)
                if match:
                    new_filename = f"{match.group(1)[:3]}-{match.group(2)}.xls"
                    zip_info.filename = new_filename
                else:
                    print(f"Extracted {original_filename} without renaming")
                zip_ref.extract(zip_info, extract_to)
            else:
                # match = re.search(r'black-start-revenue-requirements-(table-)?(\w+)-(\d{4})\s*(?:\(\d+\))?.xlsx',
                #                   original_filename)
                # if match:
                #     # Construct new filename using abbreviations of the month and the year
                #     new_filename = f"{match.group(2)[:3]}-{match.group(3)}.xlsx"
                #     zip_info.filename = new_filename

                month_map = {
                    '01': 'jan', '02': 'feb', '03': 'mar', '04': 'apr', '05': 'may', '06': 'jun',
                    '07': 'jul', '08': 'aug', '09': 'sep', '10': 'oct', '11': 'nov', '12': 'dec'
                }

                match = re.search(r'black-start-revenue-requirements-(table-)?(\w+)-(\d{4}).xlsx',
                                  original_filename, re.IGNORECASE) or \
                        re.search(r'black-start-revenue-requirements-(\w+)-(\d{4})\s*\(?\d*\)?\.xlsx', original_filename, re.IGNORECASE) \
                        or re.search(r'BlackStart Revenue Requirement_V(\d{1,2})_(\d{4})\.xlsx', original_filename,
                                     re.IGNORECASE)
                if match:
                    if  match.lastindex== 3:
                        new_filename = f"{match.group(2)[:3]}-{match.group(3)}.xlsx"
                    else:  # For the second regex format without month
                        month = match.group(1)
                        year = match.group(2)

                        if month in month_map:
                            month_new = month_map[month]
                        else:
                            month_new = month[:3]
                        new_filename = f"{month_new}-{year}.xlsx"
                    zip_info.filename = new_filename

                else:
                    print(f"Extracted {original_filename} without renaming")
                zip_ref.extract(zip_info, extract_to)

def delete_contents(directory, delete_zip=False, delete_xlsx=False, delete_pdf=False, delete_csv=False,
                    target_year=None):
    """
    Delete specified file types from a directory if they contain the target year in their filename.

    Parameters:
    - directory (str): Directory path where deletion tasks are performed.
    - delete_zip (bool): If True, delete .zip files.
    - delete_xlsx (bool): If True, delete .xlsx or .xls file.
    - delete_pdf (bool): If True, delete .pdf files.
    - delete_csv (bool): If True, delete .csv files.
    - target_year (str): Year to target for file deletion, included in the filename.

    Returns:
    - None
    """
    if not os.path.isdir(directory):
        print(f"The provided path {directory} is not a directory")
        return

    # A flag to check if any specific conditions are set
    specific_conditions = delete_zip or delete_xlsx or delete_pdf or target_year or delete_csv

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            # Check each file type and delete if conditions are met
            if ((delete_zip and filename.endswith('.zip')) or
                    (delete_xlsx and filename.endswith('.xlsx')) or
                    (delete_pdf and filename.endswith('.pdf')) or
                    (delete_csv and filename.endswith('.csv'))):
                if target_year is None or target_year in filename:
                    os.remove(file_path)
                    print(f"Deleted {filename} from {target_year}: {file_path}")
            # General deletion if no specific conditions are set
            elif not specific_conditions:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                    print(f"Deleted file: {file_path}")
                else:
                    shutil.rmtree(file_path)
                    print(f"Deleted directory: {file_path}")
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

def sort_files_by_date(directory, target_year):
    """
    Order the files chronologically by month order within a specified year.

    Parameters:
    - directory (str): The file path of the folder storing the data files in '.xlsx'.
    - target_year (str): The year of the data of re-ordering.

    Returns:
    - List: A list contains sorted file paths by month within a year (in str).

         # for get_date_and_path():
         Parameter:
         - filename (str): The path of files.

          Returns:
        - Tuple: (datetime object, full file path)
    """
    files = os.listdir(directory)
    # Filter files by the specified year and ensure they are of expected format 'mmm-yyyy.xlsx'
    filtered_files = [f for f in files if (f.endswith('.xlsx') or f.endswith('.xls')) and target_year in f]

    def get_date_and_path(filename):
        parts = filename.split('-')
        month_str = parts[0]
        year_str = parts[1][:4]

        # Create a datetime object (assuming day 1 for sorting purposes)
        date = datetime.strptime(f"{month_str} {year_str}", "%b %Y")
        full_path = os.path.join(directory, filename)
        return (date, full_path)

    files_with_dates = [get_date_and_path(file) for file in filtered_files]

    sorted_files_with_dates = sorted(files_with_dates, key=lambda x: x[0])
    sorted_file_paths = [path for _, path in sorted_files_with_dates]

    return sorted_file_paths

def check_missing_months(directory, checking_year):
    """
        Check for missing monthly data within the specified directory.

        Parameters:
        - directory (str): The path of folder containing unzipped files.
        - checking_year (str): The year of the data as we check for missing months per year.

        Returns:
        - List: A list contains the missing month name of the missing data.
        """
    all_months = {'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'}
    month_regex = re.compile(r'-(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-', re.IGNORECASE)
    months_found = set()

    for filename in os.listdir(directory):
        if (filename.endswith('.xlsx') or filename.endswith('.xls')) and any(
                filename.lower().startswith(month + '-') for month in all_months):
            parts = filename.split('-')
            # Assuming filename format 'month-year.xlsx'
            month = parts[0].lower()
            year = parts[1][:4]
            if year == checking_year and month in all_months:
                months_found.add(month)

    missing_months = sorted(all_months - months_found)
    if missing_months:
        print("Missing months:", missing_months)
    else:
        print("None missing monthly data")
    return missing_months

def make_files_writable(directory):
    """
    Modify the file permissions in a specified directory to make all files writable.

    Parameters:
    - directory (str): The path to the directory whose files are to be made writable.

    Return:
    - None: print a message to show which file has been set to writable
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            if not os.access(file_path, os.W_OK):
                # Remove read-only attribute
                os.chmod(file_path, stat.S_IWRITE)
                print(f"Made writable: {file_path}")



def manual_read_excel(directory, is_black_start=False, is_rrr=False, is_scaling=False):
    file_paths = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    manual_files_paths = []

    for file_path in file_paths:
        filename = os.path.basename(file_path)
        original_filename = filename

        filename = "default_filename"
        if is_black_start:
            match = re.search(r'(\w+)-(\d{4})', file_path)
            if match:
                filename = f"{match.group(1)[:3]}-{match.group(2)}.xlsx"

        #todo: edit this later
        # elif is_rrr:
        #     match = re.search(r'(\w+)-(\d{4})\.ashx', file_path) or re.search(r'(\d{4})\.ashx', file_path)
        #     if match:
        #         year = match.group(1) if len(match.groups()) == 1 else match.group(2)
        #         filename = f"{year}.zip"
        # elif is_scaling:
        #     match = re.search(r'(\d{4}-\d{4})', file_path)
        #     if match:
        #         year = match.group(0)
        #         if int(year.split('-')[0]) <= 2015:
        #             filename = f"{year}.xls"
        #         else:
        #             filename = f"{year}.xlsx"

        if filename == "default_filename":
            print(f"Date information not found in URL for file {original_filename}")
            continue

        # Construct full path for the renamed file
        new_file_path = os.path.join(directory, filename)
        if new_file_path != file_path:
            os.rename(file_path, new_file_path)
            print(f"Renamed '{file_path}' to '{new_file_path}'")
        manual_files_paths.append(new_file_path)

    return manual_files_paths


