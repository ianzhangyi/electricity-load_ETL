import pandas as pd
import numpy as np
import urllib
from datetime import date, timedelta
from pandas.io import sql
import requests
import zipfile
import os
import plotly.io as pio
pio.renderers.default = 'chrome'
from OpenSSL import crypto
import ssl
import urllib.request
from bs4 import BeautifulSoup
import re
from selenium import webdriver
import os.path
import traceback

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', '{:.3f}'.format)
np.set_printoptions(suppress=True)

# Import the module with all db functions
import db_operations as dbop

class IsoDataImporter:
    """
    This is the class for pulling data from various ISO websites, such as pulling outage data from OASIS, pulling SE/Commercial model from ExtraNet
    """
    def __init__(self, iso_name=''):
        self.iso_name = iso_name
        self.conn, self.engine = dbop.db_connect(db=self.iso_name)
        self.settings = {}
        if self.iso_name == 'MISO':
            # Maximum MW for STAT bids
            self.settings['oasis_url'] = r'https://www.oasis.oati.com'
            self.settings['oasis_plannedoutage_url'] = r'https://www.oasis.oati.com/cgi-bin/webplus.dll?script=/woa/woa-planned-outages-report.html&Provider=MISO'
            self.settings['oasis_rtoutage_url'] = r'https://www.oasis.oati.com/cgi-bin/webplus.dll?script=/woa/woa-real-time-outages-report.html&Provider=MISO'
            self.settings['plannedoutage_folder'] = r'D:\DailyOperations\MISO\Import_to_DB\Outage\PlannedOutage'
            self.settings['historicaloutage_folder'] = r'D:\DailyOperations\MISO\Import_to_DB\Outage\HistoricalOutage'
            self.settings['rtoutage_folder'] = r'D:\DailyOperations\MISO\Import_to_DB\Outage\RTOutage'
            self.settings['archive_folder'] = r'D:\DailyOperations\MISO\Import_to_DB\Outage\Archive'
            self.settings['extranet_url'] = r'https://www.misoenergy.org/extranet/'
            self.settings['extranet_login_url'] = r'https://www.misoenergy.org/Account/Login'
            self.settings['extranet_usrname'] = ''
            self.settings['extranet_password'] = ''
            self.settings['se_folder'] = r'D:\MISO_SE\SE_Cases'
            self.settings['pfx_cert_url'] = r'D:\DailyOperations\MISO\Certificates\miso_mftrc_ygu_oati.pfx'
            self.settings['pfx_cert_pwd'] = ''
            self.settings['pem_cert_url'] = r'D:\DailyOperations\MISO\Certificates\miso_mftrc_ygu_oati.pem'
            self.settings['caseid'] = '1'
        if self.iso_name == 'SPP':
            # Maximum MW for STAT bids
            self.settings['oasis_url'] = r'https://www.oasis.oati.com'
            self.settings['oasis_plannedoutage_url'] = r'http://transoutage.spp.org//report.aspx?download=true&includenulls=true&actualendgreaterthan=1/1/2016'
            self.settings['se_username'] = ''
            self.settings['se_password'] = ''
            self.settings['se_folder'] = r'D:\SPP_SE\SE_Cases'
            self.settings['pfx_cert_url'] = r'D:\DailyOperations\SPP\Certificates\webcares_ygu_oati_aftr.pfx'
            self.settings['pfx_cert_pwd'] = 'Welcome2ParkAve!'
            self.settings['pem_cert_url'] = r'D:\DailyOperations\SPP\Certificates\webcares_ygu_oati_aftr.pem'
            self.settings['caseid'] = '2'

    def convert_pfx_to_pem_format(self):
        with open(self.settings['pfx_cert_url'], "rb") as pfx:
            cert = crypto.load_pkcs12(pfx.read(), self.settings['pfx_cert_pwd'])
        pkey = cert.get_privatekey()
        with open(self.settings['pem_cert_url'], "wb") as pem:
            pem.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))
            certificates_tuple = (cert.get_certificate(),) + (cert.get_ca_certificates())
            for item in certificates_tuple:
                pem.write(crypto.dump_certificate(crypto.FILETYPE_PEM, item))
        return


    def get_oasis_plannedoutagefile_list(self):
        """ Get a list of planned outage file urls"""
        # rq = requests.get(self.settings['oasis_url'], verify=self.settings['pem_cert_url'])
        # if rq.status_code == 200:
        #     print('Oasis website connected!')
        #     soup = BeautifulSoup(rq.content)
        #     samples = soup.find_all(name='a', href=re.compile(r'/^.*?\b2308_Planned_Outages\b.*?\bxml\b.*?$/m'))
        context = ssl.create_default_context()
        context.load_cert_chain(self.settings['pem_cert_url'])
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context))
        with opener.open(self.settings['oasis_plannedoutage_url']) as response:
            html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        # print(html)
        result = soup.find_all(name='a', href=re.compile("2308_Planned_Outages"), string=re.compile("xml"))
        result1 = soup.find_all(name='a', href=re.compile("HistoricalOutages_CROW"), string=re.compile("xml"))
        self.plannedoutage_file_lists = []
        for item in result:
            self.plannedoutage_file_lists.append(item['href'])
        self.historicaloutage_file_lists = []
        for item in result1:
            self.historicaloutage_file_lists.append(item['href'])

        return self.plannedoutage_file_lists, self.historicaloutage_file_lists


    def download_plannedoutage_file(self, target_date=date.today()):
        target_year = target_date.year
        target_month = target_date.month
        target_day = target_date.day
        target_file_list = [x for x in self.plannedoutage_file_lists if str(target_year) + '-' + format(int(target_month), '02d') + '-' + format(int(target_day), '02d') in x]
        if len(target_file_list) == 0:
            print('Can not find planned outage file for target date.')
        else:
            target_file_name = target_file_list[-1]
        rq = requests.get(self.settings['oasis_url'] + target_file_name, verify=self.settings['pem_cert_url'])
        with open(self.settings['plannedoutage_folder'] + '\\' + target_file_name.split('/')[-1], 'wb') as output:
            output.write(rq.content)

        return


    def download_se_from_iso(self, start_date=date.today()-timedelta(days=30), end_date=date.today()-timedelta(days=14)):
        """
        :param start_date: start_date to download SE files
        :param end_date: end_date to download SE files
        :return:
        """

        if self.iso_name == 'MISO':
            # First get target url for SE models
            target_hours = ['00', '05', '12', '18']
            target_date = start_date
            while target_date <= end_date:
                for hr in target_hours:
                    # Get target_se_name and target_se_url
                    target_se_name = 'miso_se_{}-{}00.zip'.format(target_date.strftime('%Y%m%d'), hr)
                    target_se_url = r'https://www.misoenergy.org/api/documents/getbyname/{}'.format(target_se_name)
                    if not os.path.isfile(self.settings['se_folder'] + '\\' + target_se_name):
                        try:
                            print('Trying to download {}'.format(target_se_name))
                            result = self.download_se(target_se_name=target_se_name, target_se_url=target_se_url)
                            print('''Finished downloading SE file: {}'''.format(target_se_name))
                        except:
                            print('''Error downloading SE file: {}'''.format(target_se_name))
                target_date = target_date + timedelta(days=1)
        return


    def download_se(self, target_se_name=None, target_se_url=None):
        # First open the target url
        # !!! chromedriver.exe is specific to the version of Chrome the user is using, when encountering errors, need to go to the following URL to get hte compatible one: https://chromedriver.chromium.org/downloads
        try:
            browser = webdriver.Chrome(executable_path=r"C:\Users\Yang\Desktop\Research\codes\chromedriver.exe")
            # Check version of chromedriver.exe and Chrome
            str1 = browser.capabilities['browserVersion']
            str2 = browser.capabilities['chrome']['chromedriverVersion'].split(' ')[0]
        except Exception:
            traceback.print_exc()

        print(str1)
        print(str2)
        print(str1[0:2])
        print(str2[0:2])
        if str1[0:2] != str2[0:2]:
            print("please download correct chromedriver version")

        browser.minimize_window()
        browser.get(self.settings['extranet_login_url'])
        #browser.get(self.settings['extranet_url'])
        # enter username and password
        email = browser.find_element_by_id("Email")
        password = browser.find_element_by_id("Password")
        email.send_keys(self.settings['extranet_usrname'])
        password.send_keys(self.settings['extranet_password'])
        browser.find_element_by_class_name('pull-left').click()
        cookies = browser.get_cookies()
        updated_cookies = {}
        for item in cookies:
            print(item)
            updated_cookies[item['name']] = item['value']
        browser.close()
        browser.quit()
        # Need to used the updated_cookies as the input for cookies
        r = requests.get(target_se_url, cookies=updated_cookies)
        if r.status_code == 200:
            with open(self.settings['se_folder'] + '\\' + target_se_name, 'wb') as output:
                output.write(r.content)
                output.close()

        # Unzip the SE zip file
        with zipfile.ZipFile(self.settings['se_folder'] + '\\' + target_se_name, "r") as zip_ref:
            zip_ref.extractall(self.settings['se_folder'])
        return


def read_miso_dailyloadbylrz(target_date):
    """" Read Historical Daily Forecast and Actual Load by Local Resource Zone (xls) file from MISO website, update daily in the morning for beginning of year to current day"""
    try:
        date_today = target_date.strftime('%Y%m%d')
        url = '''https://docs.misoenergy.org/marketreports/{}_dfal_HIST.xls'''.format(date_today)
        dailyloadbylrz_df = pd.read_excel(url, header=0, skiprows=5, skipfooter=2)
    except:
        print('Error downloading the Historical Daily Forecast and Actual Load by Local Resource Zone (xls) file')
    dailyloadbylrz_df['MarketDay'] = pd.to_datetime(dailyloadbylrz_df['MarketDay'], errors='coerce')
    dailyloadbylrz_df[['HourEnding', 'MTLF (MWh)', 'ActualLoad (MWh)']] = dailyloadbylrz_df[['HourEnding', 'MTLF (MWh)', 'ActualLoad (MWh)']].apply(pd.to_numeric, errors='coerce')
    dailyloadbylrz_df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    dailyloadbylrz_df.rename(columns={'MarketDay': 'Market_Date'}, inplace=True)
    dailyloadbylrz_df = dailyloadbylrz_df.loc[pd.notnull(dailyloadbylrz_df.Market_Date), :].copy()
    return dailyloadbylrz_df


def upload_miso_dailyloadbylrz_df(dailyloadbylrz_df, engine, target_date):
    """ Upload the dailyloadbylrz_df to DB, need to remove existing record first"""
    # Delete existing records
    del_sql = '''DELETE from Target_Table where Market_Date >= \'{}\''''.format(target_date - timedelta(2))
    result = sql.execute(del_sql, engine)
    # Upload new records
    dailyloadbylrz_df.loc[dailyloadbylrz_df.Market_Date >= pd.Timestamp((target_date - timedelta(2))).to_pydatetime(), :].to_sql('Target_Table', con=engine, if_exists='append', index=False, chunksize=10000)
    return 0

def read_upload_miso_dailyloadbylrz(target_date=date.today()):
    """ MISO only publishes the file for the current day """
    conn, engine = dbop.db_connect(db='MISO')
    dailyloadbylrz_df = read_miso_dailyloadbylrz(target_date)
    status = upload_miso_dailyloadbylrz_df(dailyloadbylrz_df, engine, target_date)
    return status

def main():
    ipt = IsoDataImporter(iso_name='MISO')
    # ipt.convert_pfx_to_pem_format()
    # ipt.get_oasis_plannedoutagefile_list()
    # ipt.download_plannedoutage_file(target_date=date.today())
    ipt.download_se_from_iso(start_date=date.today() - timedelta(days=30), end_date=date.today() - timedelta(days=14))
    read_upload_miso_dailyloadbylrz(target_date=date.today())
    return


if __name__ == "__main__":
    x = 0




