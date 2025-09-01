import pandas as pd
# import modin.pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib
import requests
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
import os
import os.path
import http.client, urllib.request, urllib.parse, urllib.error, base64
import os.path
from util.configUtil import ConfigUtil
import time

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
np.set_printoptions(suppress=True)

# Driver={ODBC Driver 13 for SQL Server};Server=tcp:5ddev1.database.windows.net,1433;Database=AnalysisDB;Uid={};Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
def db_connect(db=None, pool_size=None):
    cwd = os.getcwd()
    # it returns error if using ./ directly
    basedir = os. path. dirname(__file__) + "\\"
    m = ConfigUtil(basedir=basedir)
    rt = m.loadConfig()

    try:
        uid = rt.get(db, 'UID')
        pwd = rt.get(db, 'PWD')
        if uid == '':
            uid = rt.get('Default_Credential', 'UID')
        if pwd == '':
            pwd = rt.get('Default_Credential', 'PWD')
    except Exception as e:
        # if we do not have certain section in config file, we pass in default credential
        uid = rt.get('Default_Credential', 'UID')
        pwd = rt.get('Default_Credential', 'PWD')

    if db == 'local' or db == 'SPP':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=AnalysisDB;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'PJM':
        # 'Used to point to FTRPJM, switched to FTRStaging'
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=FTRStaging;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
            r'Autocommit=true'
        ).format(uid, pwd)
    elif db == 'PJM_PathFinder':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=PathFinder;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'Daniu_PathFinder':
        vnet_ip = get_vnetip('DESKTOP-NQ7U7IN')
        # r'Server=desktop-nq7u7in;'
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=PathFinder_PJM;'
            r'UId={};'
            r'Pwd={};'.format('DESKTOP-NQ7U7IN', uid, pwd)
        )
    elif db == 'FTRStaging':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=FTRStaging;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'FTRPJM':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=FTRPJM;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'PJMSelector':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=PJM;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'PJM_Archive':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=FTRAnalytics;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'MISO':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=AnalysisDB;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'SE':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=SE;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'DZ_Cloud':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=PJM;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'ERCOT':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=FTRERCOT;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'DZ':
        # r'Server=desktop-nq7u7in;'
        # 5DPRINCETON1
        vnet_ip = get_vnetip('5DPRINCETON1')
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Output;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format('5DPRINCETON1', uid, pwd)
        conn_str1 = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Output;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format(vnet_ip, uid, pwd)
    elif db == 'DZ_PJM':
        vnet_ip = get_vnetip('5DPRINCETON2')
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Output;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format('5DPRINCETON2', uid, pwd)
        conn_str1 = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Output;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format(vnet_ip, uid, pwd)
    elif db == 'NODAL_PJM':
        vnet_ip = get_vnetip(servername='5DPRINCETON2')
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=NODAL_PJM;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format('5DPRINCETON2', uid, pwd)
        conn_str1 = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=NODAL_PJM;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format(vnet_ip, uid, pwd)
    elif db == 'ISO_PJM':
        vnet_ip = get_vnetip(servername='5DPRINCETON03')
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=ISO_PJM;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format('5DPRINCETON03', uid, pwd)
        conn_str1 = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=ISO_PJM;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format(vnet_ip, uid, pwd)
    elif db == 'Supplemental':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=LAMBO2;'
            r'Database=SupplementalDB;'
            r'Trusted_Connection=yes;'
        )
    elif db == 'local_server':
        conn_str = (
            r'Driver=ODBC Driver 13 for SQL Server;'
            r'Server=.;'
            r'Database=Dayzer_Input;'
            r'Trusted_Connection=yes;'
        )
    elif db == 'DZP2':
        vnet_ip = get_vnetip('5DPRINCETON2')
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Output;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format('5DPRINCETON2', uid, pwd)
        conn_str1 = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Output;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format(vnet_ip, uid, pwd)
    elif db == '5DP1_Dayzer_Input':
        vnet_ip = get_vnetip('5DPRINCETON1')
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Input;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format('5DPRINCETON1', uid, pwd)
        conn_str1 = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Input;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format(vnet_ip, uid, pwd)
    elif db == '5DP2_Dayzer_Input':
        vnet_ip = get_vnetip('5DPRINCETON2')
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Input;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format('5DPRINCETON2', uid, pwd)
        conn_str1 = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Dayzer_Input;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format(vnet_ip, uid, pwd)
    elif db == 'Azure_Dayzer':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=Dayzer;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'Caiso_Staging':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=CAISOStaging;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'Ddev2_FTRPJM':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev2.database.windows.net,1433;'
            r'Database=FTRPJM;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'MISO_Intern':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev3.database.windows.net,1433;'
            r'Database=AnalysisDB;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'OTCStaging':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database=OTCStaging;'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)

    elif db in ('Load_Readonly', 'LoadStaging_Readonly', 'Load', 'LoadStaging'):
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database={};'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(db, uid, pwd)
    elif db in ('Weather'):
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=tcp:5ddev1.database.windows.net,1433;'
            r'Database={};'
            r'UId={};'
            r'Pwd={};'
            r'Encrypt=yes;'
            r'TrustServerCertificate=no;'
            r'Connection Timeout=300;'
        ).format(db, uid, pwd)
    # elif db == 'ISO_ERCOT':
    #     conn_str = (
    #         r'Driver=ODBC Driver 17 for SQL Server;'
    #         r'Server=172.16.0.136;'
    #         r'Database=ISO_ERCOT_MIS;'
    #         r'UId={};'
    #         r'Pwd={};'
    #         r'Connection Timeout=300;'
    #     ).format(uid, pwd)
    elif db == 'ISO_ERCOT':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=desktop-rt3bov8;'
            r'Database=ISO_ERCOT_MIS;'
            r'UId={};'
            r'Pwd={};'
            r'TrustServerCertificate=yes;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'lambo3':
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server=Lambo3;'
            r'Database=ISO_ERCOT_MIS;'
            r'UId={};'
            r'Pwd={};'
            r'TrustServerCertificate=yes;'
            r'Connection Timeout=300;'
        ).format(uid, pwd)
    elif db == 'Trans_PJM':
        vnet_ip = get_vnetip(servername='5DPRINCETON2')
        conn_str = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Trans_PJM;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format('5DPRINCETON2', uid, pwd)
        conn_str1 = (
            r'Driver=ODBC Driver 17 for SQL Server;'
            r'Server={};'
            r'Database=Trans_PJM;'
            r'UId={};'
            r'Pwd={};'
            r'Connection Timeout=300;'
        ).format(vnet_ip, uid, pwd)

    quoted_conn_str = urllib.parse.quote_plus(conn_str)
    if pool_size is None:
        pool_size = 10
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_conn_str), echo_pool=True, pool_size=pool_size, max_overflow=10, pool_pre_ping=True, encoding="utf-8", fast_executemany=True)
    count = 0
    while count < 4:
        try:
            conn = engine.connect()
            break
        except:
            # print('Error creating connection {}, try again'.format(int(count)))
            pass
        count = count + 1
    if (db in ('DZ', 'DZ_PJM', 'NODAL_PJM', 'ISO_PJM', 'DZP2', '5DP1_Dayzer_Input')) and count == 4:
        quoted_conn_str = urllib.parse.quote_plus(conn_str1)
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_conn_str), echo_pool=True, pool_size=pool_size, max_overflow=10, pool_pre_ping=True, encoding="utf-8", fast_executemany=True)
        count = 0
        while count < 4:
            try:
                conn = engine.connect()
                break
            except:
                # print('Error creating connection {} via VNET IP, try again'.format(int(count)))
                pass
            count = count + 1
    return conn, engine

def get_vnetip(servername='5DPRINCETON03'):
    """
    5DPRINCETON1
    5DPRINCETON2
    5DPRINCETON03
    DESKTOP-NQ7U7IN
    """
    print("let's connect")
    conn_temp, engine_temp = db_connect('FTRStaging')
    select_sql = "select VNetIp from [dbo].[VNetServerHeartBeats] where servername='{}'".format(servername)
    vnet_ip = read_sql(select_sql, conn_temp).values[0][0]
    print(vnet_ip)
    return vnet_ip

def read_sql(select_sql=None, engine=None, retry_num=3):
    """
    Replace pd.read_sql and add retry logic in it
    """
    count = 0
    df = pd.DataFrame()
    while count < retry_num:
        try:
            df = pd.read_sql(select_sql, engine)
            break
        except Exception as e:
            time.sleep(count)
        count += 1
        if count == retry_num:
            raise Exception('Error running SQL')
    return df

if __name__ == "__main__":
    print('Main')
