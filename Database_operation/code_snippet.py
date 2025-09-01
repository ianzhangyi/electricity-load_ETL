import pandas as pd
import db_operations as dbop

conn, engine = dbop.db_connect('LoadStaging')
edc_name = 'OH_AEP'
customer_class = 'RES'
df_load = pd.read_sql(f"SELECT * FROM [dbo].[Load_HourlyVolumeHist] where EDCName = '{edc_name}' and CustomerClass = '{customer_class}'", engine)
