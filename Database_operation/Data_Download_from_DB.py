import pandas as pd
import db_operations as dbop
import os



# access database
conn, engine = dbop.db_connect('LoadStaging')
LocaleName = 'APS'
save_directory = 'C:\\Users\\5DIntern3_2024\\Work\\PE_MD'

df_load = pd.read_sql(f"SELECT * FROM [dbo].[Load_PJMHourlyDerationFactor] where LocaleName = '{LocaleName}'", engine)
print(f"{LocaleName} data downloaded from Database")

# Define the directory to save the CSV file
csv_file_path = os.path.join(save_directory, f'DerationFactor_{LocaleName}.csv')
# Save the DataFrame to a CSV file
df_load.to_csv(csv_file_path, index=False)

# # Make the CSV file read-only
# os.chmod(csv_file_path, 0o444)

print(f"{LocaleName} DataFrame saved to {csv_file_path}")
