import pandas as pd
import db_operations as dbop

def check_continuity(df, date_column, table_name):
    df = df.copy()
    # Ensure the date column is in datetime format
    df[date_column] = pd.to_datetime(df[date_column])

    # Generate a DataFrame with all expected dates
    start_year = df[date_column].dt.year.min()
    end_year = df[date_column].dt.year.max()
    expected_dates = pd.date_range(start=f'{start_year}-06-01', end=f'{end_year}-06-01', freq='AS-JUN')
    expected_df = pd.DataFrame({f'{date_column}': expected_dates})

    # Merge the actual data with the expected data
    merged_df = pd.merge(expected_df, df, on=date_column, how='left', indicator=True)

    # Identify missing dates
    missing_dates = merged_df[merged_df['_merge'] == 'left_only'][date_column]
    print(f'Checking missing values in {table_name}')
    if not missing_dates.empty:
        print(f"Missing dates in {table_name}:")
        print(missing_dates)
    # else:
    #     print("All expected dates are present.")


def main():
    # Load Data
    conn, engine = dbop.db_connect('LoadStaging')
    data_db = pd.read_sql('select * from [dbo].[Load_PJMPLCNSPL] where VolumeType != \'NSPL_Volume\'', conn)
    conn.close()
    df_db = data_db.copy()
    df_output = pd.read_csv(r'C:\Users\5DIntern3_2024\Work\PJM\5CoincidentPeaks\Data_5CoincidentPeaks\5CoincidentPeaks_final.csv')

    # Check data continuity
    print('Checking output file')
    for locale_name in df_output['LocaleName'].unique():
        for volume_type in df_output[df_output['LocaleName'] == locale_name]['VolumeType'].unique():
            check_continuity(df_output[(df_output['LocaleName'] == locale_name) & (df_output['VolumeType'] == volume_type)], 'FlowMonth', f'output_data_{locale_name}_{volume_type}')
    print('Checking database file')
    for locale_name in df_db['LocaleName'].unique():
        for volume_type in df_db['VolumeType'].unique():
            check_continuity(df_db[(df_db['LocaleName'] == locale_name) & (df_db['VolumeType'] == volume_type)], 'FlowMonth', f'output_data_{locale_name}_{volume_type}')
    print('Checking Complete')


if __name__ == "__main__":
    main()