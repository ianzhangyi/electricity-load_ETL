import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from jinja2 import Environment, FileSystemLoader
import base64
from datetime import datetime


def find_xlsx_files_path(folder_path, keywords):
    file_paths = {}
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.xlsx') and keywords in file:
                file_path = os.path.join(root, file)
                file_paths[file] = file_path
    return file_paths


def load_historical_data(hourly_volume_file_path_sales_hist, hourly_volume_file_path_generation_hist):
    hourly_volume_df_list = []
    for data_type in hourly_volume_file_path_generation_hist:
        hourly_volume_df_list.append(pd.read_excel(hourly_volume_file_path_generation_hist[data_type], header=1))
    hourly_volume_df_gen = pd.concat(hourly_volume_df_list, ignore_index=True)
    hourly_volume_df_gen['DATA_TYPE'] = 'GENERATION DATA'

    hourly_volume_df_list = []
    for data_type in hourly_volume_file_path_sales_hist:
        hourly_volume_df_list.append(pd.read_excel(hourly_volume_file_path_sales_hist[data_type], header=1))
    hourly_volume_df_sales = pd.concat(hourly_volume_df_list, ignore_index=True)
    hourly_volume_df_sales['DATA_TYPE'] = 'SALES DATA'

    hourly_volume_df_hist = pd.concat([hourly_volume_df_sales, hourly_volume_df_gen], ignore_index=True)

    return hourly_volume_df_hist


def load_hourly_volume_sales_data(hourly_volume_file_path):
    hourly_volume_df_list = []
    for data_type in hourly_volume_file_path:
        hourly_volume_df_list.append(pd.read_excel(hourly_volume_file_path[data_type], header=1))
    hourly_volume_df = pd.concat(hourly_volume_df_list, ignore_index=True)
    hourly_volume_df['DATA_TYPE'] = 'SALES DATA'

    return hourly_volume_df

def load_hourly_volume_generation_data(hourly_volume_file_path):
    hourly_volume_df_list = []
    for data_type in hourly_volume_file_path:
        hourly_volume_df_list.append(pd.read_excel(hourly_volume_file_path[data_type], header=1))
    hourly_volume_df = pd.concat(hourly_volume_df_list, ignore_index=True)
    hourly_volume_df['DATA_TYPE'] = 'GENERATION DATA'

    return hourly_volume_df


def load_daily_volume_data(daily_volume_file_path):
    daily_volume_df_list = []
    for data_type in daily_volume_file_path:
        daily_volume_df_list.append(pd.read_excel(daily_volume_file_path[data_type], header=0))
    daily_volume_df = pd.concat(daily_volume_df_list, ignore_index=True)

    return daily_volume_df


def process_hourly_volume(hourly_volume_df, edc_name):
    df = hourly_volume_df.copy()
    df = df[df['CLASS'] == 'TYPE TOTAL']
    hourly_volume_list = []
    for date_type in ['SALES DATA', 'GENERATION DATA']:
        for type in ['RES', 'TYPE I', 'TYPE II']:
            for market in ['PEPCO MD ALT', 'PEPCO MD SOS', 'PEPCO MD ELIG']:
                sub_df = df[(df['TYPE'] == type) & (df['DATA_TYPE'] == date_type) & (df['MARKET'] == market)]
                sub_df = sub_df.copy()

                hour_columns = []
                for hour in range(1, 25):
                    hour_columns.append(f'HE{hour}')
                column_names = sub_df.columns.tolist()
                remaining_columns = [element for element in column_names if element not in hour_columns]
                # Deal with format issues in EPT time
                sub_df_melted = sub_df.melt(id_vars=remaining_columns, value_vars=hour_columns, var_name='hour', value_name='volume')
                sub_df_melted['hour'] = sub_df_melted['hour'].str[2:].astype(int) - 1
                sub_df_melted['datehour'] = pd.to_datetime(sub_df_melted['DATE']) + pd.to_timedelta(sub_df_melted['hour'], unit="h")
                sub_df_melted['Datetime_beginning_ept'] = sub_df_melted['datehour'].dt.tz_localize(tz='America/New_York', ambiguous='NaT', nonexistent='NaT')
                sub_df_melted = sub_df_melted[~((sub_df_melted['Datetime_beginning_ept'].isna()) & (sub_df_melted['hour'] == 2))]
                sub_df_melted = pd.concat([sub_df_melted, sub_df_melted[sub_df_melted['Datetime_beginning_ept'].isna()]], ignore_index=True)
                # Convert EPT to UTC
                sub_df_melted = sub_df_melted.sort_values(by=['datehour'], ignore_index=True)
                sub_df_melted['Datetime_beginning_ept'] = sub_df_melted['datehour'].dt.tz_localize(tz='America/New_York', ambiguous='infer')
                sub_df_melted['Datetime_beginning_utc'] = sub_df_melted['Datetime_beginning_ept'].dt.tz_convert("UTC")

                if type == 'RES':
                    customer_class = 'RES'
                elif type == 'TYPE I':
                    customer_class = 'Type 1 Non-RES'
                else:
                    customer_class = 'Type 2 Non-RES'

                hourly_volume_data = {
                    'Datetime_beginning_utc': sub_df_melted["Datetime_beginning_utc"],
                    'EDCName': edc_name,
                    'CustomerClass': customer_class,
                    'VolumeType': 'Retail_Premise'if date_type == 'SALES DATA' else 'Wholesale_Derated',
                    'EGS_HourlyVolume': sub_df_melted['volume']/1000 if market == 'PEPCO MD ALT' else 0,
                    'Default_HourlyVolume': sub_df_melted['volume']/1000 if market == 'PEPCO MD SOS' else 0,
                    'Eligible_HourlyVolume': sub_df_melted['volume']/1000 if market == 'PEPCO MD ELIG' else 0,
                    'VolumeComment': ''
                }
                hourly_volume_list.append(pd.DataFrame(hourly_volume_data))

    hourly_volume_df_processed = pd.concat(hourly_volume_list, ignore_index=True)
    hourly_volume_df_processed = hourly_volume_df_processed.groupby(['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType', 'VolumeComment'], as_index=False).sum()
    hourly_volume_df_processed = hourly_volume_df_processed[['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType', 'EGS_HourlyVolume', 'Default_HourlyVolume', 'Eligible_HourlyVolume', 'VolumeComment']]
    hourly_volume_df_processed = hourly_volume_df_processed.sort_values(by=['Datetime_beginning_utc', 'CustomerClass', 'VolumeType'], ignore_index=True)
    # Keep Eligible_HourlyVolume = Default_HourlyVolume + EGS_HourlyVolume
    hourly_volume_df_processed['Eligible_HourlyVolume'] = hourly_volume_df_processed['Default_HourlyVolume'] + hourly_volume_df_processed['EGS_HourlyVolume']

    return hourly_volume_df_processed


def process_daily_volume(daily_volume_df, edc_name):
    df = daily_volume_df.copy()
    daily_volume_list = []
    customer_count_list = []
    missing_date_list = []
    for service_type in ['MDR', 'MD1', 'MD2Q']:
        sub_df = df[df['Service Type'] == service_type].copy()
        sub_df = sub_df[['Data Date ', 'Service Type', 'SOS CPLC MW', 'SOS NSPLC MW', 'SOS COUNT', 'Eligible CPLC MW', 'Eligible NSPLC MW', 'Eligible Count']]

        # Save missing df and fill it later
        missing_df = sub_df[sub_df.isna().any(axis=1)]
        missing_df = missing_df.drop_duplicates(subset='Data Date ')

        # Save dataFrame for monthly customer count
        monthly_sub_df = sub_df[['Data Date ', 'Service Type', 'SOS COUNT', 'Eligible Count']].copy()

        # Process data
        sub_df = sub_df.fillna(0)
        sub_df = sub_df.groupby(['Data Date ', 'Service Type'], as_index=False).sum()
        sub_df['FlowDate'] = pd.to_datetime(sub_df['Data Date '])

        missing_df['FlowDate'] = pd.to_datetime(missing_df['Data Date '])
        missing_date_list.append(missing_df)

        monthly_sub_df = monthly_sub_df.groupby(['Data Date ', 'Service Type'], as_index=False).sum()
        monthly_sub_df = monthly_sub_df.dropna()
        monthly_sub_df['FlowDate'] = pd.to_datetime(monthly_sub_df['Data Date '])

        if service_type == 'MD1':
            customer_class = 'Type 1 Non-RES'
        elif service_type == 'MD2Q':
            customer_class = 'Type 2 Non-RES'
        else:
            customer_class = 'RES'

        daily_volume_data = {
            'FlowDate': sub_df['FlowDate'],
            'EDCName': edc_name,
            'CustomerClass': customer_class,
            'VolumeType': 'PLC_Unscaled',
            'EGS_DailyVolume': 0,
            'Default_DailyVolume': sub_df['SOS CPLC MW'],
            'Eligible_DailyVolume': sub_df['Eligible CPLC MW'],
            'VolumeComment': ''
        }
        daily_volume_list.append(pd.DataFrame(daily_volume_data))

        daily_volume_data = {
            'FlowDate': sub_df['FlowDate'],
            'EDCName': edc_name,
            'CustomerClass': customer_class,
            'VolumeType': 'NSPL_Scaled',
            'EGS_DailyVolume': 0,
            'Default_DailyVolume': sub_df['SOS NSPLC MW'],
            'Eligible_DailyVolume': sub_df['Eligible NSPLC MW'],
            'VolumeComment': ''
        }
        daily_volume_list.append(pd.DataFrame(daily_volume_data))

        customer_count_data = {
            'FlowMonth': monthly_sub_df['FlowDate'].dt.strftime('%Y-%m-01'),
            'EDCName': edc_name,
            'CustomerClass': customer_class,
            'VolumeType': 'CustomerCount',
            'EGS_MonthlyVolume': 0,
            'Default_MonthlyVolume': monthly_sub_df['SOS COUNT'],
            'Eligible_MonthlyVolume': monthly_sub_df['Eligible Count'],
            'VolumeComment': ''
        }
        customer_count_list.append(pd.DataFrame(customer_count_data))

    # Concat daily data
    daily_volume_processed = pd.concat(daily_volume_list, ignore_index=True)
    # Calculate EGS_DailyVolume
    daily_volume_processed['EGS_DailyVolume'] = daily_volume_processed['Eligible_DailyVolume'] - daily_volume_processed['Default_DailyVolume']
    daily_volume_processed = daily_volume_processed.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'], ignore_index=True)

    # Forward fill daily missing values
    missing_date_df = pd.concat(missing_date_list, ignore_index=True)
    value_columns = ['EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume']
    missing_date = list(missing_date_df['FlowDate'].unique())
    missing_date.append(datetime.strptime('2017-08-14 00:00:00', '%Y-%m-%d %H:%M:%S'))
    for date in missing_date:
        for customer_class in ['Type 1 Non-RES', 'Type 2 Non-RES', 'RES']:
            for volume_type in daily_volume_processed['VolumeType'].unique():
                daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == date) &
                                           (daily_volume_processed['CustomerClass'] == customer_class) &
                                           (daily_volume_processed['VolumeType'] == volume_type), value_columns] = \
                    daily_volume_processed.loc[(daily_volume_processed['FlowDate'] == date - pd.to_timedelta(1, unit="d")) &
                                               (daily_volume_processed['CustomerClass'] == customer_class) &
                                               (daily_volume_processed['VolumeType'] == volume_type), value_columns].values

    daily_volume_processed['FlowDate'] = daily_volume_processed['FlowDate'].dt.strftime('%Y-%m-%d')
    # Concat monthly data
    monthly_volume_processed = pd.concat(customer_count_list, ignore_index=True)
    monthly_volume_processed = monthly_volume_processed.groupby(['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType', 'VolumeComment'], as_index=False).mean()

    # Fix abnormal data in monthly customer count
    columns = ['EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume']
    for customer_class in monthly_volume_processed['CustomerClass'].unique():
        monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2017-08-01'), columns] =\
            monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2017-07-01'), columns].values/3 + \
            monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2017-06-01'), columns].values/3 + \
            monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2017-10-01'), columns].values/3

        monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2017-09-01'), columns] =\
            monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2017-07-01'), columns].values/3 + \
            monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2017-10-01'), columns].values/3 + \
            monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2017-11-01'), columns].values/3

        monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2022-12-01'), columns] =\
            monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2022-11-01'), columns].values/2 + \
            monthly_volume_processed.loc[(monthly_volume_processed['CustomerClass'] == customer_class) & (monthly_volume_processed['FlowMonth'] == '2023-01-01'), columns].values/2

    # Calculate EGS_MonthlyVolume
    monthly_volume_processed['Eligible_MonthlyVolume'] = monthly_volume_processed['Eligible_MonthlyVolume'].round(0)
    monthly_volume_processed['Default_MonthlyVolume'] = monthly_volume_processed['Default_MonthlyVolume'].round(0)
    monthly_volume_processed['EGS_MonthlyVolume'] = monthly_volume_processed['Eligible_MonthlyVolume'] - monthly_volume_processed['Default_MonthlyVolume']

    # Reorder columns and sort values
    monthly_volume_processed = monthly_volume_processed.sort_values(by=['FlowMonth', 'CustomerClass'], ignore_index=True)
    monthly_volume_processed = monthly_volume_processed[['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType',
                                                         'EGS_MonthlyVolume', 'Default_MonthlyVolume',
                                                         'Eligible_MonthlyVolume', 'VolumeComment']]

    return daily_volume_processed, monthly_volume_processed


def plot_monthly_data(df, output_dir, edc_name):
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
        plt.savefig(f'{output_dir}/{edc_name}_CustomerCounts_{customer_class}_plot.png')
        plot_path[f'monthly_{customer_class}'] = f'{output_dir}/{edc_name}_CustomerCounts_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


def plot_hourly_data(df, output_dir, edc_name):
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

    for volume_type in data['VolumeType'].unique():
        for customer_class in customer_classes:
            fig, axs = plt.subplots(len(columns_to_plot), 1, figsize=(20, 20), sharex=False)

            class_data = data[(data['CustomerClass'] == customer_class) & (data['VolumeType'] == volume_type)]

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
            plt.savefig(f'{output_dir}/{edc_name}_HourlyLoad_{customer_class}_{volume_type}_plot.png')
            plot_path[f'hourly_{customer_class}_{volume_type}'] = f'{output_dir}/{edc_name}_HourlyLoad_{customer_class}_{volume_type}_plot.png'
            plt.close(fig)

    return plot_path


def plot_daily_data(df, output_dir, edc_name):
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
        plt.savefig(f'{output_dir}/{edc_name}_PLC_NSPL_{customer_class}_plot.png')
        plot_path[f'daily_{customer_class}'] = f'{output_dir}/{edc_name}_PLC_NSPL_{customer_class}_plot.png'
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


def generate_keystats(monthly_df, hourly_df, daily_df):
    data = {'daily': daily_df, 'hourly': hourly_df, 'monthly': monthly_df}
    report_keystats_table = {}
    for freq in ['daily', 'hourly', 'monthly']:
        columns = [f'EGS_{freq.capitalize()}Volume', f'Default_{freq.capitalize()}Volume', f'Eligible_{freq.capitalize()}Volume']
        if freq == 'hourly':
            df = data[freq]

            for volume_type in df['VolumeType'].unique():
                sub_df = df[df['VolumeType'] == volume_type]
                customer_classes_list = sub_df['CustomerClass'].unique()
                for customer_classes in customer_classes_list:
                    report_keystats_table.update({f'{freq}_{customer_classes}_{volume_type}': sub_df[sub_df['CustomerClass'] == customer_classes][columns].describe().T})
        else:
            df = data[freq]
            customer_classes_list = df['CustomerClass'].unique()
            for customer_classes in customer_classes_list:
                report_keystats_table.update({f'{freq}_{customer_classes}': df[df['CustomerClass'] == customer_classes][columns].describe().T})

    return report_keystats_table


def generate_report(etl_report_output_path, report_keystats_table, plot_path, edc_name):
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
            'plot': encode_image_to_base64(plot_path[data_type])
        })

    date_today = datetime.today().strftime('%Y-%m-%d')

    # Data for the template
    report_data = {
        'report_title': f'{edc_name} ETL Report',
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



def main(base_path):
    # Load hourly data
    print('Loading data...')
    hourly_volume_file_path_sales = {
        '2017_sales': f'{base_path}/PEPCOSOS.MD2017.SALES.xlsx',
        '2018_sales': f'{base_path}/PEPCOSOS.MD2018.SALES.xlsx',
        '2019_sales': f'{base_path}/PEPCO MD 2019 SALES.xlsx',
        '2020_sales': f'{base_path}/PEPCO MD 2020 SALES.xlsx',
        '2021_sales': f'{base_path}/PEPCO MD 2021 SALES.xlsx',
        '2022_sales': f'{base_path}/PEPCO MD 2022 SALES.xlsx',
        '2023_sales': f'{base_path}/PEPCO MD 2023 SALES.xlsx',
        '2024_sales': f'{base_path}/PEPCO MD 2024 SALES.xlsx',
        '2024_sales_by_may': f'{base_path}/Pepco MD SOS 03.01.24 to 5.21.24 SALES.xlsx',
    }

    hourly_volume_file_path_generation = {
        '2017': f'{base_path}/PEP MD SOS 2017.xlsx',
        '2018': f'{base_path}/PEP MD SOS 2018.xlsx',
        '2019': f'{base_path}/Pepco MD SOS 2019.xlsx',
        '2020': f'{base_path}/Pepco MD SOS 2020.xlsx',
        '2021': f'{base_path}/Pepco MD SOS 2021.xlsx',
        '2022': f'{base_path}/Pepco MD SOS 2022.xlsx',
        '2023': f'{base_path}/Pepco MD SOS 2023.xlsx',
        '2024': f'{base_path}/Pepco MD SOS 2024.xlsx',
        '2024_by_may': f'{base_path}/Pepco MD SOS 03.01.24 to 5.21.24.xlsx'
    }

    daily_volume_file_path = {
        '2017': f'{base_path}/2017 Pepco MD CPLC, NSPLC and Customer Counts.xlsx',
        '2018': f'{base_path}/2018 Pepco MD CPLC, NSPLC and Customer Count.xlsx',
        '2019': f'{base_path}/2019 Pepco MD CPLC, NSPLC and Customer Count.xlsx',
        '2020': f'{base_path}/2020 Pepco MD CPLC, NSPLC and Customer Count.xlsx',
        '2021': f'{base_path}/2021 Pepco MD CPLC, NSPLC and Customer Count.xlsx',
        '2022': f'{base_path}/2022 Pepco MD CPLC, NSPLC, and customer counts.xlsx',
        '2023': f'{base_path}/2023 Pepco MD CPLC, NSPLC, and customer counts.xlsx',
        '2024': f'{base_path}/2024 Pepco MD CPLC, NSPLC, and customer counts.xlsx',
    }

    hourly_volume_df_generation = load_hourly_volume_generation_data(hourly_volume_file_path_generation)
    hourly_volume_df_sales = load_hourly_volume_sales_data(hourly_volume_file_path_sales)
    hourly_volume_df = pd.concat([hourly_volume_df_generation, hourly_volume_df_sales], ignore_index=True)

    daily_volume_df = load_daily_volume_data(daily_volume_file_path)

    hourly_volume_file_path_sales_hist = find_xlsx_files_path(f'{base_path}/historical_data', 'SALES')
    hourly_volume_file_path_generation_hist = find_xlsx_files_path(f'{base_path}/historical_data', 'PEP MD SOS')

    hourly_volume_df_hist = load_historical_data(hourly_volume_file_path_sales_hist, hourly_volume_file_path_generation_hist)

    hourly_volume_df = pd.concat([hourly_volume_df, hourly_volume_df_hist], ignore_index=True)

    # Update daily and monthly volume
    daily_volume_df_hist = pd.read_excel(r"C:\Users\5DIntern3_2024\Work\PEPCO_MD\historical_data\daily_PLC&NSPL_hist.xlsx")
    monthly_volume_df_hist = pd.read_excel(r"C:\Users\5DIntern3_2024\Work\PEPCO_MD\historical_data\monthly_CustomerCount_hist.xlsx")

    # Process data
    print('Processing data...')
    edc_name = "MD_PEPCO"
    hourly_volume_processed = process_hourly_volume(hourly_volume_df, edc_name)
    daily_volume_processed, monthly_volume_processed = process_daily_volume(daily_volume_df, edc_name)

    # Combine monthly volume with historical data
    monthly_volume_processed = pd.concat([monthly_volume_processed, monthly_volume_df_hist], ignore_index=True)
    monthly_volume_processed = monthly_volume_processed.sort_values(by=['FlowMonth', 'CustomerClass'], ignore_index=True)

    # Handling missing values in daily volume
    monthly_df_list = []
    customer_class_list = daily_volume_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        for volume_type in ['NSPL_Scaled', 'PLC_Unscaled']:
            df_raw = daily_volume_processed[(daily_volume_processed['CustomerClass'] == customer_class) & (daily_volume_processed['VolumeType'] == volume_type)]
            complete_range = pd.date_range(start=daily_volume_processed['FlowDate'].min(), end=daily_volume_processed['FlowDate'].max(), freq='D')
            missing_dates_hours = complete_range[~complete_range.isin(df_raw['FlowDate'])]
            missing_df = missing_dates_hours.strftime('%Y-%m-%d').to_frame(name='FlowDate')
            df_processed = pd.concat([missing_df, df_raw], ignore_index=True)
            df_processed = df_processed.sort_values(by=['FlowDate'], ignore_index=True)
            df_processed = df_processed.ffill()
            monthly_df_list.append(df_processed)
    daily_volume_processed = pd.concat(monthly_df_list, ignore_index=True)
    # Combine with historical data
    daily_volume_processed = pd.concat([daily_volume_processed, daily_volume_df_hist], ignore_index=True)
    daily_volume_processed = daily_volume_processed.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'], ignore_index=True)

    # Output path
    output_path = f'{base_path}/output_data'
    # Create output directory if it does not exist
    os.makedirs(output_path, exist_ok=True)

    hourly_output_path = f'{output_path}/{edc_name}_HourlyVolume_processed.xlsx'
    monthly_output_path = f'{output_path}/{edc_name}_CustomerCount_processed.xlsx'
    daily_output_path = f'{output_path}/{edc_name}_NSPL_PLC_processed.xlsx'

    etl_report_output_path = f'{output_path}/ETL_report'

    # Plot data for correction
    print('Saving plot...')
    plot_path = {}
    plot_path.update(plot_monthly_data(monthly_volume_processed, etl_report_output_path, edc_name))
    plot_path.update(plot_hourly_data(hourly_volume_processed, etl_report_output_path, edc_name))
    plot_path.update(plot_daily_data(daily_volume_processed, etl_report_output_path, edc_name))

    # Generate ETL report
    report_keystats_table = generate_keystats(monthly_volume_processed, hourly_volume_processed, daily_volume_processed)
    generate_report(etl_report_output_path, report_keystats_table, plot_path, edc_name)

    # Check Continuity
    print('Checking continuity...')

    # Check monthly data continuity
    customer_class_list = monthly_volume_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        check_continuity(monthly_volume_processed[monthly_volume_processed['CustomerClass'] == customer_class],
                         'FlowMonth', 'M', f'{customer_class} monthly volume')

    customer_class_list = hourly_volume_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        for volume_type in hourly_volume_processed['VolumeType'].unique():
            check_continuity(hourly_volume_processed[(hourly_volume_processed['CustomerClass'] == customer_class) & (hourly_volume_processed['VolumeType'] == volume_type)],'Datetime_beginning_utc', 'H', f'{customer_class} {volume_type} hourly volume')

    customer_class_list = daily_volume_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        for volume_type in ['NSPL_Scaled', 'PLC_Unscaled']:
            check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == customer_class) & (daily_volume_processed['VolumeType'] == volume_type)], 'FlowDate', 'D', f'{customer_class} {volume_type} daily volume')

    # Save processed data
    print('Saving data...')
    save_processed_data(hourly_volume_processed, hourly_output_path, 'hourly')
    save_processed_data(monthly_volume_processed, monthly_output_path, 'monthly')
    save_processed_data(daily_volume_processed, daily_output_path, 'daily')

if __name__ == "__main__":
    base_path = 'C:\\Users\\5DIntern3_2024\\Work\\PEPCO_MD'
    main(base_path)

