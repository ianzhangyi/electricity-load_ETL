"""
Update the end_date in load_hourly_volume_data for further processing
"""

import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from jinja2 import Environment, FileSystemLoader
import base64
from datetime import datetime, timedelta


def load_hourly_volume_data(hourly_volume_file_path, start_str, end_str):
    # Initialize start and end dates
    start_date = datetime.strptime(start_str, '%b-%y')
    end_date = datetime.strptime(end_str, '%b-%y')

    # Generate list of month strings
    sheet_name_ls = []
    current_date = start_date
    while current_date <= end_date:
        sheet_name_ls.append(current_date.strftime('%b-%y'))
        # Move to the next month
        next_month = current_date + timedelta(days=31)  # ensures we move to the next month
        current_date = datetime(next_month.year, next_month.month, 1)

    # Define possible variations for month names
    month_variations = {
        "Jan": ["Jan"],
        "Feb": ["Feb"],
        "Mar": ["Mar"],
        "Apr": ["Apr"],
        "May": ["May"],
        "Jun": ["Jun", "June"],
        "Jul": ["Jul", "July"],
        "Aug": ["Aug"],
        "Sep": ["Sep", "Sept"],
        "Oct": ["Oct"],
        "Nov": ["Nov"],
        "Dec": ["Dec"]
    }

    # Read all sheets into a dictionary
    all_sheets = pd.read_excel(hourly_volume_file_path, sheet_name=None)

    # Clean the sheet names by stripping any extra spaces
    cleaned_sheets = {sheet_name.strip(): df for sheet_name, df in all_sheets.items()}

    df_list = []
    for sheet_name in sheet_name_ls:
        # Extract the month abbreviation from the sheet name
        month_abbr, year = sheet_name.split('-')

        # Find all possible variations for the given month abbreviation
        possible_names = month_variations.get(month_abbr, [month_abbr])

        # Try to find the sheet by any of its possible names
        found_sheet = False
        for possible_name in possible_names:
            possible_sheet_name = f"{possible_name}-{year}"
            if possible_sheet_name in cleaned_sheets:
                sheet_df = cleaned_sheets[possible_sheet_name]
                df_list.append(sheet_df)
                found_sheet = True
                break

        if not found_sheet:
            print(f"Sheet '{sheet_name}' not found in the file.")

    combined_df = pd.concat(df_list, ignore_index=True)

    return combined_df


def load_daily_volume_data(daily_volume_file_path):
    file_sheets = pd.read_excel(daily_volume_file_path, sheet_name=None)
    sheet_df_list = []
    for sheet_name in file_sheets:
        if 'PLC Trends DY' in sheet_name:
            sheet_df = pd.read_excel(daily_volume_file_path, sheet_name=sheet_name)
            sheet_df_list.append(sheet_df[['Type', 'Class', 'Svc', 'DATEX', 'capplc', 'count', 'trnplc']])
    combined_df = pd.concat(sheet_df_list, ignore_index=True)
    return combined_df


def process_hourly_volume(hourly_volume_df, edc_name):
    df = hourly_volume_df.copy()
    df['CustomerClass'] = df['WEBSupplier'].str[:3]
    df['SupplierType'] = df['WEBSupplier'].str[-1:]
    df['CustomerClass'] = df['CustomerClass'].replace({'PL1': 'Type 1 Non-RES', 'PL2': 'Type 2 Non-RES', 'PRX': 'RES', 'PRL': 'RES'})
    df = df.drop(columns='WEBSupplier')
    grouped_df = df.groupby(['DateHour', 'CustomerClass', 'SupplierType'], as_index=False).sum()

    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str, format='%d%b%y:%H:%M:%S')
        except ValueError:
            return pd.to_datetime(date_str.replace('D', '').replace('S', ''), format='%Y-%m-%d %H:%M')

    grouped_df['Datetime_beginning_ept'] = grouped_df['DateHour'].apply(parse_dates)

    customer_class_ls = grouped_df['CustomerClass'].unique().tolist()
    hourly_volume_list = []
    for customer_class in customer_class_ls:
        for supplier_type in ['X', 'C']:
            sub_df = grouped_df[(grouped_df['CustomerClass'] == customer_class) & (grouped_df['SupplierType'] == supplier_type)]

            # Fix duplication in raw data on 2014-11-02
            sub_df.loc[(sub_df['Datetime_beginning_ept'] == '2014-11-02 01:00:00'), ['SumOfkWh_Premise_With_UFE',
                                                                                     'SumOfkWh_PJM_Settlement']] = \
            sub_df.loc[(sub_df['Datetime_beginning_ept'] == '2014-11-02 01:00:00'), ['SumOfkWh_Premise_With_UFE',
                                                                                     'SumOfkWh_PJM_Settlement']] / 2
            # Duplicate 2013 to 2020 Nov 01:00:00 to match the ept format
            duplicate_list = []
            duplicate_list.append(sub_df[sub_df['Datetime_beginning_ept'] == '2020-11-01 01:00:00'])
            duplicate_list.append(sub_df[sub_df['Datetime_beginning_ept'] == '2019-11-03 01:00:00'])
            duplicate_list.append(sub_df[sub_df['Datetime_beginning_ept'] == '2018-11-04 01:00:00'])
            duplicate_list.append(sub_df[sub_df['Datetime_beginning_ept'] == '2017-11-05 01:00:00'])
            duplicate_list.append(sub_df[sub_df['Datetime_beginning_ept'] == '2016-11-06 01:00:00'])
            duplicate_list.append(sub_df[sub_df['Datetime_beginning_ept'] == '2015-11-01 01:00:00'])
            duplicate_list.append(sub_df[sub_df['Datetime_beginning_ept'] == '2014-11-02 01:00:00'])
            duplicate_list.append(sub_df[sub_df['Datetime_beginning_ept'] == '2013-11-03 01:00:00'])

            duplicate_list.append(sub_df)
            sub_df = pd.concat(duplicate_list,  ignore_index=True)
            sub_df = sub_df.sort_values(by=['Datetime_beginning_ept'], ignore_index=True)
            sub_df['Datetime_beginning_ept'] = sub_df['Datetime_beginning_ept'].dt.round('H')

            index = sub_df[sub_df['Datetime_beginning_ept'] == '2021-11-07 02:00:00'].index[0]
            sub_df.loc[index, 'Datetime_beginning_ept'] -= pd.Timedelta(hours=1)

            index = sub_df[sub_df['Datetime_beginning_ept'] == '2022-11-06 02:00:00'].index[0]
            sub_df.loc[index, 'Datetime_beginning_ept'] -= pd.Timedelta(hours=1)

            index = sub_df[sub_df['Datetime_beginning_ept'] == '2023-11-05 02:00:00'].index[0]
            sub_df.loc[index, 'Datetime_beginning_ept'] -= pd.Timedelta(hours=1)

            # Fix EPT pattern issue on 2016-03-13
            index = sub_df[sub_df['Datetime_beginning_ept'] == '2016-03-13 02:00:00'].index[0]
            sub_df.loc[index, 'Datetime_beginning_ept'] += pd.Timedelta(hours=1)

            sub_df['Datetime_beginning_utc'] = sub_df['Datetime_beginning_ept'].dt.tz_localize(tz='America/New_York', ambiguous='infer').dt.tz_convert("UTC")


            wholesale_data = {
                'Datetime_beginning_utc': sub_df['Datetime_beginning_utc'],
                'EDCName': edc_name,
                'CustomerClass': customer_class,
                'VolumeType': 'Wholesale_Derated',
                'EGS_HourlyVolume': sub_df['SumOfkWh_PJM_Settlement']/1000 if supplier_type == 'C' else 0,
                'Default_HourlyVolume': sub_df['SumOfkWh_PJM_Settlement']/1000 if supplier_type == 'X' else 0,
                'Eligible_HourlyVolume': sub_df['SumOfkWh_PJM_Settlement']/1000,
                'VolumeComment': ''
            }
            hourly_volume_list.append(pd.DataFrame(wholesale_data))

            premise_data = {
                'Datetime_beginning_utc': sub_df['Datetime_beginning_utc'],
                'EDCName': edc_name,
                'CustomerClass': customer_class,
                'VolumeType': 'Retail_Premise',
                'EGS_HourlyVolume': sub_df['SumOfkWh_Premise_With_UFE']/1000 if supplier_type == 'C' else 0,
                'Default_HourlyVolume': sub_df['SumOfkWh_Premise_With_UFE']/1000 if supplier_type == 'X' else 0,
                'Eligible_HourlyVolume': sub_df['SumOfkWh_Premise_With_UFE']/1000,
                'VolumeComment': ''
            }
            hourly_volume_list.append(pd.DataFrame(premise_data))

    hourly_volume_processed = pd.concat(hourly_volume_list,  ignore_index=True)
    hourly_volume_processed = hourly_volume_processed.groupby(['Datetime_beginning_utc', 'EDCName', 'CustomerClass', 'VolumeType', 'VolumeComment'], as_index=False).sum()
    hourly_volume_processed = hourly_volume_processed.sort_values(by=['Datetime_beginning_utc', 'VolumeType', 'CustomerClass'], ignore_index=True)

    return hourly_volume_processed


def process_daily_volume(daily_volume_df, edc_name):
    df = daily_volume_df.copy()
    df['Datetime'] = pd.to_datetime(df['DATEX'], format='%d%b%Y:%H:%M:%S')
    df['Type'] = df['Type'].replace({'PL1': 'Type 1 Non-RES', 'PL2': 'Type 2 Non-RES', 'PRX': 'RES', 'PRL': 'RES'})
    df = df.drop(columns=['Class', 'DATEX'])
    grouped_df = df.groupby(['Type', 'Svc', 'Datetime'], as_index=False).sum()

    daily_volume_list = []
    customer_count_list = []
    for customer_class in ['Type 1 Non-RES', 'Type 2 Non-RES', 'RES']:
        for supplier_type in ['X', 'C']:
            sub_df = grouped_df[(grouped_df['Type'] == customer_class) & (grouped_df['Svc'] == supplier_type)]
            daily_volume_data = {
                'FlowDate': sub_df['Datetime'].dt.strftime('%Y-%m-%d'),
                'EDCName': edc_name,
                'CustomerClass': customer_class,
                'VolumeType': 'PLC_Unscaled',
                'EGS_DailyVolume': sub_df['capplc']/1000 if supplier_type == 'C' else 0,
                'Default_DailyVolume': sub_df['capplc']/1000 if supplier_type == 'X' else 0,
                'Eligible_DailyVolume': sub_df['capplc']/1000,
                'VolumeComment': ''
            }
            daily_volume_list.append(pd.DataFrame(daily_volume_data))

            daily_volume_data = {
                'FlowDate': sub_df['Datetime'].dt.strftime('%Y-%m-%d'),
                'EDCName': edc_name,
                'CustomerClass': customer_class,
                'VolumeType': 'NSPL_Unscaled',
                'EGS_DailyVolume': sub_df['trnplc']/1000 if supplier_type == 'C' else 0,
                'Default_DailyVolume': sub_df['trnplc']/1000 if supplier_type == 'X' else 0,
                'Eligible_DailyVolume': sub_df['trnplc']/1000,
                'VolumeComment': ''
            }
            daily_volume_list.append(pd.DataFrame(daily_volume_data))

            customer_count_data = {
                'FlowMonth': sub_df['Datetime'].dt.strftime('%Y-%m-%d'),
                'EDCName': edc_name,
                'CustomerClass': customer_class,
                'VolumeType': 'CustomerCount',
                'EGS_MonthlyVolume': sub_df['count'] if supplier_type == 'C' else 0,
                'Default_MonthlyVolume': sub_df['count'] if supplier_type == 'X' else 0,
                'Eligible_MonthlyVolume': sub_df['count'],
                'VolumeComment': ''
            }
            customer_count_list.append(pd.DataFrame(customer_count_data))

    other_df = grouped_df[grouped_df['Type'] == 'OTH']
    daily_volume_data = {
        'FlowDate': other_df['Datetime'].dt.strftime('%Y-%m-%d'),
        'EDCName': edc_name,
        'CustomerClass': 'OTH',
        'VolumeType': 'PLC_Unscaled',
        'EGS_DailyVolume': other_df['capplc'] / 1000,
        'Default_DailyVolume': 0,
        'Eligible_DailyVolume': other_df['capplc'] / 1000,
        'VolumeComment': ''
    }
    daily_volume_list.append(pd.DataFrame(daily_volume_data))

    daily_volume_data = {
        'FlowDate': other_df['Datetime'].dt.strftime('%Y-%m-%d'),
        'EDCName': edc_name,
        'CustomerClass': 'OTH',
        'VolumeType': 'NSPL_Unscaled',
        'EGS_DailyVolume': other_df['trnplc'] / 1000,
        'Default_DailyVolume': 0,
        'Eligible_DailyVolume': other_df['trnplc'] / 1000,
        'VolumeComment': ''
    }
    daily_volume_list.append(pd.DataFrame(daily_volume_data))

    customer_count_data = {
        'FlowMonth': other_df['Datetime'].dt.strftime('%Y-%m-%d'),
        'EDCName': edc_name,
        'CustomerClass': 'OTH',
        'VolumeType': 'CustomerCount',
        'EGS_MonthlyVolume': other_df['count'],
        'Default_MonthlyVolume': 0,
        'Eligible_MonthlyVolume': other_df['count'],
        'VolumeComment': ''
    }
    customer_count_list.append(pd.DataFrame(customer_count_data))

    # Concat and process daily_volume DataFrame
    daily_volume_processed = pd.concat(daily_volume_list, ignore_index=True)
    daily_volume_processed = daily_volume_processed.groupby(['FlowDate', 'EDCName', 'CustomerClass', 'VolumeComment', 'VolumeType'], as_index=False).sum()
    daily_volume_processed = daily_volume_processed.sort_values(by=['FlowDate', 'CustomerClass', 'VolumeType'], ignore_index=True)
    daily_volume_processed = daily_volume_processed[['FlowDate', 'EDCName', 'CustomerClass', 'VolumeType', 'EGS_DailyVolume', 'Default_DailyVolume', 'Eligible_DailyVolume', 'VolumeComment']]

    # Concat and process customer_count DataFrame
    monthly_volume_processed = pd.concat(customer_count_list, ignore_index=True)
    monthly_volume_processed = monthly_volume_processed.groupby(['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeComment', 'VolumeType'], as_index=False).sum()
    monthly_volume_processed = monthly_volume_processed.copy()
    monthly_volume_processed['FlowMonth'] = pd.to_datetime(monthly_volume_processed['FlowMonth']).dt.strftime('%Y-%m-01')
    monthly_volume_processed = monthly_volume_processed.groupby(['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeComment', 'VolumeType'], as_index=False).mean()
    monthly_volume_processed = monthly_volume_processed.sort_values(by=['FlowMonth', 'CustomerClass', 'VolumeType'], ignore_index=True)
    monthly_volume_processed = monthly_volume_processed[['FlowMonth', 'EDCName', 'CustomerClass', 'VolumeType', 'EGS_MonthlyVolume', 'Default_MonthlyVolume', 'Eligible_MonthlyVolume', 'VolumeComment']]
    monthly_volume_processed = monthly_volume_processed.copy()
    # Round customer count to integer
    for column in ['EGS_MonthlyVolume', 'Default_MonthlyVolume']:
        monthly_volume_processed[column] = monthly_volume_processed[column].round(0)
    monthly_volume_processed['Eligible_MonthlyVolume'] = monthly_volume_processed['EGS_MonthlyVolume'] + monthly_volume_processed['Default_MonthlyVolume']

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

    for volume_type in df['VolumeType'].unique():
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

    # File paths
    hourly_volume_file_path = f'{base_path}/POLRHourlyLoads202404.xlsx'
    daily_volume_file_path = f'{base_path}/SOSPIP20240630.xlsx'

    # Historical Data Paths
    hourly_volume_file_path_hist22 = f'{base_path}/historical_data/POLRHourlyLoads202201.xlsx'
    hourly_volume_file_path_hist19 = f'{base_path}/historical_data/POLRHourlyLoads201907.xlsx'


    # Load data
    print('Loading data...')
    hourly_volume_df = load_hourly_volume_data(hourly_volume_file_path, 'Jan-23', 'Apr-24')# Table Jan-23 contains all data between Jan-20 and Jan-23
    hourly_volume_df_hist22 = load_hourly_volume_data(hourly_volume_file_path_hist22, 'Jan-17', 'Dec-19')
    hourly_volume_df_hist19 = load_hourly_volume_data(hourly_volume_file_path_hist19, 'Jan-13', 'Dec-16')
    hourly_volume_df = pd.concat([hourly_volume_df_hist19, hourly_volume_df_hist22, hourly_volume_df], ignore_index=True)
    daily_volume_df = load_daily_volume_data(daily_volume_file_path)

    # Process data
    print('Processing data...')
    edc_name = "MD_BGE"
    hourly_volume_processed = process_hourly_volume(hourly_volume_df, edc_name)
    daily_volume_processed, monthly_volume_processed = process_daily_volume(daily_volume_df, edc_name)

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
        check_continuity(hourly_volume_processed[hourly_volume_processed['CustomerClass'] == customer_class],
                         'Datetime_beginning_utc', 'H', f'{customer_class} hourly volume')

    customer_class_list = daily_volume_processed['CustomerClass'].unique()
    for customer_class in customer_class_list:
        for volume_type in ['NSPL_Unscaled', 'PLC_Unscaled']:
            check_continuity(daily_volume_processed[(daily_volume_processed['CustomerClass'] == customer_class) & (daily_volume_processed['VolumeType'] == volume_type)], 'FlowDate', 'D', f'{customer_class} {volume_type} daily volume')

    # Save processed data
    print('Saving data...')
    save_processed_data(hourly_volume_processed, hourly_output_path, 'hourly')
    save_processed_data(monthly_volume_processed, monthly_output_path, 'monthly')
    save_processed_data(daily_volume_processed, daily_output_path, 'daily')

if __name__ == "__main__":
    base_path = 'C:\\Users\\5DIntern3_2024\\Work\\BGE_MD'
    main(base_path)

