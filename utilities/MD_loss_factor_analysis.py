import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from jinja2 import Environment, FileSystemLoader
import base64
import datetime



def plot_hourly_data(df, output_dir, loss_factor_dict):
    data = df.copy()

    plot_path = {}
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    columns_to_plot = ['EGS_CalculatedLoss', 'Default_CalculatedLoss', 'Eligible_CalculatedLoss']
    customer_classes = data['CustomerClass'].unique()

    # Set larger font size for all text elements
    plt.rcParams.update({'font.size': 14})

    for customer_class in customer_classes:
        loss_factor_value = loss_factor_dict[customer_class]
        fig, axs = plt.subplots(len(columns_to_plot), 1, figsize=(20, 20), sharex=False)

        class_data = data[data['CustomerClass'] == customer_class]

        for col_idx, column in enumerate(columns_to_plot):
            axs[col_idx].plot(pd.to_datetime(class_data['Datetime_beginning_utc']), class_data[column])
            axs[col_idx].set_title(f'{column} - {customer_class}', fontsize=18)
            axs[col_idx].set_ylabel('Loss Factor', fontsize=16)

            # Add horizontal line
            axs[col_idx].axhline(y=loss_factor_value, color='red', linestyle='--', linewidth=2)

            # Add label beside the horizontal line
            axs[col_idx].text(
                pd.to_datetime(class_data['Datetime_beginning_utc']).iloc[-1],  # Position near the end of the plot
                loss_factor_value,  # Vertical position of the label
                f'Theoretical Loss Factor :{loss_factor_value}',  # The label text
                color='red',
                fontsize=14,
                verticalalignment='bottom',  # Align the text just above the line
                horizontalalignment='left'  # Align the text to the left of the specified x position
            )

            axs[col_idx].xaxis.set_major_locator(mdates.MonthLocator(bymonth=6))
            axs[col_idx].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            axs[col_idx].xaxis.set_visible(True)

            if col_idx == len(columns_to_plot) - 1:
                axs[col_idx].set_xlabel('Datetime_beginning_utc', fontsize=16)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/Loss_Factor_{customer_class}_plot.png')
        plot_path[f'LossFactor_{customer_class}'] = f'{output_dir}/Loss_Factor_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path

def plot_UFE_data(df, output_dir):
    data = df.copy()

    plot_path = {}
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    columns_to_plot = ['EGS_UFE', 'Default_UFE', 'Eligible_UFE']
    customer_classes = data['CustomerClass'].unique()

    # Set larger font size for all text elements
    plt.rcParams.update({'font.size': 14})

    for customer_class in customer_classes:
        fig, axs = plt.subplots(len(columns_to_plot), 1, figsize=(20, 20), sharex=False)

        class_data = data[data['CustomerClass'] == customer_class]

        for col_idx, column in enumerate(columns_to_plot):
            axs[col_idx].plot(pd.to_datetime(class_data['Datetime_beginning_utc']), class_data[column])
            axs[col_idx].set_title(f'{column} - {customer_class}', fontsize=18)
            axs[col_idx].set_ylabel('UFE Volume (MW)', fontsize=16)

            axs[col_idx].xaxis.set_major_locator(mdates.MonthLocator(bymonth=6))
            axs[col_idx].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            axs[col_idx].xaxis.set_visible(True)

            if col_idx == len(columns_to_plot) - 1:
                axs[col_idx].set_xlabel('Datetime_beginning_utc', fontsize=16)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/UFE_{customer_class}_plot.png')
        plot_path[f'UFE_{customer_class}'] = f'{output_dir}/UFE_{customer_class}_plot.png'
        plt.close(fig)

    return plot_path


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
            'dataframe': format_dataframe(report_keystats_table[data_type].round(5)).to_html(index=True),
            'plot': encode_image_to_base64(plot_path[data_type])
        })

    date_today = datetime.datetime.today().strftime('%Y-%m-%d')

    # Data for the template
    report_data = {
        'report_title': f'{edc_name} Loss Factor Report',
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
    with open(f'{etl_report_output_path}/{edc_name}_loss_factor_report.html', 'w') as f:
        f.write(rendered_html)

    print(f'Report saved to {etl_report_output_path}/{edc_name}_loss_factor_report.html')


df_dpl = pd.read_excel('C://Users//5DIntern3_2024\Work\DPL_MD\output_data\MD_DPL_HourlyVolume_processed.xlsx', header=0)
df_pepco = pd.read_excel('C://Users//5DIntern3_2024\Work\PEPCO_MD\output_data\MD_PEPCO_HourlyVolume_processed.xlsx', header=0)
df_dpl_de = pd.read_excel(r'C:\Users\5DIntern3_2024\Work\DPL_DE\output_data\DE_DPL_HourlyVolume_processed.xlsx', header=0)
for edc in ['MD_DPL', 'MD_PEPCO', 'DE_DPL']:
    if edc == 'MD_DPL':
        df = df_dpl.copy()
        edc_name = 'MD_DPL'
        loss_factor_dict={
            'RES': 1.07820,
            'Type 1 Non-RES': 1.07820,
            'Type 2 Non-RES': 1.07820,#1.05669
        }
    elif edc == 'MD_PEPCO':
        df = df_pepco.copy()
        edc_name = 'MD_PEPCO'
        loss_factor_dict={
            'RES': 1.0572,
            'Type 1 Non-RES': 1.0572,
            'Type 2 Non-RES': 1.0572
        }
    else:
        df = df_dpl_de.copy()
        edc_name = 'DE_DPL'
        loss_factor_dict={
            'RSCI': 1.07438,
            'MGS': 1.07438,
            'LGS': 1.07438,
            'GSP': 1.04532
        }

    df_premise = df[df['VolumeType'] == 'Retail_Premise'].copy()
    df_wholesale = df[df['VolumeType'] == 'Wholesale_Derated'].copy()
    df_merged = df_wholesale.merge(df_premise, how='outer', on=['Datetime_beginning_utc', 'CustomerClass'], indicator=True, suffixes=('_wholesale', '_premise'))

    df_merged['EGS_CalculatedLoss'] = df_merged['EGS_HourlyVolume_wholesale']/df_merged['EGS_HourlyVolume_premise']
    df_merged['Default_CalculatedLoss'] = df_merged['Default_HourlyVolume_wholesale']/df_merged['Default_HourlyVolume_premise']
    df_merged['Eligible_CalculatedLoss'] = df_merged['Eligible_HourlyVolume_wholesale']/df_merged['Eligible_HourlyVolume_premise']

    # UFE Analysis
    res_customer_class = 'RSCI' if edc == 'DE_DPL' else 'RES'
    for volume_type in ['EGS', 'Default', 'Eligible']:
        df_merged[f'{volume_type}_UFE'] = df_merged[f'{volume_type}_HourlyVolume_premise']*(df_merged[f'{volume_type}_CalculatedLoss'] - loss_factor_dict[res_customer_class])

    plot_output_path = f'C://Users//5DIntern3_2024\Work\MD_DPL_&_MD_PEPCO_Loss_factor\output_{edc_name}'

    report_keystats_table = {}
    for customer_classes in df_merged['CustomerClass'].unique():
        report_keystats_table.update({f'LossFactor_{customer_classes}':df_merged[df_merged['CustomerClass'] == customer_classes][['EGS_CalculatedLoss', 'Default_CalculatedLoss', 'Eligible_CalculatedLoss']].describe().T})
        report_keystats_table.update({f'UFE_{customer_classes}':df_merged[df_merged['CustomerClass'] == customer_classes][['EGS_UFE', 'Default_UFE', 'Eligible_UFE']].describe().T})

    plot_path = plot_hourly_data(df_merged, plot_output_path, loss_factor_dict)
    plot_path.update(plot_UFE_data(df_merged, plot_output_path))
    generate_report(plot_output_path, report_keystats_table, plot_path, edc_name)










