from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
import pandas as pd
import re
import html

# Load environment variables
load_dotenv()

# Default arguments untuk DAG
default_args = {
    'owner': 'Plantopia',
    'start_date': days_ago(1),  # Menentukan tanggal mulai
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definisi DAG
dag = DAG(
    'DE-ETL_Plantopia',
    default_args=default_args,
    description='Automation ETL Process for Eficiency Process',
    schedule_interval='0 */12 * * *',  # Setiap 12 jam sekali
)

def get_connection():
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

def get_all_tables(engine):
    inspector = inspect(engine)
    return inspector.get_table_names()

def table_to_dataframe(engine, table_name):
    with engine.connect() as connection:
        query = f"SELECT * FROM {table_name}"
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

def remove_html_tags(text):
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    text = html.unescape(text)
    return text

def cleanse_dataframe(df):
    print("Memeriksa missing values...")
    
    # Mengisi missing values berdasarkan tipe data kolom
    for col in df.columns:
        if df[col].isnull().any():
            if df[col].dtype == 'int64' or df[col].dtype == 'float64':
                print(f"Mengisi missing values di kolom '{col}' dengan 0...")
                df[col].fillna(0, inplace=True)
            elif df[col].dtype == 'object' or df[col].dtype.name == 'category':
                print(f"Mengisi missing values di kolom '{col}' dengan '-'...")
                df[col].fillna('-', inplace=True)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                print(f"Mengisi missing values di kolom '{col}' dengan tanggal hari ini...")
                df[col].fillna(pd.Timestamp('today'), inplace=True)
    
    for col in df.columns:
        if df[col].dtype == 'object' or df[col].dtype.name == 'category':
            print(f"Membersihkan tag HTML di kolom '{col}'...")
            df[col] = df[col].apply(lambda x: remove_html_tags(x) if pd.notnull(x) else x)
    
    print("Memeriksa duplikasi...")
    duplicate_rows = df.duplicated().sum()
    print(f"Jumlah baris duplikat: {duplicate_rows}")
    
    if duplicate_rows > 0:
        print("Menghapus baris duplikat...")
        df.drop_duplicates(inplace=True)

    return df

def cleanse_dataframe_fact(df):
    print("Memeriksa missing values...")
    for col in df.columns:
        if df[col].isnull().any():
            if df[col].dtype == 'int64' or df[col].dtype == 'float64':
                print(f"Mengisi missing values di kolom '{col}' dengan -1...")
                df[col].fillna(-1, inplace=True)
    return df

def change_type_data(df):
    # Memastikan tipe data yang benar
    print(f"Memastikan tipe data yang benar...")
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].astype('category')
        elif df[column].dtype == 'float64':
            df[column] = df[column].astype('int64')
    
    # Pastikan kolom tanggal diubah menjadi datetime dengan format '%Y-%m-%d %H:%M'
    date_columns = ['created_at', 'updated_at', 'last_watered_at']
    for column in date_columns:
        if column in df.columns:
            print(f"Mengonversi kolom '{column}' menjadi datetime dengan format '%Y-%m-%d %H:%M:%S'...")
            df[column] = pd.to_datetime(df[column], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    
    # Isi nilai NULL di kolom created_at jika ada
    if 'created_at' in df.columns:
        df['created_at'].fillna(datetime.now(), inplace=True)  # Isi dengan tanggal dan waktu saat ini
    if 'updated_at' in df.columns:
        df['updated_at'].fillna(datetime.now(), inplace=True)  # Isi dengan tanggal dan waktu saat ini

    return df

def extract_task():
    engine = get_connection()
    tables = get_all_tables(engine)
    output_dir = "/home/newrey/airflow/Capstone-Project-Plantopia/data_source_csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for table in tables:
        df = table_to_dataframe(engine, table)
        df = cleanse_dataframe(df)
        df = change_type_data(df)
        csv_filename = os.path.join(output_dir, f"{table}.csv")
        df.to_csv(csv_filename, index=False)
        print(f"Saved DataFrame from table {table} to {csv_filename}")

    # Dimensional tables creation
    output_dim_dir = "/home/newrey/airflow/Capstone-Project-Plantopia/data_source_dimensional"
    if not os.path.exists(output_dim_dir):
        os.makedirs(output_dim_dir)
    
    for table in tables:
        df_variable_name = f"df_{table}"
        csv_filename = os.path.join(output_dim_dir, f"dim_{table}.csv")
        globals()[df_variable_name] = pd.read_csv(os.path.join(output_dir, f"{table}.csv"))
        globals()[df_variable_name].to_csv(csv_filename, index=False)
        print(f"Saved DataFrame Dimensional from table {table} to {csv_filename}")
        
def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def transform_task():
    # Define directory paths
    output_dir = "/home/newrey/airflow/Capstone-Project-Plantopia/data_source_csv"
    dim_dir = "/home/newrey/airflow/Capstone-Project-Plantopia/data_source_dimensional"
    final_dir = "/home/newrey/airflow/Capstone-Project-Plantopia/data_source_to_load"

    # Create directories if they do not exist
    create_directory_if_not_exists(output_dir)
    create_directory_if_not_exists(dim_dir)
    create_directory_if_not_exists(final_dir)

    # Loading dimensional dataframes
    dataframe_dimensional = []
    for filename in os.listdir(dim_dir):
        if filename.endswith('.csv'):
            table_name = os.path.splitext(filename)[0]
            df_variable_name = f"df_{table_name}"
            df_path = os.path.join(dim_dir, filename)
            globals()[df_variable_name] = pd.read_csv(df_path)
            dataframe_dimensional.append(df_variable_name)

    # Cleansing dimensional dataframes
    for dataframe in dataframe_dimensional:
        df_variable_name = f"{dataframe}"
        globals()[df_variable_name] = cleanse_dataframe(globals()[df_variable_name])

    # Rename columns for merging
    df_dim_admins = globals()['df_dim_admins']
    df_dim_admins.rename(columns={'id': 'admin_id', 'name': 'admin_name'}, inplace=True)
    df_dim_admins.to_csv(os.path.join(final_dir, 'dim_admins.csv'), index=False)
    
    df_dim_users = globals()['df_dim_users']
    df_dim_users.rename(columns={'id': 'user_id', 'name': 'user_name'}, inplace=True)
    df_dim_users.to_csv(os.path.join(final_dir, 'dim_users.csv'), index=False)

    df_dim_plants = globals()['df_dim_plants']
    df_dim_plants.rename(columns={'id': 'plant_id', 'name': 'plant_name'}, inplace=True)
    df_dim_plants['plant_name'] = df_dim_plants['plant_name'].str.split(' -').str[0].str.title()
    df_dim_plants['plant_name'] = df_dim_plants['plant_name'].str.split('-').str[0].str.title()

    # Merge tables to create fact table user activities
    df_dim_user_plants = globals()['df_dim_user_plants']
    df_dim_my_plants = df_dim_user_plants.merge(df_dim_users, on='user_id', how='left', suffixes=('', '_user'))
    df_dim_my_plants = df_dim_my_plants.merge(df_dim_plants, on='plant_id', how='left', suffixes=('', '_plant'))
    df_dim_my_plants = df_dim_my_plants[['id', 'user_name', 'plant_name', 'created_at', 'updated_at', 'last_watered_at']]
    df_dim_my_plants.rename(columns={'id': 'my_plant_id'}, inplace=True)
    df_dim_my_plants.to_csv(os.path.join(final_dir, 'dim_my_plants.csv'), index=False)

    # Merging and creating final fact table
    df_dim_user_plant_histories = globals()['df_dim_user_plant_histories']
    df_dim_user_plant_histories = df_dim_user_plant_histories.merge(df_dim_users, on='user_id', how='left', suffixes=('', '_user'))
    df_dim_user_plant_histories = df_dim_user_plant_histories[['id', 'user_name', 'plant_name', 'plant_category', 'created_at', 'updated_at']]
    df_dim_user_plant_histories.rename(columns={'id': 'planting_history_id'}, inplace=True)
    df_dim_user_plant_histories['plant_name'] = df_dim_user_plant_histories['plant_name'].str.split(' -').str[0].str.title()
    df_dim_user_plant_histories['plant_name'] = df_dim_user_plant_histories['plant_name'].str.split('-').str[0].str.title()
    df_dim_user_plant_histories.to_csv(os.path.join(final_dir, 'dim_planting_histories.csv'), index=False)

    df_dim_watering_histories = globals()['df_dim_watering_histories']
    df_dim_watering_histories = df_dim_watering_histories.merge(df_dim_users, on='user_id', how='left', suffixes=('', '_user')).merge(
        df_dim_plants, on='plant_id', how='left', suffixes=('', '_plant'))
    df_dim_watering_histories = df_dim_watering_histories[['id', 'user_name', 'plant_name', 'created_at', 'updated_at']]
    df_dim_watering_histories.rename(columns={'id': 'watering_history_id'}, inplace=True)
    df_dim_watering_histories['plant_name'] = df_dim_watering_histories['plant_name'].str.split(' -').str[0].str.title()
    df_dim_watering_histories['plant_name'] = df_dim_watering_histories['plant_name'].str.split('-').str[0].str.title()
    df_dim_watering_histories.to_csv(os.path.join(final_dir, 'dim_watering_histories.csv'), index=False)

    df_dim_customize_watering_reminders = globals()['df_dim_customize_watering_reminders']
    df_dim_customize_watering_reminders.rename(columns={'id': 'customize_watering_reminder_id'}, inplace=True)
    df_dim_customize_watering_reminders.to_csv(os.path.join(final_dir, 'dim_customize_watering_reminders.csv'), index=False)

    df_fact_user_activities = pd.merge(df_dim_my_plants, df_dim_user_plant_histories,
                                       on=["user_name", "plant_name"], how='outer',
                                       suffixes=('_my_plants', '_planting'))
    df_fact_user_activities = pd.merge(df_fact_user_activities, df_dim_watering_histories,
                                       on=["user_name", "plant_name"], how='outer',
                                       suffixes=('_fact', '_watering'))
    df_fact_user_activities = df_fact_user_activities[['my_plant_id', 'planting_history_id', 'watering_history_id']]
    df_fact_user_activities['watering_count'] = df_fact_user_activities['watering_history_id'].nunique()
    df_fact_user_activities['planting_count'] = df_fact_user_activities['planting_history_id'].nunique()
    df_fact_user_activities['user_plant_count'] = df_fact_user_activities['my_plant_id'].nunique()
    
    cleanse_dataframe_fact(df_fact_user_activities)
    change_type_data(df_fact_user_activities)
    df_fact_user_activities.to_csv(os.path.join(final_dir, 'fact_user_activities.csv'), index=False)

    # Creating final fact table plants data
    df_dim_plant_categories = globals()['df_dim_plant_categories']
    df_dim_plant_categories.rename(columns={'id': 'plant_category_id', 'name': 'plant_category'}, inplace=True)
    df_dim_plants = df_dim_plants.merge(df_dim_plant_categories, on='plant_category_id', how='left', suffixes=('', '_category'))
    df_dim_plants = df_dim_plants[['plant_id', 'plant_name', 'description', 'is_toxic', 'harvest_duration',
                                   'sunlight', 'planting_time', 'plant_category', 'climate_condition',
                                   'additional_tips', 'created_at', 'updated_at']]
    df_dim_plants.to_csv(os.path.join(final_dir, 'dim_plants.csv'), index=False)

    df_dim_plant_reminders = globals()['df_dim_plant_reminders']
    df_dim_plant_reminders = df_dim_plant_reminders[['id', 'plant_id', 'watering_frequency', 'each', 'watering_amount',
                                                     'unit', 'watering_time', 'weather_condition', 'condition_description',
                                                     'created_at', 'updated_at']]
    df_dim_plant_reminders.rename(columns={'id': 'watering_reminders_id'}, inplace=True)
    df_dim_watering_reminders = df_dim_plant_reminders

    df_dim_plant_faqs = globals()['df_dim_plant_faqs']
    df_dim_plant_faqs.rename(columns={'id': 'plant_faqs_id'}, inplace=True)

    df_dim_plant_instructions = globals()['df_dim_plant_instructions']
    df_dim_plant_instructions.rename(columns={'id': 'plant_instruction_id'}, inplace=True)
    df_dim_plant_instruction_categories = globals()['df_dim_plant_instruction_categories']
    df_dim_plant_instruction_categories.rename(columns={'id': 'instruction_category_id'}, inplace=True)
    df_dim_plant_instructions = df_dim_plant_instructions.merge(
        df_dim_plant_instruction_categories, on='instruction_category_id', how='left', suffixes=('', '_category'))
    df_dim_plant_instructions = df_dim_plant_instructions[['plant_instruction_id', 'name', 'plant_id', 'step_number', 'step_title',
                                                           'step_description', 'step_image_url', 'additional_tips', 'created_at',
                                                           'updated_at']]
    df_dim_plant_instructions.rename(columns={'name': 'name_instruction_categories'}, inplace=True)

    df_dim_plant_characteristics = globals()['df_dim_plant_characteristics']
    df_dim_plant_characteristics.rename(columns={'id': 'plant_characteristic_id'}, inplace=True)

    # Merging to final fact table plants data
    df_fact_plants_data = pd.merge(df_dim_watering_reminders, df_dim_plants, left_on="plant_id", right_on="plant_id", suffixes=('_watering_reminders', '_plant'))
    df_fact_plants_data = df_fact_plants_data[['plant_id', 'watering_reminders_id']]
    
    df_dim_plant_reminders = df_dim_plant_reminders[['watering_reminders_id', 'watering_frequency', 'each', 'watering_amount',
       'unit', 'watering_time', 'weather_condition', 'condition_description',
       'created_at', 'updated_at']]
    df_dim_plant_reminders.to_csv(os.path.join(final_dir, 'dim_watering_reminders.csv'), index=False)
    
    df_fact_plants_data = pd.merge(df_dim_plant_faqs, df_fact_plants_data, left_on="plant_id", right_on="plant_id", suffixes=('_faqs', '_fact'))
    df_fact_plants_data = df_fact_plants_data[['plant_id', 'plant_faqs_id', 'watering_reminders_id']]
    
    df_dim_plant_faqs = df_dim_plant_faqs[['plant_faqs_id', 'question', 'answer', 'created_at', 'updated_at']]
    df_dim_plant_faqs.to_csv(os.path.join(final_dir, 'dim_plant_faqs.csv'), index=False)
    
    df_fact_plants_data = pd.merge(df_dim_plant_instructions, df_fact_plants_data, left_on="plant_id", right_on="plant_id", suffixes=('_instructions', '_fact'))
    df_fact_plants_data = df_fact_plants_data[['plant_id', 'plant_faqs_id', 'plant_instruction_id', 'watering_reminders_id']]
    
    df_dim_plant_instructions = df_dim_plant_instructions[['plant_instruction_id', 'name_instruction_categories', 'step_number', 'step_title',
       'step_description', 'step_image_url', 'additional_tips', 'created_at',
       'updated_at']]
    df_dim_plant_instructions.to_csv(os.path.join(final_dir, 'dim_plant_instructions.csv'), index=False)
    
    df_fact_plants_data = pd.merge(df_dim_plant_characteristics, df_fact_plants_data, left_on="plant_id", right_on="plant_id", suffixes=('_characteristics', '_fact'))
    
    df_dim_plant_characteristics = df_dim_plant_characteristics[['plant_characteristic_id', 'height', 'height_unit', 'wide',
                                                                 'wide_unit', 'leaf_color']]
    df_dim_plant_characteristics.to_csv(os.path.join(final_dir, 'dim_plant_characteristics.csv'), index=False)
    
    df_fact_plants_data = df_fact_plants_data[['plant_id', 'plant_faqs_id', 'plant_characteristic_id', 'plant_instruction_id', 'watering_reminders_id']]
    df_fact_plants_data['total_plants'] = df_fact_plants_data['plant_id'].nunique()
    df_fact_plants_data.to_csv(os.path.join(final_dir, 'fact_plants_data.csv'), index=False)

def load_to_bigquery(csv_file_path, table_id, **kwargs):
    # Get environment variables
    project_id = os.getenv('PROJECT_ID')
    dataset_id = os.getenv('DATASET_ID')
    service_acc = os.getenv('SERVICE_ACCOUNT')

    # Set service account environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_acc

    # Initialize BigQuery client
    credentials = service_account.Credentials.from_service_account_file(service_acc)
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file_path)

    # Initialize an empty schema list
    schema = []

    # Convert created_at and updated_at columns to datetime if they exist
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
        schema.append(bigquery.SchemaField("created_at", "TIMESTAMP"))
    if 'updated_at' in df.columns:
        df['updated_at'] = pd.to_datetime(df['updated_at'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
        schema.append(bigquery.SchemaField("updated_at", "TIMESTAMP"))

    # Add other schema fields as necessary
    for column in df.columns:
        if column not in ['created_at', 'updated_at']:
            dtype = df[column].dtype
            if pd.api.types.is_integer_dtype(dtype):
                schema.append(bigquery.SchemaField(column, "INT64"))
            elif pd.api.types.is_float_dtype(dtype):
                schema.append(bigquery.SchemaField(column, "FLOAT64"))
            elif pd.api.types.is_bool_dtype(dtype):
                schema.append(bigquery.SchemaField(column, "BOOL"))
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                schema.append(bigquery.SchemaField(column, "TIMESTAMP"))
            else:
                schema.append(bigquery.SchemaField(column, "STRING"))

    # Define full table ID: project_id.dataset_id.table_id
    table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Load DataFrame into BigQuery table with schema if it exists
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    if schema:
        job_config.schema = schema

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    # Wait for job to complete
    job.result()

    print(f"Loaded {len(df)} rows into {table_id}.")

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    dag=dag,
)

data_source_dir = '/home/newrey/airflow/Capstone-Project-Plantopia/data_source_to_load'

# List to hold tasks for loading CSV files to BigQuery
load_tasks = []

# Chain tasks together: extract_task >> transform_task
extract_task >> transform_task

# Iterate over CSV files in data source directory
for csv_file in os.listdir(data_source_dir):
    if csv_file.endswith('.csv'):
        csv_file_path = os.path.join(data_source_dir, csv_file)
        table_name = os.path.splitext(csv_file)[0]  # Table name taken from file name without extension

        # Task: load_{table_name}_to_bigquery
        load_task = PythonOperator(
            task_id=f'load_{table_name}_to_bigquery',
            python_callable=load_to_bigquery,
            op_kwargs={'csv_file_path': csv_file_path, 'table_id': table_name},
            provide_context=True,
            dag=dag,
        )

        # Chain tasks together: transform_task >> load_{table_name}_to_bigquery
        transform_task >> load_task

        # Add load_task to load_tasks list
        load_tasks.append(load_task)