# %%
"""
# 1. Import Library yang dibutuhkan
"""

# %%
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
import re
import html
from google.cloud import bigquery
from google.oauth2 import service_account

# %%
"""
## 1.1 Connect to Database Backend
"""

# %%
load_dotenv()

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

def get_connection():
    return create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    )

def get_all_tables(engine):
    inspector = inspect(engine)
    return inspector.get_table_names()

def table_to_dataframe(engine, table_name):
    with engine.connect() as connection:
        query = f"SELECT * FROM {table_name}"
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

engine = get_connection()
print(f"Koneksi ke {host} untuk user {user} sukses dibuat!.")

# %%
"""
# 2. Extract
"""

# %%
"""
## 2.1 Convert Tables to Dataframe
"""

# %%
tables = get_all_tables(engine)
print(f"Tables in the database: {tables}")

# %%
"""
### 2.1.1 All Dataframe
"""

# %%
output_dir = "../data_source_csv"

for table in tables:
    df_variable_name = f"df_{table}"
    globals()[df_variable_name] = table_to_dataframe(engine, table)
    print(f"Menampilkan Dataframe dari tabel: {table}")
    display(globals()[df_variable_name])
    
    csv_filename = os.path.join(output_dir, f"{table}.csv")
    globals()[df_variable_name].to_csv(csv_filename, index=False)
    print(f"Menyimpan Dataframe dari tabel {table} ke {csv_filename} \n")

# %%
"""
# 3. Transform
"""

# %%
"""
## 3.1 Cleaning Data
"""

# %%
def remove_html_tags(text):
    """Fungsi untuk menghapus tag HTML dan entitas HTML dari teks."""
    # Menghapus tag HTML
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    # Menghapus entitas HTML
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
    
    print("Membersihkan tag HTML dari kolom teks...")
    # Membersihkan tag HTML dan entitas HTML dari kolom teks (object atau category)
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
    
    # Pastikan tipe data yang benar
    df = change_type_data(df)
    
    return df

# %%
def cleanse_dataframe_fact(df):
    print("Memeriksa missing values...")
    
    # Mengisi missing values berdasarkan tipe data kolom
    for col in df.columns:
        if df[col].isnull().any():
            if df[col].dtype == 'int64' or df[col].dtype == 'float64':
                print(f"Mengisi missing values di kolom '{col}' dengan -1...")
                df[col].fillna(-1, inplace=True)
    
    return df

# %%
for table in tables:
    df_variable_name = f"df_{table}"
    
    # Cleansing DataFrame
    globals()[df_variable_name] = cleanse_dataframe(globals()[df_variable_name])

# %%
"""
## 3.2 Informasi Dataframe
"""

# %%
def info_dataframe(df):
    print("Menampilkan Informasi Field di tiap Dataframe")
    info_dataframe = df.info()
    print(info_dataframe)
    
    return df

# %%
for table in tables:
    df_variable_name = f"df_{table}"
    
    # Informasi DataFrame
    globals()[df_variable_name] = info_dataframe(globals()[df_variable_name])

# %%
"""
## 3.3 Change Type Data
"""

# %%
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
            print(f"Mengonversi kolom '{column}' menjadi datetime dengan format '%Y-%m-%d %H:%M'...")
            df[column] = pd.to_datetime(df[column], errors='coerce', format='%Y-%m-%d %H:%M')
    
    # Isi nilai NULL di kolom created_at jika ada
    if 'created_at' in df.columns:
        df['created_at'].fillna(datetime.now(), inplace=True)  # Isi dengan tanggal dan waktu saat ini
    if 'updated_at' in df.columns:
        df['updated_at'].fillna(datetime.now(), inplace=True)  # Isi dengan tanggal dan waktu saat ini
    
    return df

# %%
dataframes = []  # List untuk menyimpan nama dataframe

for table in tables:
    df_variable_name = f"df_{table}"
    
    # Cleansing DataFrame
    globals()[df_variable_name] = change_type_data(globals()[df_variable_name])
    
    # Tambahkan nama dataframe ke dalam list
    dataframes.append(df_variable_name)

# Output untuk verifikasi
print(dataframes)

# %%
"""
### 3.3.1 Menampilkan Informasi Dataframe Kembali
"""

# %%
for table in dataframes:
    df_variable_name = f"{table}"
    
    # Informasi DataFrame
    globals()[df_variable_name] = info_dataframe(globals()[df_variable_name])

# %%
"""
### 3.3.2 Sample Dataframe
"""

# %%
df_watering_histories

# %%
"""
## 3.4 Table Fakta &  Dimensional
"""

# %%
"""
### 3.4.1 Save to CSV Table Dimensional
"""

# %%
dataframes

# %%
output_dir = "../data_source_dimensional"

for table in tables:
    df_variable_name = f"df_{table}"
    
    csv_filename = os.path.join(output_dir, f"dim_{table}.csv")
    globals()[df_variable_name].to_csv(csv_filename, index=False)
    print(f"Menyimpan Dataframe Dimensional dari dataframe df_{table} ke {csv_filename} \n")

# %%
"""
### 3.4.2 Dataframe Dimensional
"""

# %%
# Path ke direktori yang berisi file CSV
input_dir = '../data_source_dimensional'

# List untuk menyimpan nama dataframe yang dibuat
dataframe_dimensional = []

# Loop untuk membaca semua file CSV dalam direktori
for filename in os.listdir(input_dir):
    if filename.endswith('.csv'):
        # Mengambil nama tabel dari nama file
        table_name = os.path.splitext(filename)[0]  # Menghilangkan ekstensi .csv
        
        # Membuat nama variabel dataframe
        df_variable_name = f"df_{table_name}"
        
        # Membaca file CSV menjadi dataframe
        df_path = os.path.join(input_dir, filename)
        globals()[df_variable_name] = pd.read_csv(df_path)
        
        # Tambahkan nama dataframe ke dalam list
        dataframe_dimensional.append(df_variable_name)

# Output untuk verifikasi
print(dataframe_dimensional)

# %%
dataframe_dimensional

# %%
for dataframe in dataframe_dimensional:
    df_variable_name = f"{dataframe}"
    
    # Cleansing DataFrame
    globals()[df_variable_name] = cleanse_dataframe(globals()[df_variable_name])

# %%
df_dim_plants

# %%
"""
### 3.4.3 Merged to Fact Table User Activities
"""

# %%
"""
#### 3.4.3.1 Merged dim_my_plants
"""

# %%
df_dim_users.rename(columns={'id': 'user_id'}, inplace=True)
df_dim_users.rename(columns={'name': 'user_name'}, inplace=True)
df_dim_users

# %%
df_dim_plants.rename(columns={'id': 'plant_id'}, inplace=True)
df_dim_plants.rename(columns={'name': 'plant_name'}, inplace=True)
df_dim_plants

# %%
df_dim_user_plants

# %%
# Melakukan merge dalam satu baris kode dengan suffixes untuk menghindari konflik kolom
df_dim_my_plants = df_user_plants.merge(df_dim_users, on='user_id', how='left', suffixes=('', '_user'))

# Memeriksa nama kolom setelah merge
print(df_dim_my_plants.columns)

# %%
# Melakukan merge dalam satu baris kode dengan suffixes untuk menghindari konflik kolom
df_dim_my_plants = df_dim_my_plants.merge(df_dim_plants, on='plant_id', how='left', suffixes=('', '_plant'))

# Memeriksa nama kolom setelah merge
print(df_dim_my_plants.columns)

# %%
# Memilih kolom yang diinginkan
df_dim_my_plants = df_dim_my_plants[['id', 'user_name', 'plant_name', 'created_at', 'updated_at', 'last_watered_at']]
df_dim_my_plants

# %%
df_dim_my_plants.rename(columns={'id': 'my_plant_id'}, inplace=True)

# %%
df_dim_my_plants.info()

# %%
"""
##### a. Save to CSV
"""

# %%
df_dim_my_plants.to_csv('../data_source_to_load/dim_my_plants.csv', index=False)

# %%
"""
#### 3.4.3.2 Merged dim_planting_histories
"""

# %%
# Melakukan merge dalam satu baris kode dengan suffixes untuk menghindari konflik kolom
df_dim_user_plant_histories = df_dim_user_plant_histories.merge(
    df_dim_users, on='user_id', how='left', suffixes=('', '_user'))

# Memeriksa nama kolom setelah merge
print(df_dim_user_plant_histories.columns)

# %%
# Memilih kolom yang diinginkan
df_dim_user_plant_histories = df_dim_user_plant_histories[['id', 'user_name', 'plant_name', 'plant_category', 'created_at', 'updated_at']]
df_dim_user_plant_histories.rename(columns={'id': 'planting_history_id'}, inplace=True)
df_dim_user_plant_histories

# %%
"""
##### a. Save to CSV
"""

# %%
df_dim_user_plant_histories.to_csv('../data_source_to_load/dim_planting_histories.csv', index=False)

# %%
"""
#### 3.4.3.3 Merged dim_watering_histories
"""

# %%
# Melakukan merge dalam satu baris kode dengan suffixes untuk menghindari konflik kolom
df_dim_watering_histories = df_watering_histories.merge(df_dim_users, on='user_id', how='left', suffixes=('', '_user')).merge(
    df_dim_plants, on='plant_id', how='left', suffixes=('', '_plant'))

df_dim_watering_histories = df_dim_watering_histories[['id', 'user_name', 'plant_name', 'created_at', 'updated_at']]
df_dim_watering_histories.rename(columns={'id': 'watering_history_id'}, inplace=True)
df_dim_watering_histories

# %%
"""
##### a. Save to CSV
"""

# %%
df_dim_watering_histories.to_csv('../data_source_to_load/dim_watering_histories.csv', index=False)

# %%
"""
#### 3.4.3.4 dim_customize_watering_reminders
"""

# %%
df_dim_customize_watering_reminders.info()

# %%
df_dim_customize_watering_reminders.rename(columns={'id': 'customize_watering_reminder_id'}, inplace=True)
df_dim_customize_watering_reminders

# %%
"""
##### a. Save to CSV
"""

# %%
df_dim_customize_watering_reminders.to_csv('../data_source_to_load/dim_customize_watering_reminders.csv', index=False)

# %%
"""
#### 3.4.3.5 Merged Final Fact Table User Activities
"""

# %%
# Menggabungkan keseluruhan id dataframe kedalam Fact User Activites
df_fact_user_activities = pd.merge(df_dim_my_plants, df_dim_user_plant_histories,
                                   on=["user_name", "plant_name"], how='outer',
                                   suffixes=('_my_plants', '_planting'))

df_fact_user_activities = pd.merge(df_fact_user_activities, df_dim_watering_histories,
                                   on=["user_name", "plant_name"], how='outer',
                                   suffixes=('_fact', '_watering'))

df_fact_user_activities = df_fact_user_activities[['my_plant_id', 'planting_history_id', 'watering_history_id']]
df_fact_user_activities

# %%
df_fact_user_activities['watering_count'] = df_fact_user_activities['watering_history_id'].nunique()
df_fact_user_activities['planting_count'] = df_fact_user_activities['planting_history_id'].nunique()
df_fact_user_activities['user_plant_count'] = df_fact_user_activities['my_plant_id'].nunique()
df_fact_user_activities

# %%
"""
#### a. Change Type Data
"""

# %%
cleanse_dataframe_fact(df_fact_user_activities)
change_type_data(df_fact_user_activities)
df_fact_user_activities

# %%
"""
##### b. Save to CSV
"""

# %%
df_fact_user_activities.to_csv('../data_source_to_load/fact_user_activities.csv', index=False)

# %%
"""
### 3.4.4 Merged to Fact Table Plants Data
"""

# %%
"""
#### 3.4.4.1 Merged dim_plants
"""

# %%
df_dim_plants.info()

# %%
df_dim_plants

# %%
df_dim_plant_categories.rename(columns={'id': 'plant_category_id'}, inplace=True)
df_dim_plant_categories.rename(columns={'name': 'plant_category'}, inplace=True)
df_dim_plant_categories

# %%
# Melakukan merge dalam satu baris kode dengan suffixes untuk menghindari konflik kolom
df_dim_plants = df_dim_plants.merge(
    df_dim_plant_categories, on='plant_category_id', how='left', suffixes=('', '_category'))

# Memeriksa nama kolom setelah merge
print(df_dim_plants.columns)

# %%
df_dim_plants = df_dim_plants[['plant_id', 'plant_name', 'description', 'is_toxic', 'harvest_duration',
                            'sunlight', 'planting_time', 'plant_category', 'climate_condition',
                            'additional_tips', 'created_at', 'updated_at']]
df_dim_plants

# %%
"""
##### a. Save to CSV
"""

# %%
df_dim_plants.to_csv('../data_source_to_load/dim_plants.csv', index=False)

# %%
"""
#### 3.4.4.2 dim_watering_reminders
"""

# %%
df_dim_plant_reminders = df_dim_plant_reminders[['id', 'plant_id', 'watering_frequency', 'each', 'watering_amount',
       'unit', 'watering_time', 'weather_condition', 'condition_description',
       'created_at', 'updated_at']]
df_dim_plant_reminders

# %%
df_dim_plant_reminders.rename(columns={'id': 'watering_reminders_id'}, inplace=True)
df_dim_watering_reminders = df_dim_plant_reminders
df_dim_watering_reminders

# %%
"""
##### a. Merged to Fact Table Plants Data
"""

# %%
# Menggabungkan kedua dataframe
df_fact_plants_data = pd.merge(df_dim_watering_reminders, df_dim_plants, left_on="plant_id", right_on="plant_id", suffixes=('_watering_reminders', '_plant'))

# Menampilkan hasil gabungan
df_fact_plants_data.columns

# %%
df_fact_plants_data = df_fact_plants_data[['plant_id', 'watering_reminders_id']]
df_fact_plants_data

# %%
"""
##### b. Save to CSV
"""

# %%
df_dim_plant_reminders = df_dim_plant_reminders[['watering_reminders_id', 'watering_frequency', 'each', 'watering_amount',
       'unit', 'watering_time', 'weather_condition', 'condition_description',
       'created_at', 'updated_at']]
df_dim_plant_reminders

# %%
df_dim_plant_reminders.to_csv('../data_source_to_load/dim_watering_reminders.csv', index=False)

# %%
"""
#### 3.4.4.3 dim_plant_faqs
"""

# %%
df_dim_plant_faqs

# %%
df_dim_plant_faqs.rename(columns={'id': 'plant_faqs_id'}, inplace=True)
df_dim_plant_faqs

# %%
"""
##### a. Merged to Fact Table Plants Data
"""

# %%
# Menggabungkan kedua dataframe
df_fact_plants_data = pd.merge(df_dim_plant_faqs, df_fact_plants_data, left_on="plant_id", right_on="plant_id", suffixes=('_faqs', '_fact'))

# Menampilkan hasil gabungan
df_fact_plants_data.columns

# %%
df_fact_plants_data = df_fact_plants_data[['plant_id', 'plant_faqs_id', 'watering_reminders_id']]
df_fact_plants_data

# %%
"""
##### b. Save to CSV 
"""

# %%
df_dim_plant_faqs = df_dim_plant_faqs[['plant_faqs_id', 'question', 'answer',
       'created_at', 'updated_at']]
df_dim_plant_faqs

# %%
df_dim_plant_faqs.to_csv('../data_source_to_load/dim_plant_faqs.csv', index=False)

# %%
"""
#### 3.4.4.4 dim_plant_instructions
"""

# %%
df_dim_plant_instructions

# %%
df_dim_plant_instructions.rename(columns={'id': 'plant_instruction_id'}, inplace=True)
df_dim_plant_instructions

# %%
df_dim_plant_instruction_categories.rename(columns={'id': 'instruction_category_id'}, inplace=True)
df_dim_plant_instruction_categories

# %%
# Melakukan merge dalam satu baris kode dengan suffixes untuk menghindari konflik kolom
df_dim_plant_instructions = df_dim_plant_instructions.merge(
    df_dim_plant_instruction_categories, on='instruction_category_id', how='left', suffixes=('', '_category'))

# Memeriksa nama kolom setelah merge
print(df_dim_plant_instructions.columns)

# %%
df_dim_plant_instructions = df_dim_plant_instructions[['plant_instruction_id', 'name', 'plant_id', 'step_number', 'step_title',
       'step_description', 'step_image_url', 'additional_tips', 'created_at',
       'updated_at']]

df_dim_plant_instructions.rename(columns={'name': 'name_instruction_categories'}, inplace=True)

df_dim_plant_instructions

# %%
"""
##### a. Merged to Fact Table Plants Data
"""

# %%
# Menggabungkan kedua dataframe
df_fact_plants_data = pd.merge(df_dim_plant_instructions, df_fact_plants_data, left_on="plant_id", 
                               right_on="plant_id", suffixes=('_instructions', '_fact'))

# Menampilkan hasil gabungan
df_fact_plants_data.columns

# %%
df_fact_plants_data = df_fact_plants_data[['plant_id', 'plant_faqs_id', 'plant_instruction_id', 'watering_reminders_id']]

df_fact_plants_data

# %%
"""
##### b. Save to CSV
"""

# %%
df_dim_plant_instructions.info()

# %%
df_dim_plant_instructions = df_dim_plant_instructions[['plant_instruction_id', 'name_instruction_categories', 'step_number', 'step_title',
       'step_description', 'step_image_url', 'additional_tips', 'created_at',
       'updated_at']]
df_dim_plant_instructions

# %%
df_dim_plant_instructions.to_csv('../data_source_to_load/dim_plant_instructions.csv', index=False)

# %%
"""
#### 3.4.4.5 dim_plant_characteristics
"""

# %%
df_dim_plant_characteristics

# %%
df_dim_plant_characteristics.rename(columns={'id': 'plant_characteristic_id'}, inplace=True)
df_dim_plant_characteristics

# %%
"""
##### a. Merged to Fact Table Plants Data
"""

# %%
# Menggabungkan kedua dataframe
df_fact_plants_data = pd.merge(df_dim_plant_characteristics, df_fact_plants_data, left_on="plant_id", 
                               right_on="plant_id", suffixes=('_characteristics', '_fact'))

# Menampilkan hasil gabungan
df_fact_plants_data.columns

# %%
df_fact_plants_data = df_fact_plants_data[['plant_id', 'plant_faqs_id', 'plant_characteristic_id', 'plant_instruction_id', 'watering_reminders_id']]

df_fact_plants_data

# %%
"""
##### b. Save to CSV
"""

# %%
df_dim_plant_characteristics = df_dim_plant_characteristics[['plant_characteristic_id', 'height', 'height_unit', 'wide',
       'wide_unit', 'leaf_color']]

df_dim_plant_characteristics

# %%
df_dim_plant_characteristics.to_csv('../data_source_to_load/dim_plant_characteristics.csv', index=False)

# %%
"""
#### 3.4.4.6 Merged Fact Table Plants Data Final
"""

# %%
# Menghitung total_plants
df_fact_plants_data['total_plants'] = df_fact_plants_data['plant_id'].nunique()
df_fact_plants_data

# %%
"""
##### a. Save to CSV
"""

# %%
df_fact_plants_data.to_csv('../data_source_to_load/fact_plants_data.csv', index=False)

# %%
"""
# 4. Load
"""

# %%
load_dotenv()

project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET_ID')
service_acc = os.getenv('SERVICE_ACCOUNT')

os.environ['SERVICE_ACCOUNT'] = service_acc

credentials = service_account.Credentials.from_service_account_file(service_acc)

client = bigquery.Client(credentials=credentials, project=project_id)

def load_csv_to_bigquery(csv_file_path, table_id):
    # Baca file CSV ke DataFrame
    df = pd.read_csv(csv_file_path)

    # Bersihkan DataFrame
    df_cleaned = cleanse_dataframe(df)
    
    df_cleaned = change_type_data(df)

    # Tentukan ID tabel penuh: dataset_id.table_name
    table_id = f"{project_id}.{dataset_id}.{table_id}"
    
    # Muat DataFrame ke tabel BigQuery
    job = client.load_table_from_dataframe(df_cleaned, table_id)
    
    # Tunggu pekerjaan selesai
    job.result()
    
    print(f"Loaded {len(df_cleaned)} rows into {table_id}.")

# Direktori tempat file CSV Anda berada
data_source_dir = '../data_source_to_load'

# Muat setiap file CSV di direktori tersebut ke tabel BigQuery yang sesuai
for csv_file in os.listdir(data_source_dir):
    if csv_file.endswith('.csv'):
        csv_file_path = os.path.join(data_source_dir, csv_file)
        table_name = os.path.splitext(csv_file)[0]  # Nama tabel diambil dari nama file tanpa ekstensi
        load_csv_to_bigquery(csv_file_path, table_name)