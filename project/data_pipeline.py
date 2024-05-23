import os
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

# Configure Kaggle API
api = KaggleApi()
api.authenticate()

# Give Kaggle dataset URL
KAGGLE_URL = 'berkeleyearth/climate-change-earth-surface-temperature-data'  

# Give specific CSV file name that need to be selected from Kaggle data folder
SELECT_CSV_FILE = 'GlobalLandTemperaturesByCountry.csv'

# Create a directory to saving cleaned data file
DATA_DIRECTORY = os.path.expanduser('./data')
if not os.path.exists(DATA_DIRECTORY):
    os.makedirs(DATA_DIRECTORY)

# Create function to receive dataset from Kaggle and return its directory path
def receive_kaggle_dataset(dataset_url):
    local_directory = os.path.join(DATA_DIRECTORY, dataset_url.split('/')[-1])
    api.dataset_download_files(dataset_url, path=local_directory, unzip=True)
    return local_directory

# Create function to clean and transform data set
def clean_dataset(file_path):
    # read file as dataframe
    df = pd.read_csv(file_path)
    # Drop rows with missing values from dataframe
    df.dropna(inplace=True)  
    # Drop duplicate rows from dataframe
    df.drop_duplicates(inplace=True)  
    # Transform date column to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])  
    return df

# Create a function to store cleaned dataframe into an SQLite file
def create_sqlite_database(dataframe, db_filename):
    database_path = os.path.join(DATA_DIRECTORY, db_filename)
    connect_db = sqlite3.connect(database_path)
    dataframe.to_sql('data', connect_db, if_exists='replace', index=False)
    connect_db.close()

# create main function
def main():
    data_directory = receive_kaggle_dataset(KAGGLE_URL)
    selected_csv_path = os.path.join(data_directory, SELECT_CSV_FILE)
    cleaned_data = clean_dataset(selected_csv_path)
    database_file_name = os.path.splitext(SELECT_CSV_FILE)[0] + '.sqlite'
    create_sqlite_database(cleaned_data, database_file_name)
    print(f"Data from {SELECT_CSV_FILE} processed and saved in {database_file_name}")

if __name__ == "__main__":
    main()