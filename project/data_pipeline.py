import os
import pandas as pd
import sqlite3
#from IPython.display import display
from kaggle.api.kaggle_api_extended import KaggleApi


# Configure Kaggle API
api = KaggleApi()
api.authenticate()


# Create a directory to saving cleaned data file
DATA_DIRECTORY = os.path.expanduser('./data')
if not os.path.exists(DATA_DIRECTORY):
    os.makedirs(DATA_DIRECTORY)



  
# Give KAGGLE dataset URL
KAGGLE_URL = 'https://www.kaggle.com/datasets/headsortails/us-natural-disaster-declarations'


# select specific CSV file from Kaggle data folder
SELECT_CSV_KAGGLE = 'us_disaster_declarations.csv'


# Create function to receive dataset from Kaggle and return its directory path
def receive_kaggle_dataset(dataset_url):
    # Extract dataset name from the URL
    parts = dataset_url.split('/datasets/')
    dataset_name = parts[-1]
    # Download dataset
    local_directory = os.path.join(DATA_DIRECTORY, dataset_url.split('/')[-1])
    api.dataset_download_files(dataset_name, path=local_directory, unzip=True)
    print(f"local_directory is {local_directory} extracted from url {dataset_url}")
    return local_directory


# Create function to clean and transform kaggle data set
def clean_dataset_kaggle(file_path):
    # read file as dataframe
    df = pd.read_csv(file_path)
    # Drow unnecessary columns
    df = df.drop(['fema_declaration_string', 'disaster_number', 'declaration_title', 'ih_program_declared', 
                'ia_program_declared', 'pa_program_declared', 'hm_program_declared', 'incident_begin_date', 
                'incident_end_date', 'disaster_closeout_date', 'fips', 'place_code', 'designated_area', 
                'declaration_request_number', 'last_ia_filing_date', 'last_refresh', 'hash', 'id'], axis=1)
    
    # Create year and month from 'date' column
    df['declaration_date'] = pd.to_datetime(df['declaration_date'])
    
    df['year'] = df['declaration_date'].dt.year
    
    df['month'] = df['declaration_date'].dt.month

    # Keep rows only with year between 1990 and 2013
    df = df[(df['year'] >= 1990) & (df['year'] <= 2013)]

    # Add a column 'country' as 'United States of America' in dataframe
    df['country'] = 'United States of America'

    # Group by 'country', 'year', and 'incident_type' and count the total number of incidents
    result_df = df.groupby(['country', 'year', 'month', 'incident_type']).size().reset_index(name='total_incidents')

    # Pivot the table to get counts of each declaration type ('DR', 'EM', 'FM')
    pivot_df = df.pivot_table(index=['country', 'year', 'month', 'incident_type'], columns='declaration_type', aggfunc='size', fill_value=0)

    # Arrange the pivot_df table columns as flat table
    pivot_df.columns = ['number_of_' + str(col).lower() for col in pivot_df.columns]

    # Merge the pivot_df table with the result_df DataFrame
    df = pd.merge(result_df, pivot_df, on=['country', 'year', 'month', 'incident_type'], how='left')

    print(df)
    return df




# Give Berkley dataset URL
BERKLEY_URL = 'berkeleyearth/climate-change-earth-surface-temperature-data'

# select specific CSV file from Berkley data folder
SELECT_CSV_BERKLEY = 'GlobalLandTemperaturesByCountry.csv'

# Create function to receive dataset from Berkley and return its directory path
def receive_berkley_dataset(dataset_url):
    local_directory = os.path.join(DATA_DIRECTORY, dataset_url.split('/')[-1])
    api.dataset_download_files(dataset_url, path=local_directory, unzip=True)
    print(f"local_directory is {local_directory} extracted from url {dataset_url}")
    return local_directory

# Create function to clean and transform data set
def clean_dataset_berkley(file_path):
    # read file as dataframe
    df = pd.read_csv(file_path)

    # Drop unnecessary columns
    df = df.drop(['AverageTemperatureUncertainty'], axis=1)

    # Rename columns
    df.rename(columns={'dt': 'date', 'AverageTemperature': 'temp', 'Country': 'country'}, inplace=True)

    # Create year and month from 'date' column
    df['date'] = pd.to_datetime(df['date'])
    
    df['year'] = df['date'].dt.year
    
    df['month'] = df['date'].dt.month

    # Keep dataset only for country value 'United States'
    df = df[df['country'] == 'United States']

    # Replace value 'United States' with 'United States of America'
    df['country'] = 'United States of America'

    # Keep rows only with year between 1990 and 2013
    df = df[(df['year'] >= 1990) & (df['year'] <= 2013)]

    # Group by 'country', 'year', and 'month' columns and calculate the avg temperature rounded off to one decimal place
    groupby_df = df.groupby(['country', 'year', 'month']).agg({'temp': lambda x: round(x.mean(), 1)}).reset_index()
    groupby_df.rename(columns={'temp': 'avg_temp'}, inplace=True)

    print(groupby_df)
    return groupby_df




# Create a function to store cleaned dataframe into an SQLite file
def create_sqlite_database(dataframe, db_filename):
    database_path = os.path.join(DATA_DIRECTORY, db_filename)
    connect_db = sqlite3.connect(database_path)
    dataframe.to_sql('data', connect_db, if_exists='replace', index=False)
    connect_db.close()


# create main function
def main():
    
    
    #Dataset 1: Berkley
    data_directory = receive_berkley_dataset(BERKLEY_URL)
    selected_csv_path = os.path.join(data_directory, SELECT_CSV_BERKLEY)
    cleaned_data = clean_dataset_berkley(selected_csv_path)
    database_file_name = os.path.splitext(SELECT_CSV_BERKLEY)[0] + '.sqlite'
    create_sqlite_database(cleaned_data, database_file_name)
    print(f"Data from {SELECT_CSV_BERKLEY} processed and saved in {database_file_name}")
     

    #Dataset 2: Kaggle
    #data_directory = 'data'
    data_directory = receive_kaggle_dataset(KAGGLE_URL)
    selected_csv_path = os.path.join(data_directory, SELECT_CSV_KAGGLE)
    cleaned_data = clean_dataset_kaggle(selected_csv_path)
    database_file_name = os.path.splitext(SELECT_CSV_KAGGLE)[0] + '.sqlite'
    create_sqlite_database(cleaned_data, database_file_name)
    print(f"Data from {SELECT_CSV_KAGGLE} processed and saved in {database_file_name}")


if __name__ == "__main__":
    main()