import os
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
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
    disaster_df = pd.merge(result_df, pivot_df, on=['country', 'year', 'month', 'incident_type'], how='left')

    print(disaster_df)
    return disaster_df




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
    Temp_df = df.groupby(['country', 'year', 'month']).agg({'temp': lambda x: round(x.mean(), 1)}).reset_index()
    Temp_df.rename(columns={'temp': 'avg_temp'}, inplace=True)

    print(Temp_df)
    return Temp_df




# Create function to merge the cleaned disaster and temp data frames
def join_dataframe(temp_dataframe, disaster_dataframe):

    merged_df = pd.merge(temp_dataframe, disaster_dataframe, on=['country', 'year', 'month'])
    print(merged_df.head())
    return merged_df




# Create function to analyse the merged disaster and temp data frame
def analyse_dataframe(temp_disaster_df):


    # Distribution of Incident Types
    incident_type_distribution = temp_disaster_df['incident_type'].value_counts()

    # Year-wise and Month-wise Analysis
    year_wise_incidents = temp_disaster_df.groupby('year').size()
    month_wise_incidents = temp_disaster_df.groupby('month').size()

    # Average Temperature Trends
    avg_temp_trends_year = temp_disaster_df.groupby('year')['avg_temp'].mean()
    avg_temp_trends_month = temp_disaster_df.groupby('month')['avg_temp'].mean()

    # Correlation Analysis (excluding non-numeric columns)
    numeric_df = temp_disaster_df.select_dtypes(include='number')
    correlation_matrix = numeric_df.corr()


    # Visualizations

    # 2. Distribution of Incident Types - Bar plot
    plt.figure(figsize=(12, 6))
    incident_type_distribution.plot(kind='bar', color='skyblue')
    plt.title('Distribution of Incident Types')
    plt.xlabel('Incident Type')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.show()

    # 3. Year-wise Number of Incidents - Line plot
    plt.figure(figsize=(12, 6))
    year_wise_incidents.plot(kind='line', marker='o', color='green')
    plt.title('Year-wise Number of Incidents')
    plt.xlabel('Year')
    plt.ylabel('Number of Incidents')
    plt.grid(True)
    plt.show()

    # 4. Month-wise Number of Incidents - Bar plot
    plt.figure(figsize=(12, 6))
    month_wise_incidents.plot(kind='bar', color='coral')
    plt.title('Month-wise Number of Incidents')
    plt.xlabel('Month')
    plt.ylabel('Number of Incidents')
    plt.xticks(rotation=0)
    plt.show()

    # 5. Average Temperature Trends Over the Years - Line plot
    plt.figure(figsize=(12, 6))
    avg_temp_trends_year.plot(kind='line', marker='o', color='blue')
    plt.title('Average Temperature Trends Over the Years')
    plt.xlabel('Year')
    plt.ylabel('Average Temperature (°C)')
    plt.grid(True)
    plt.show()

    # 6. Average Temperature Trends Over the Months - Line plot
    plt.figure(figsize=(12, 6))
    avg_temp_trends_month.plot(kind='line', marker='o', color='purple')
    plt.title('Average Temperature Trends Over the Months')
    plt.xlabel('Month')
    plt.ylabel('Average Temperature (°C)')
    plt.grid(True)
    plt.show()

    # 7. Correlation Matrix - Heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title('Correlation Matrix')
    plt.show()



# create main function
def main():
    
    
    #Dataset 1: Berkley (Temperature dataframe)
    data_directory = receive_berkley_dataset(BERKLEY_URL)
    selected_csv_path = os.path.join(data_directory, SELECT_CSV_BERKLEY)
    temp_dataframe = clean_dataset_berkley(selected_csv_path)


    #Dataset 2: Kaggle (Disaster dataframe)
    data_directory = receive_kaggle_dataset(KAGGLE_URL)
    selected_csv_path = os.path.join(data_directory, SELECT_CSV_KAGGLE)
    disaster_dataframe = clean_dataset_kaggle(selected_csv_path)


    temp_disaster_df = join_dataframe(temp_dataframe, disaster_dataframe)

    #temp_disaster_df.to_csv('2_Project/temp_disaster_df.csv', index=False)

    analyse_dataframe(temp_disaster_df)


if __name__ == "__main__":
    main()





