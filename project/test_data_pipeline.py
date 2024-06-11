import os
import pandas as pd
import sqlite3
import pytest
from unittest import mock

# Mock KaggleApi import to prevent ImportError
with mock.patch.dict('sys.modules', {'kaggle.api.kaggle_api_extended': mock.Mock()}):
    # Import functions and variables from data_pipeline script
    from data_pipeline import (
        clean_dataset_kaggle,
        clean_dataset_berkley,
        create_sqlite_database,
        DATA_DIRECTORY,
        SELECT_CSV_KAGGLE,
        SELECT_CSV_BERKLEY,
    )

# Create sample data for testing
sample_kaggle_data = pd.DataFrame({
    'fema_declaration_string': ['DR-3-LA'],
    'disaster_number': [1],
    'state': ['GA'],
    'declaration_type': ['DR'],
    'declaration_date': ['2010-05-02T00:00:00Z'],
    'fy_declared': [1953],	
    'incident_type': ['Tornado'],	
    'declaration_title': ['Tornado'],
    'ih_program_declared': [0],
    'ia_program_declared': [1],
    'pa_program_declared': [1],
    'hm_program_declared': [1],
    'incident_begin_date': ['2010-05-02T00:00:00Z'],
    'incident_end_date': ['2010-05-02T00:00:00Z'],
    'disaster_closeout_date': ['2010-05-02T00:00:00Z'],
    'fips': [13000],
    'place_code': [0],
    'designated_area': ['Statewide'],
    'declaration_request_number': [53013],
    'last_ia_filing_date': ['NA'],
    'last_refresh': ['2023-03-18T13:22:03Z'],
    'hash': ['48af1afcc4535aa910ddb5b85eebe047dc703a6b'],
    'id': ['da5c8f17-c28f-4c41-8e06-fa8efc85aa4a']
})

sample_berkley_data = pd.DataFrame({
    'dt': ['2010-01-01'],
    'AverageTemperature': [40.5],
    'AverageTemperatureUncertainty': [2.294],
    'Country': ['United States']
})

def test_clean_dataset_kaggle():

    # Save the sample data to a CSV file
    print(f"DATA_DIRECTORY: {DATA_DIRECTORY}")
    print(f"SELECT_CSV_KAGGLE: {SELECT_CSV_KAGGLE}")

    # Save the sample data to a CSV file
    sample_kaggle_data.to_csv(os.path.join(DATA_DIRECTORY, SELECT_CSV_KAGGLE), index=False)
    
    # Clean the dataset
    cleaned_df = clean_dataset_kaggle(os.path.join(DATA_DIRECTORY, SELECT_CSV_KAGGLE))

   # Print the cleaned dataframe for debugging
    print(cleaned_df)
    
    # Assertions
    assert not cleaned_df.empty
    assert 'year' in cleaned_df.columns
    assert 'month' in cleaned_df.columns
    assert 'country' in cleaned_df.columns
    assert 'total_incidents' in cleaned_df.columns

def test_clean_dataset_berkley():
    # Save the sample data to a CSV file
    sample_berkley_data.to_csv(os.path.join(DATA_DIRECTORY, SELECT_CSV_BERKLEY), index=False)
    
    # Clean the dataset
    cleaned_df = clean_dataset_berkley(os.path.join(DATA_DIRECTORY, SELECT_CSV_BERKLEY))
    
    # Assertions
    assert not cleaned_df.empty
    assert 'year' in cleaned_df.columns
    assert 'month' in cleaned_df.columns
    assert 'avg_temp' in cleaned_df.columns

def test_create_sqlite_database():
    sample_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    db_filename = 'test_db.sqlite'
    
    # Create SQLite database
    create_sqlite_database(sample_df, db_filename)
    
    db_path = os.path.join(DATA_DIRECTORY, db_filename)
    assert os.path.isfile(db_path)
    
    # Verify the content of the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    assert len(tables) == 1
    assert tables[0][0] == 'data'
    conn.close()


if __name__ == "__main__":
    pytest.main()