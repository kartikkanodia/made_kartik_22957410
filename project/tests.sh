#!/bin/bash

# Step 1: Create and activate a virtual environment (optional)
python3 -m venv venv
source venv/bin/activate

# Step 2: Install required dependencies
pip install pandas sqlite3 pytest

# Step 3: Run the Python script
pytest test_data_pipeline.py
