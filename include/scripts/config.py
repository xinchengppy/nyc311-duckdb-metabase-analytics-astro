import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Data directories
DATA_DIR = 'include/data/raw'
DB_DIR = 'include/db'
SQL_DIR = 'include/sql'
CSV_FILE = os.path.join(DATA_DIR, 'nyc_311.csv')
DB_WRITE_FILE = os.path.join(DB_DIR, 'analytics.duckdb')
DB_READ_FILE = os.path.join(DB_DIR, 'analytics_read.duckdb') # For read-only connections
BASIC_URL = "https://data.cityofnewyork.us/api/v3/views/erm2-nwe9/query.csv"

# API settings
# NYC Open Data App Token (get from https://data.cityofnewyork.us/profile/app_tokens)
# Can be set in .env file as NYC_OPEN_DATA_APP_TOKEN=your_token
APP_TOKEN = os.getenv('NYC_OPEN_DATA_APP_TOKEN', '')