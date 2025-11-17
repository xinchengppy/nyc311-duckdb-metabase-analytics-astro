import logging
from include.scripts.data_ingestion import download_nyc311_data
from include.scripts.db_creation import update_duckdb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting ETL update...")
    download_nyc311_data(days_back=2)
    update_duckdb()
    logger.info("ETL update completed.")