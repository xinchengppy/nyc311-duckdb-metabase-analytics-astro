import os
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from include.scripts.metabase_refresh import refresh_metabase
from datetime import datetime, timedelta
import shutil

from include.scripts.data_ingestion import download_nyc311_data
from include.scripts.db_creation import update_duckdb
from include.scripts.config import DB_WRITE_FILE, DB_READ_FILE

default_args = {
    "owner": "xincheng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="nyc311_daily_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)
def nyc311_etl():

    @task
    def extract():
        df = download_nyc311_data(days_back=60)
        return len(df)

    @task
    def load():
        update_duckdb()
        return "duckdb updated"
    
    @task
    def read_only_copy():
        shutil.copy(DB_WRITE_FILE, DB_READ_FILE)
        return "read-only copy created"
    

    restart_metabase = BashOperator(
        task_id="restart_metabase",
        bash_command="""
            echo "Restarting Metabase container..."
            docker restart metaduck
            
            echo "Waiting for Metabase to be healthy..."
            for i in {1..30}; do
                if docker exec metaduck curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
                    echo "Metabase is online"
                    exit 0
                fi
                echo "  Attempt $i/30..."
                sleep 2
            done
            
            echo "Metabase failed to start"
            exit 1
        """,
    )

    @task
    def metabase_refresh():
        refresh_metabase()
        return "Metabase refreshed"

    extract() >> load() >> read_only_copy() >> restart_metabase >> metabase_refresh()
nyc311_etl_dag = nyc311_etl()