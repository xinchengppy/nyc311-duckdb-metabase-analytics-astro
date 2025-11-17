import os
import logging
import duckdb

from include.scripts.config import DB_WRITE_FILE, CSV_FILE, SQL_DIR

logger = logging.getLogger(__name__)

def get_connection():
    os.makedirs(os.path.dirname(DB_WRITE_FILE), exist_ok=True)
    return duckdb.connect(DB_WRITE_FILE, read_only=False)


def load_sql_file(path: str, **kwargs):
    with open(path, "r") as f:
        sql = f.read()

    for k, v in kwargs.items():
        sql = sql.replace(f"{{{{{k}}}}}", v)

    return sql


def run_staging(con):
    path = os.path.join(SQL_DIR, "staging.sql")
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    sql = load_sql_file(path, CSV_FILE=CSV_FILE)

    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute(sql)
    logger.info("Staging table updated")


MODEL_FILES = [
    "dim_complaint_type.sql",
    "fct_requests.sql",
    "complaint_volume.sql",
    "top_complaint_types.sql",
    "resolution_by_type.sql",
    "complaint_dashboard.sql"
]


def run_models(con):
    models_dir = os.path.join(SQL_DIR, "models")
    con.execute("CREATE SCHEMA IF NOT EXISTS models;")

    for fname in MODEL_FILES:
        path = os.path.join(models_dir, fname)
        if not os.path.exists(path):
            logger.warning(f"Missing model: {path}")
            continue

        sql = load_sql_file(path)
        con.execute(sql)
        logger.info(f"Model executed: {fname}")


def update_duckdb():
    logger.info("Updating DuckDB...")
    con = get_connection()
    try:
        run_staging(con)
        run_models(con)
        con.execute("CHECKPOINT;")
        logger.info("DuckDB updated successfully")
    finally:
        con.close()