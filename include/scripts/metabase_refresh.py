import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

METABASE_URL = os.getenv("METABASE_URL", "http://host.docker.internal:3000")
METABASE_USER = os.getenv("METABASE_USER")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")
METABASE_DB_ID = int(os.getenv("METABASE_DB_ID", "2"))

def metabase_login():
    try:
        resp = requests.post(
            f"{METABASE_URL}/api/session",
            json={"username": METABASE_USER, "password": METABASE_PASSWORD},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()["id"]
    except Exception as e:
        logger.error(f"Metabase login failed: {e}")
        raise

def trigger_sync(session_id: str):
    url = f"{METABASE_URL}/api/database/{METABASE_DB_ID}/sync_schema"
    resp = requests.post(url, cookies={"metabase.SESSION": session_id})
    if resp.status_code == 200:
        logger.info("Schema sync triggered successfully.")
    else:
        logger.error(f"Schema sync failed → {resp.status_code}: {resp.text}")

def trigger_rescan(session_id: str):
    url = f"{METABASE_URL}/api/database/{METABASE_DB_ID}/rescan_values"
    resp = requests.post(url, cookies={"metabase.SESSION": session_id})
    if resp.status_code == 200:
        logger.info("Values rescan triggered successfully.")
    else:
        logger.error(f"Values rescan failed → {resp.status_code}: {resp.text}")

def refresh_metabase():
    logger.info("Starting Metabase refresh...")
    session_id = metabase_login()
    trigger_sync(session_id)
    trigger_rescan(session_id)
    logger.info("Metabase refresh completed successfully!")
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    refresh_metabase()