import os
import logging
import pandas as pd
import requests
from datetime import datetime, date, timezone
from dateutil.relativedelta import relativedelta

from include.scripts.config import APP_TOKEN, CSV_FILE, BASIC_URL, DATA_DIR

logger = logging.getLogger(__name__)

def _compute_cutoff_date(days_back: int) -> str:
    cutoff_date = datetime.combine(
        date.today() - relativedelta(days=days_back),
        datetime.min.time(),
        tzinfo=timezone.utc
    )
    return cutoff_date.strftime("%Y-%m-%dT%H:%M:%S")


def _build_soql_query(cutoff_str: str) -> str:
    cols = [
        "unique_key", "created_date", "closed_date", "agency", "agency_name",
        "complaint_type", "descriptor", "location_type", "incident_zip",
        "incident_address", "street_name", "cross_street_1", "cross_street_2",
        "intersection_street_1", "intersection_street_2", "address_type", "city",
        "landmark", "facility_type", "status", "due_date", "resolution_description",
        "resolution_action_updated_date", "community_board", "bbl", "borough",
        "x_coordinate_state_plane", "y_coordinate_state_plane", "open_data_channel_type",
        "park_facility_name", "park_borough", "vehicle_type", "taxi_company_borough",
        "taxi_pick_up_location", "bridge_highway_name", "bridge_highway_direction",
        "road_ramp", "bridge_highway_segment", "latitude", "longitude", "location"
    ]
    select_clause = ", ".join(f"`{c}`" for c in cols)
    return f"SELECT {select_clause} WHERE created_date >= '{cutoff_str}' ORDER BY created_date DESC"


def _prepare_output_path():
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(CSV_FILE):
        os.remove(CSV_FILE)
        logger.info(f"Removed old CSV → {CSV_FILE}")


def download_nyc311_data(days_back: int = 60) -> pd.DataFrame:
    _prepare_output_path()

    cutoff = _compute_cutoff_date(days_back)
    query = _build_soql_query(cutoff)

    logger.info(f"Downloading NYC 311 data after {cutoff}")

    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    params = {"query": query}

    try:
        r = requests.get(BASIC_URL, params=params, headers=headers, timeout=60)
        r.raise_for_status()

        with open(CSV_FILE, "wb") as f:
            f.write(r.content)

        df = pd.read_csv(CSV_FILE, low_memory=False)
        logger.info(f"Downloaded {len(df)} rows → {CSV_FILE}")

        return df

    except Exception as e:
        logger.error(f"NYC 311 download failed: {e}")
        raise