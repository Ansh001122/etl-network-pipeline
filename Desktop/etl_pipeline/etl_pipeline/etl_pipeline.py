"""
ETL Pipeline — Network Events Data
Author : Ansh Raj
Description: Extracts raw CSV data, applies transformations and data quality
             validations, then loads clean records into a MySQL database.
             Demonstrates core data engineering concepts: ETL, schema design,
             data modeling, validation, and Python/Pandas proficiency.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  EXTRACT
# ─────────────────────────────────────────────
def extract(filepath: str) -> pd.DataFrame:
    """Load raw CSV data into a DataFrame."""
    log.info(f"Extracting data from {filepath}")
    df = pd.read_csv(filepath)
    log.info(f"Extracted {len(df):,} rows, {len(df.columns)} columns")
    return df


# ─────────────────────────────────────────────
#  TRANSFORM
# ─────────────────────────────────────────────
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all transformation and validation steps.
    Returns a clean, enriched DataFrame ready for loading.
    """
    log.info("Starting transformation...")
    original_count = len(df)

    # 1. Standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # 2. Parse & validate timestamps
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")
    invalid_ts = df["event_timestamp"].isna().sum()
    if invalid_ts > 0:
        log.warning(f"Dropping {invalid_ts} rows with unparseable timestamps")
    df = df.dropna(subset=["event_timestamp"])

    # 3. Drop rows missing critical fields (completeness check)
    required_cols = ["device_id", "event_type", "bytes_transferred"]
    before = len(df)
    df = df.dropna(subset=required_cols)
    dropped = before - len(df)
    if dropped > 0:
        log.warning(f"Dropped {dropped} rows missing required fields")

    # 4. Remove duplicates (uniqueness check)
    before = len(df)
    df = df.drop_duplicates(subset=["device_id", "event_timestamp", "event_type"])
    dupes = before - len(df)
    if dupes > 0:
        log.warning(f"Removed {dupes} duplicate records")

    # 5. Type casting & range validation
    df["bytes_transferred"] = pd.to_numeric(df["bytes_transferred"], errors="coerce").fillna(0).astype(int)
    df["bytes_transferred"] = df["bytes_transferred"].clip(lower=0)  # no negative bytes

    # 6. Standardize string fields
    df["device_id"] = df["device_id"].str.strip().str.upper()
    df["event_type"] = df["event_type"].str.strip().str.lower()
    df["region"] = df["region"].str.strip().str.title() if "region" in df.columns else "Unknown"

    # 7. Derived / enrichment columns
    df["event_date"] = df["event_timestamp"].dt.date
    df["event_hour"] = df["event_timestamp"].dt.hour
    df["bytes_kb"] = (df["bytes_transferred"] / 1024).round(2)
    df["load_timestamp"] = datetime.utcnow()

    log.info(f"Transform complete: {original_count:,} → {len(df):,} clean rows")
    return df


# ─────────────────────────────────────────────
#  VALIDATE (data quality gate)
# ─────────────────────────────────────────────
def validate(df: pd.DataFrame) -> bool:
    """
    Gate the pipeline on data quality assertions.
    Returns True if all checks pass, False otherwise.
    """
    log.info("Running data quality checks...")
    passed = True

    checks = {
        "No null device_id":       df["device_id"].notna().all(),
        "No null event_type":      df["event_type"].notna().all(),
        "bytes_transferred >= 0":  (df["bytes_transferred"] >= 0).all(),
        "Row count > 0":           len(df) > 0,
        "event_timestamp is date": pd.api.types.is_datetime64_any_dtype(df["event_timestamp"]),
    }

    for check, result in checks.items():
        status = "PASS" if result else "FAIL"
        log.info(f"  [{status}] {check}")
        if not result:
            passed = False

    return passed


# ─────────────────────────────────────────────
#  LOAD
# ─────────────────────────────────────────────
def load(df: pd.DataFrame, table: str = "network_events") -> None:
    """Load clean DataFrame into MySQL using SQLAlchemy + PyMySQL."""
    db_url = (
        f"mysql+pymysql://{os.getenv('DB_USER', 'root')}:"
        f"{os.getenv('DB_PASSWORD', 'password')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '3306')}/"
        f"{os.getenv('DB_NAME', 'cisco_etl_db')}"
    )

    log.info(f"Connecting to MySQL: {os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}")
    engine = create_engine(db_url, echo=False)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # connection health check
    log.info("MySQL connection OK")

    df.to_sql(
        name=table,
        con=engine,
        if_exists="append",   # append so re-runs don't wipe data
        index=False,
        chunksize=1000,       # batch inserts for large datasets
        method="multi"
    )
    log.info(f"Loaded {len(df):,} rows into `{table}`")


# ─────────────────────────────────────────────
#  ANALYTICS QUERIES (post-load verification)
# ─────────────────────────────────────────────
def run_summary_queries(engine) -> None:
    """Run basic analytical queries to verify loaded data."""
    queries = {
        "Total records loaded": "SELECT COUNT(*) as total FROM network_events",
        "Records by event type": "SELECT event_type, COUNT(*) as count FROM network_events GROUP BY event_type ORDER BY count DESC",
        "Top 5 devices by data transferred": """
            SELECT device_id, SUM(bytes_transferred) as total_bytes
            FROM network_events
            GROUP BY device_id
            ORDER BY total_bytes DESC
            LIMIT 5
        """,
        "Hourly distribution": """
            SELECT event_hour, COUNT(*) as events
            FROM network_events
            GROUP BY event_hour
            ORDER BY event_hour
        """
    }

    db_url = (
        f"mysql+pymysql://{os.getenv('DB_USER', 'root')}:"
        f"{os.getenv('DB_PASSWORD', 'password')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '3306')}/"
        f"{os.getenv('DB_NAME', 'cisco_etl_db')}"
    )
    engine = create_engine(db_url)

    for label, query in queries.items():
        df = pd.read_sql(query, engine)
        log.info(f"\n── {label} ──\n{df.to_string(index=False)}")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def run_pipeline(filepath: str = "data/network_events_raw.csv") -> None:
    log.info("=" * 55)
    log.info("  ETL PIPELINE STARTED")
    log.info("=" * 55)

    try:
        raw_df      = extract(filepath)
        clean_df    = transform(raw_df)
        quality_ok  = validate(clean_df)

        if not quality_ok:
            log.error("Data quality gate FAILED — aborting load.")
            return

        load(clean_df)
        log.info("=" * 55)
        log.info("  PIPELINE COMPLETED SUCCESSFULLY")
        log.info("=" * 55)

    except Exception as e:
        log.error(f"Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()
