# ETL Pipeline — Network Events Data Engineering Project

A production-style ETL (Extract → Transform → Load) pipeline built with **Python**, **Pandas**, and **MySQL**. Demonstrates core data engineering skills: pipeline design, data quality validation, schema design, and data modeling.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Data Processing | Pandas, NumPy |
| Database | MySQL 8.0 |
| ORM / Connector | SQLAlchemy + PyMySQL |
| Schema | Fact + Dimension tables (Star Schema) |

---

## Project Structure

```
etl_pipeline/
├── etl_pipeline.py              # Main ETL script (Extract → Transform → Validate → Load)
├── scripts/
│   └── generate_sample_data.py  # Generates 10,000+ sample records with data quality issues
├── sql/
│   └── schema.sql               # MySQL DDL — fact table, dimension table, audit table, view
├── data/                        # Raw CSV data goes here (git-ignored)
├── logs/                        # Pipeline logs (git-ignored)
├── requirements.txt
└── .env.example                 # Environment variable template
```

---

## What the Pipeline Does

### Extract
- Reads raw CSV data (network event logs with 10,000+ records)

### Transform
- Standardizes column names and string fields
- Parses and validates timestamps
- Drops rows missing critical fields (completeness check)
- Removes duplicates (uniqueness check)
- Validates numeric ranges (no negative bytes)
- Adds derived columns: `event_date`, `event_hour`, `bytes_kb`, `load_timestamp`

### Validate (Data Quality Gate)
Five automated checks gate the load step:
1. No null `device_id`
2. No null `event_type`
3. `bytes_transferred >= 0`
4. Row count > 0
5. `event_timestamp` is a valid datetime

### Load
- Batch inserts clean records into MySQL (`chunksize=1000`)
- Uses `append` mode so re-runs don't overwrite existing data
- Connection health check before load

---

## Database Schema

**Star Schema design:**

```
devices (dimension)          network_events (fact)
──────────────────           ─────────────────────────────────────
device_id  PK          ←──  device_id  FK
device_name                  id  PK  AUTO_INCREMENT
device_type                  event_timestamp
region                       event_type
registered_at                bytes_transferred
                             bytes_kb
                             protocol
                             status_code
                             region
                             event_date
                             event_hour
                             load_timestamp
```

Indexes on `device_id`, `event_timestamp`, `event_type`, and `event_date` for query performance.

---

## Setup & Run

### 1. Clone the repo
```bash
git clone https://github.com/Ansh001122/etl-network-pipeline.git
cd etl-network-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure MySQL
```bash
cp .env.example .env
# Edit .env with your MySQL credentials
```

### 4. Create the schema
```bash
mysql -u root -p < sql/schema.sql
```

### 5. Generate sample data
```bash
python scripts/generate_sample_data.py
```

### 6. Run the pipeline
```bash
python etl_pipeline.py
```

---

## Sample Output

```
2024-06-04 10:00:01 [INFO] ETL PIPELINE STARTED
2024-06-04 10:00:01 [INFO] Extracting data from data/network_events_raw.csv
2024-06-04 10:00:01 [INFO] Extracted 10,100 rows, 7 columns
2024-06-04 10:00:01 [INFO] Starting transformation...
2024-06-04 10:00:01 [WARNING] Dropping 50 rows with unparseable timestamps
2024-06-04 10:00:01 [WARNING] Dropped 200 rows missing required fields
2024-06-04 10:00:01 [WARNING] Removed 100 duplicate records
2024-06-04 10:00:01 [INFO] Transform complete: 10,100 → 9,750 clean rows
2024-06-04 10:00:01 [INFO] Running data quality checks...
2024-06-04 10:00:01 [INFO]   [PASS] No null device_id
2024-06-04 10:00:01 [INFO]   [PASS] No null event_type
2024-06-04 10:00:01 [INFO]   [PASS] bytes_transferred >= 0
2024-06-04 10:00:01 [INFO]   [PASS] Row count > 0
2024-06-04 10:00:01 [INFO]   [PASS] event_timestamp is date
2024-06-04 10:00:01 [INFO] MySQL connection OK
2024-06-04 10:00:02 [INFO] Loaded 9,750 rows into `network_events`
2024-06-04 10:00:02 [INFO] PIPELINE COMPLETED SUCCESSFULLY
```

---

## Key Concepts Demonstrated

- **ETL pipeline** design with separation of concerns (extract / transform / validate / load)
- **Data quality validation** with automated checks and pipeline gating
- **Star schema** data modeling (fact + dimension tables)
- **SQL** — DDL with indexes, foreign keys, views, and window functions
- **Pandas** — data cleaning, type casting, deduplication, enrichment
- **SQLAlchemy** — ORM-based MySQL integration with batch loading
- **Python** best practices — logging, environment variables, error handling

---

*Built as a portfolio project to demonstrate data engineering fundamentals.*
