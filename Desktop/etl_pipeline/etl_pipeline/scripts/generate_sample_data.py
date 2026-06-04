"""
generate_sample_data.py
Generates realistic sample network event data for the ETL pipeline demo.
Run this first: python scripts/generate_sample_data.py
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

NUM_RECORDS    = 10_000
DEVICES        = [f"DEV-{str(i).zfill(4)}" for i in range(1, 51)]   # 50 devices
EVENT_TYPES    = ["connection", "data_transfer", "auth_request", "disconnect", "error"]
REGIONS        = ["us-east", "us-west", "eu-central", "ap-south", "ap-northeast"]
START_DATE     = datetime(2024, 1, 1)

rows = []
for _ in range(NUM_RECORDS):
    ts = START_DATE + timedelta(
        days=random.randint(0, 180),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    rows.append({
        "device_id":         random.choice(DEVICES),
        "event_timestamp":   ts.strftime("%Y-%m-%d %H:%M:%S"),
        "event_type":        random.choice(EVENT_TYPES),
        "bytes_transferred": random.randint(0, 1_500_000),
        "region":            random.choice(REGIONS),
        "protocol":          random.choice(["TCP", "UDP", "HTTP", "HTTPS"]),
        "status_code":       random.choice([200, 200, 200, 400, 404, 500]),
    })

# Inject intentional data quality issues for the pipeline to catch
# ~2% null device_ids
for i in random.sample(range(NUM_RECORDS), 200):
    rows[i]["device_id"] = None

# ~1% duplicate rows
dupes = random.sample(rows[:500], 100)
rows.extend(dupes)

# ~0.5% bad timestamps
for i in random.sample(range(NUM_RECORDS), 50):
    rows[i]["event_timestamp"] = "NOT_A_DATE"

df = pd.DataFrame(rows)
df.to_csv("data/network_events_raw.csv", index=False)
print(f"Generated {len(df):,} rows → data/network_events_raw.csv")
print(f"  Intentional nulls: ~200  |  Dupes: ~100  |  Bad timestamps: ~50")
print("  Run the ETL pipeline to see validation in action.")
