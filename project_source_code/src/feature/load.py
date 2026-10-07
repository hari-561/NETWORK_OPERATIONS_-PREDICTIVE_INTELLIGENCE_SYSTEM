import pandas as pd

from sqlalchemy import create_engine, text


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DB_URL = (
    "mysql+pymysql://root:root@localhost:3306/telecom_analytics"
)

CSV_PATH = "D:/capstone1/phase1/curated_data_all/activity_alerts.csv"


# ============================================================
# EXPECTED CSV COLUMNS
# ============================================================

EXPECTED_COLUMNS = [
    "grid_id",
    "timestamp",
    "alert_type",
    "current_activity",
    "baseline_activity",
    "reason",
]


# ============================================================
# LOAD CSV
# ============================================================

print("Loading activity_alerts.csv...")

df = pd.read_csv(CSV_PATH)

print(f"Rows read from CSV: {len(df):,}")


# ============================================================
# VALIDATE SCHEMA
# ============================================================

missing_columns = [
    col
    for col in EXPECTED_COLUMNS
    if col not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )


# Keep only expected columns
df = df[EXPECTED_COLUMNS].copy()


# ============================================================
# TYPE CONVERSION
# ============================================================

df["grid_id"] = pd.to_numeric(
    df["grid_id"],
    errors="coerce"
)

df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    errors="coerce"
)

df["current_activity"] = pd.to_numeric(
    df["current_activity"],
    errors="coerce"
)

df["baseline_activity"] = pd.to_numeric(
    df["baseline_activity"],
    errors="coerce"
)


# ============================================================
# VALIDATE NULLS
# ============================================================

if df["grid_id"].isna().any():
    raise ValueError("grid_id contains invalid/null values")

if df["timestamp"].isna().any():
    raise ValueError("timestamp contains invalid/null values")

if df["alert_type"].isna().any():
    raise ValueError("alert_type contains null values")

if df["current_activity"].isna().any():
    raise ValueError(
        "current_activity contains invalid/null values"
    )

if df["reason"].isna().any():
    raise ValueError("reason contains null values")


# ============================================================
# VALIDATE ALERT TYPES
# ============================================================

allowed_alert_types = {
    "HIGH_ACTIVITY",
    "ACTIVITY_SPIKE",
    "ACTIVITY_DROP",
}

invalid_types = set(df["alert_type"].dropna()) - allowed_alert_types

if invalid_types:
    raise ValueError(
        f"Invalid alert types found: {invalid_types}"
    )


# ============================================================
# REMOVE DUPLICATES FROM CSV
# ============================================================

before = len(df)

df = df.drop_duplicates(
    subset=[
        "grid_id",
        "timestamp",
        "alert_type",
    ]
)

after = len(df)

print(
    f"Duplicate rows removed: {before - after:,}"
)


# ============================================================
# CREATE DATABASE ENGINE
# ============================================================

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
)


# ============================================================
# CREATE TABLE
# ============================================================

create_table_sql = text("""
CREATE TABLE IF NOT EXISTS activity_alerts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    grid_id INT NOT NULL,
    timestamp DATETIME NOT NULL,

    alert_type VARCHAR(50) NOT NULL,

    current_activity DOUBLE NOT NULL,
    baseline_activity DOUBLE NULL,

    reason TEXT NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uq_activity_alert (
        grid_id,
        timestamp,
        alert_type
    ),

    INDEX idx_alert_timestamp (timestamp),
    INDEX idx_alert_grid (grid_id),
    INDEX idx_alert_type (alert_type)
)
""")


with engine.begin() as conn:

    conn.execute(create_table_sql)


# ============================================================
# INSERT DATA
# ============================================================

insert_sql = text("""
INSERT INTO activity_alerts (
    grid_id,
    timestamp,
    alert_type,
    current_activity,
    baseline_activity,
    reason
)
VALUES (
    :grid_id,
    :timestamp,
    :alert_type,
    :current_activity,
    :baseline_activity,
    :reason
)
ON DUPLICATE KEY UPDATE
    current_activity = VALUES(current_activity),
    baseline_activity = VALUES(baseline_activity),
    reason = VALUES(reason)
""")


records = df.to_dict(orient="records")


print(
    f"Inserting {len(records):,} alert records..."
)


with engine.begin() as conn:

    conn.execute(
        insert_sql,
        records
    )


# ============================================================
# VERIFY
# ============================================================

with engine.connect() as conn:

    count = conn.execute(
        text("""
            SELECT COUNT(*)
            FROM activity_alerts
        """)
    ).scalar()

print(
    f"activity_alerts table contains: {count:,} rows"
)

print("Activity alerts loaded successfully.")