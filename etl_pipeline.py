import pandas as pd
import sqlite3
import os
from datetime import datetime

DB_NAME = "survey.db"
LOG_FILE = "etl_log.txt"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    with open(LOG_FILE, "a") as f:
        f.write(log_entry + "\n")

# ─── EXTRACT ───
def extract_from_csv(filepath):
    log(f"EXTRACT: Reading data from {filepath}")
    try:
        df = pd.read_csv(filepath)
        log(f"EXTRACT: Successfully read {len(df)} records")
        return df
    except Exception as e:
        log(f"EXTRACT ERROR: {e}")
        return None

# ─── TRANSFORM ───
def transform(df):
    log("TRANSFORM: Starting data transformation")

    # Drop duplicates
    before = len(df)
    df = df.drop_duplicates()
    log(f"TRANSFORM: Removed {before - len(df)} duplicate rows")

    # Fill missing values
    df['comments'] = df['comments'].fillna("No comment provided")
    df['department'] = df['department'].fillna("Unknown")

    # Validate score columns are between 1-5
    score_cols = ['work_life_balance', 'job_satisfaction', 'team_collaboration',
                  'management_support', 'career_growth', 'overall_engagement']
    for col in score_cols:
        if col in df.columns:
            df[col] = df[col].clip(1, 5)
            log(f"TRANSFORM: Validated and clipped column '{col}' to range 1-5")

    # Add derived column
    df['engagement_category'] = df['overall_engagement'].apply(
        lambda x: 'High' if x >= 4 else ('Medium' if x >= 3 else 'Low')
    )
    log("TRANSFORM: Added engagement_category column")

    # Add ETL timestamp
    df['etl_processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("TRANSFORM: Added ETL processing timestamp")

    log(f"TRANSFORM: Transformation complete. {len(df)} records ready to load")
    return df

# ─── LOAD ───
def load_to_db(df):
    log("LOAD: Loading transformed data into SQLite database")
    try:
        conn = sqlite3.connect(DB_NAME)
        df.to_sql("etl_processed", conn, if_exists="replace", index=False)
        conn.close()
        log(f"LOAD: Successfully loaded {len(df)} records into 'etl_processed' table")
        return True
    except Exception as e:
        log(f"LOAD ERROR: {e}")
        return False

# ─── EXPORT ───
def export_to_csv(df):
    output_path = "etl_output.csv"
    df.to_csv(output_path, index=False)
    log(f"EXPORT: Saved transformed data to {output_path}")

# ─── FULL PIPELINE ───
def run_etl(filepath="survey_export.csv"):
    log("=" * 50)
    log("ETL PIPELINE STARTED")
    log("=" * 50)

    # Step 1: Extract
    df = extract_from_csv(filepath)
    if df is None:
        log("ETL FAILED: Could not extract data")
        return False

    # Step 2: Transform
    df = transform(df)

    # Step 3: Load
    success = load_to_db(df)

    # Step 4: Export
    export_to_csv(df)

    log("=" * 50)
    log("ETL PIPELINE COMPLETED SUCCESSFULLY")
    log("=" * 50)
    return df

if __name__ == "__main__":
    run_etl()
    