import pandas as pd
import sqlite3
from datetime import datetime

DB_NAME = "survey.db"

def run_quality_checks():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM surveys", conn)
    conn.close()

    results = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Check 1: No empty names
    empty_names = df['employee_name'].isna().sum() + (df['employee_name'] == '').sum()
    results.append({
        "check": "No Empty Employee Names",
        "status": "PASS" if empty_names == 0 else "FAIL",
        "details": f"{empty_names} empty name(s) found",
        "timestamp": timestamp
    })

    # Check 2: All scores within range 1-5
    score_cols = ['work_life_balance', 'job_satisfaction', 'team_collaboration',
                  'management_support', 'career_growth', 'overall_engagement']
    out_of_range = 0
    for col in score_cols:
        out_of_range += ((df[col] < 1) | (df[col] > 5)).sum()
    results.append({
        "check": "All Scores Within Range (1-5)",
        "status": "PASS" if out_of_range == 0 else "FAIL",
        "details": f"{out_of_range} out-of-range value(s) found",
        "timestamp": timestamp
    })

    # Check 3: No duplicate submissions
    duplicates = df.duplicated(subset=['employee_name', 'submitted_at']).sum()
    results.append({
        "check": "No Duplicate Submissions",
        "status": "PASS" if duplicates == 0 else "FAIL",
        "details": f"{duplicates} duplicate(s) found",
        "timestamp": timestamp
    })

    # Check 4: All departments are valid
    valid_depts = ["HR", "Engineering", "Sales", "Marketing", "Finance", "Operations"]
    invalid_depts = df[~df['department'].isin(valid_depts)].shape[0]
    results.append({
        "check": "All Departments Are Valid",
        "status": "PASS" if invalid_depts == 0 else "FAIL",
        "details": f"{invalid_depts} invalid department(s) found",
        "timestamp": timestamp
    })

    # Check 5: Minimum response count
    total = len(df)
    results.append({
        "check": "Minimum 1 Survey Response Exists",
        "status": "PASS" if total >= 1 else "FAIL",
        "details": f"{total} total response(s) in database",
        "timestamp": timestamp
    })

    # Check 6: Anomaly detection - flag very low engagement
    low_engagement = df[df['overall_engagement'] <= 2].shape[0]
    results.append({
        "check": "Anomaly Detection: Low Engagement Alert",
        "status": "WARNING" if low_engagement > 0 else "PASS",
        "details": f"{low_engagement} employee(s) with overall engagement <= 2",
        "timestamp": timestamp
    })

    return pd.DataFrame(results), df

def get_summary_stats(df):
    score_cols = ['work_life_balance', 'job_satisfaction', 'team_collaboration',
                  'management_support', 'career_growth', 'overall_engagement']
    stats = df[score_cols].describe().round(2)
    return stats
