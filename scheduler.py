import time
import schedule
from datetime import datetime
from database import get_all_responses
from etl_pipeline import run_etl
from data_quality import run_quality_checks
import pandas as pd

LOG_FILE = "scheduler_log.txt"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    with open(LOG_FILE, "a") as f:
        f.write(entry + "\n")

def export_and_run_etl():
    log("SCHEDULER: Starting scheduled ETL job")
    try:
        df = get_all_responses()
        if df.empty:
            log("SCHEDULER: No data found. Skipping ETL.")
            return
        df.to_csv("survey_export.csv", index=False)
        log("SCHEDULER: Exported survey data to CSV")
        run_etl("survey_export.csv")
        log("SCHEDULER: ETL job completed successfully")
    except Exception as e:
        log(f"SCHEDULER ERROR in ETL job: {e}")

def run_quality_job():
    log("SCHEDULER: Starting scheduled Data Quality job")
    try:
        results, _ = run_quality_checks()
        failed = results[results['status'] == 'FAIL']
        warnings = results[results['status'] == 'WARNING']
        log(f"SCHEDULER: Quality checks done. {len(results)} checks, {len(failed)} failed, {len(warnings)} warnings")
    except Exception as e:
        log(f"SCHEDULER ERROR in Quality job: {e}")

def generate_daily_report():
    log("SCHEDULER: Generating daily report")
    try:
        df = get_all_responses()
        if df.empty:
            log("SCHEDULER: No data for report")
            return
        report = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "total_responses": len(df),
            "avg_engagement": round(df['overall_engagement'].mean(), 2),
            "top_department": df.groupby('department')['overall_engagement'].mean().idxmax()
        }
        log(f"SCHEDULER: Daily Report — {report}")
    except Exception as e:
        log(f"SCHEDULER ERROR in report: {e}")

def start_scheduler():
    log("SCHEDULER: Starting pipeline orchestrator")
    log("SCHEDULER: Jobs scheduled — ETL every 10 min, Quality checks every 30 min, Report daily")

    schedule.every(10).minutes.do(export_and_run_etl)
    schedule.every(30).minutes.do(run_quality_job)
    schedule.every().day.at("08:00").do(generate_daily_report)

    # Run all jobs once immediately on start
    export_and_run_etl()
    run_quality_job()
    generate_daily_report()

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    start_scheduler()
    