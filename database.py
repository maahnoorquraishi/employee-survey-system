import sqlite3
import pandas as pd
from datetime import datetime

DB_NAME = "survey.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS surveys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_name TEXT,
            department TEXT,
            role TEXT,
            work_life_balance INTEGER,
            job_satisfaction INTEGER,
            team_collaboration INTEGER,
            management_support INTEGER,
            career_growth INTEGER,
            overall_engagement INTEGER,
            comments TEXT,
            submitted_at TEXT
        )
    ''')
    conn.commit()
    conn.close()

def insert_response(data: dict):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        INSERT INTO surveys (
            employee_name, department, role,
            work_life_balance, job_satisfaction, team_collaboration,
            management_support, career_growth, overall_engagement,
            comments, submitted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data['employee_name'], data['department'], data['role'],
        data['work_life_balance'], data['job_satisfaction'],
        data['team_collaboration'], data['management_support'],
        data['career_growth'], data['overall_engagement'],
        data['comments'], datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))
    conn.commit()
    conn.close()

def get_all_responses():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM surveys", conn)
    conn.close()
    return df

def get_department_avg():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query('''
        SELECT department,
            ROUND(AVG(work_life_balance), 2) as Work_Life_Balance,
            ROUND(AVG(job_satisfaction), 2) as Job_Satisfaction,
            ROUND(AVG(team_collaboration), 2) as Team_Collaboration,
            ROUND(AVG(management_support), 2) as Management_Support,
            ROUND(AVG(career_growth), 2) as Career_Growth,
            ROUND(AVG(overall_engagement), 2) as Overall_Engagement
        FROM surveys GROUP BY department
    ''', conn)
    conn.close()
    return df
