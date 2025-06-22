import polars as pl
import os
import sqlite3
from pathlib import Path

SILVER_DIR = Path("data/silver")
DB_PATH = "database.db"

def create_table_if_not_exists(conn):
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS trips (
            trip_duration INTEGER,
            started_time TEXT,
            start_station_name TEXT,
            user_type TEXT
        )
    ''')
    conn.commit()

def load_csv_to_db(conn, csv_path):
    df = pl.read_csv(csv_path)
    cur = conn.cursor()
    for row in df.iter_rows(named=True):
        cur.execute(
            'INSERT INTO trips (trip_duration, started_time, start_station_name, user_type) VALUES (?, ?, ?, ?)',
            (row['trip_duration'], row['started_time'], row['start_station_name'], row['user_type'])
        )
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    create_table_if_not_exists(conn)
    for fname in os.listdir(SILVER_DIR):
        if fname.endswith('.csv'):
            print(f"Loading {fname}...")
            load_csv_to_db(conn, SILVER_DIR / fname)
    conn.close()
    print("All files loaded into database.db.")

if __name__ == "__main__":
    main()
