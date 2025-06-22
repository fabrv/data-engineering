import polars as pl
import os
import psycopg2
from psycopg2 import sql
from pathlib import Path

SILVER_DIR = Path("data/silver")

# Database connection settings (edit as needed)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'your_db',
    'user': 'your_user',
    'password': 'your_password'
}

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS trips (
                trip_duration INTEGER,
                started_time TIMESTAMP,
                start_station_name TEXT,
                user_type TEXT
            )
        ''')
        conn.commit()

def load_csv_to_db(conn, csv_path):
    df = pl.read_csv(csv_path)
    with conn.cursor() as cur:
        for row in df.iter_rows(named=True):
            cur.execute(
                'INSERT INTO trips (trip_duration, started_time, start_station_name, user_type) VALUES (%s, %s, %s, %s)',
                (row['trip_duration'], row['started_time'], row['start_station_name'], row['user_type'])
            )
        conn.commit()

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    create_table_if_not_exists(conn)
    for fname in os.listdir(SILVER_DIR):
        if fname.endswith('.csv'):
            print(f"Loading {fname}...")
            load_csv_to_db(conn, SILVER_DIR / fname)
    conn.close()
    print("All files loaded.")

if __name__ == "__main__":
    main()
