import sqlite3
from pathlib import Path
from datetime import datetime
import sys
sys.path.append('/app/citibike_project')
from utils.slack_notifier import notify_failure, notify_success, notify_completion

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

DB_PATH = Path("/app/data/database.db")

# ---------- DDL -------------------------------------------------
DDL = """
DELETE FROM trips WHERE start_time IS NULL;

/* trips_per_station_day */
DROP TABLE IF EXISTS trips_per_station_day;
CREATE TABLE trips_per_station_day AS
SELECT
    start_station_name,
    DATE(start_time) AS trip_date,
    COUNT(*) AS trip_count
FROM trips
WHERE start_station_name IS NOT NULL
GROUP BY start_station_name, trip_date;

/* avg_duration_by_hour */
DROP TABLE IF EXISTS avg_duration_by_hour;
CREATE TABLE avg_duration_by_hour AS
SELECT
    substr(start_time,12,2) AS hour,               -- HH
    AVG(trip_duration)      AS avg_duration,
    COUNT(*)                AS trip_count
FROM trips
WHERE trip_duration BETWEEN 1 AND 86399            -- 1 s – 24 h
GROUP BY hour
ORDER BY hour;

/* top_stations_monthly */
DROP TABLE IF EXISTS top_stations_monthly;
CREATE TABLE top_stations_monthly AS
WITH monthly AS (
    SELECT
        substr(start_time,1,7) AS year_month,      -- YYYY-MM
        start_station_name,
        COUNT(*)              AS trip_count,
        ROW_NUMBER() OVER (
            PARTITION BY substr(start_time,1,7)
            ORDER BY COUNT(*) DESC
        ) AS rank
    FROM trips
    WHERE start_station_name IS NOT NULL
    GROUP BY year_month, start_station_name
)
SELECT *
FROM monthly
WHERE rank <= 10
ORDER BY year_month, rank;

/* user_type_summary */
DROP TABLE IF EXISTS user_type_summary;
CREATE TABLE user_type_summary AS
SELECT
    user_type,
    COUNT(*)           AS total_trips,
    AVG(trip_duration) AS avg_duration,
    MIN(trip_duration) AS min_duration,
    MAX(trip_duration) AS max_duration
FROM trips
WHERE user_type IS NOT NULL
GROUP BY user_type;

/* indexes */
CREATE INDEX IF NOT EXISTS idx_trips_date
  ON trips_per_station_day(trip_date);
CREATE INDEX IF NOT EXISTS idx_trips_station
  ON trips_per_station_day(start_station_name);
CREATE INDEX IF NOT EXISTS idx_top_stations
  ON top_stations_monthly(year_month);
"""

# ---------- Core -----------------------------------------------
def create_gold_tables(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()

    # Asegura que exista la tabla de log antes de usarla
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_process_log (
            process_timestamp       TEXT PRIMARY KEY,
            tables_created          INTEGER,
            total_trips_processed   INTEGER
        )
    """)
    conn.commit()

    # Trip count from load_control
    print("Gold step: counting trips ...")
    cur.execute("SELECT COALESCE(SUM(rows_loaded),0) FROM load_control")
    total_trips = cur.fetchone()[0]

    # Already processed?
    cur.execute("SELECT total_trips_processed FROM gold_process_log "
                "ORDER BY process_timestamp DESC LIMIT 1")
    last = cur.fetchone()
    if last and last[0] == total_trips:
        print(f"Gold tables already up to date ({total_trips} trips)")
        return False

    print(f"Creating Gold tables from {total_trips} trips ...")
    conn.executescript(DDL)

    # Log
    cur.execute(
        "INSERT INTO gold_process_log "
        "(process_timestamp, tables_created, total_trips_processed) "
        "VALUES (?, 4, ?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), total_trips)
    )
    conn.commit()

    # Summary
    for tbl in (
        "trips_per_station_day",
        "avg_duration_by_hour",
        "top_stations_monthly",
        "user_type_summary"
    ):
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"  {tbl}: {cur.fetchone()[0]} rows")

    return True

def main():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        if create_gold_tables(conn):
            print("Gold tables created successfully")
    except Exception as exc:
        print(f"Error creating gold tables: {exc}")
        raise
    finally:
        conn.close()

@transformer
def transform(data, *args, **kwargs):
    try:
        main()
        
        # Success notification for gold step
        notify_success("Gold Analytics", {
            "tables_created": "4 analytical tables",
            "database": str(DB_PATH),
            "status": "Analytics ready"
        })
        
        # Pipeline completion notification
        notify_completion(5, "ETL pipeline completed")
        
        return {
            "status": "completed",
            "database_path": str(DB_PATH),
            "message": "Gold tables processed"
        }
    except Exception as e:
        notify_failure("Gold Analytics", str(e), {
            "step": "create_analytical_tables",
            "database": str(DB_PATH)
        })
        raise

if __name__ == "__main__":
    main()
