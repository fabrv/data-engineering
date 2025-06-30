import sqlite3
from pathlib import Path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

DB_PATH = Path("data/database.db")

DDL = """
-- limpiar registros sin timestamp válido
DELETE FROM trips WHERE start_time IS NULL;

-- trips_per_station_day
DROP TABLE IF EXISTS trips_per_station_day;
CREATE TABLE trips_per_station_day AS
SELECT 
    start_station_name,
    DATE(start_time) AS trip_date,
    COUNT(*) AS trip_count
FROM trips
GROUP BY start_station_name, trip_date;

-- avg_duration_by_hour
DROP TABLE IF EXISTS avg_duration_by_hour;
CREATE TABLE avg_duration_by_hour AS
SELECT 
    strftime('%H', start_time) AS hour,
    AVG(trip_duration) AS avg_duration
FROM trips
GROUP BY hour;

-- top_stations_monthly
DROP TABLE IF EXISTS top_stations_monthly;
CREATE TABLE top_stations_monthly AS
WITH monthly AS (
    SELECT 
        strftime('%Y', start_time) AS year,
        strftime('%m', start_time) AS month,
        start_station_name,
        COUNT(*) AS trip_count,
        ROW_NUMBER() OVER (
            PARTITION BY strftime('%Y', start_time), strftime('%m', start_time)
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM trips
    GROUP BY year, month, start_station_name
)
SELECT * FROM monthly WHERE rn <= 10; -- top-10 por mes
"""

def create_gold_tables(conn: sqlite3.Connection):
    conn.executescript(DDL)
    conn.commit()

def main():
    conn = sqlite3.connect(str(DB_PATH))
    create_gold_tables(conn)
    conn.close()
    print("Tablas Gold creadas en database.db")

@transformer
def transform(data, *args, **kwargs):
    main()
    return {
        "status": "completed", 
        "database_path": str(DB_PATH), 
        "message": "Tablas Gold creadas en database.db"
    }

if __name__ == "__main__":
    main()