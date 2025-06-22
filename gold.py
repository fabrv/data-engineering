import sqlite3

DB_PATH = "database.db"

def create_gold_tables(conn):
    cur = conn.cursor()
    # Remove rows with NULL started_time
    cur.execute('DELETE FROM trips WHERE started_time IS NULL')
    # trips_per_station_day
    cur.execute('''
        CREATE TABLE IF NOT EXISTS trips_per_station_day AS
        SELECT 
            start_station_name,
            DATE(started_time) AS trip_date,
            COUNT(*) AS trip_count
        FROM trips
        GROUP BY start_station_name, trip_date
    ''')
    # avg_duration_by_hour
    cur.execute('''
        CREATE TABLE IF NOT EXISTS avg_duration_by_hour AS
        SELECT 
            strftime('%H', started_time) AS hour,
            AVG(trip_duration) AS avg_duration
        FROM trips
        GROUP BY hour
    ''')
    # top_stations_monthly
    cur.execute('''
        CREATE TABLE IF NOT EXISTS top_stations_monthly AS
        SELECT 
            strftime('%Y', started_time) AS year,
            strftime('%m', started_time) AS month,
            start_station_name,
            COUNT(*) AS trip_count
        FROM trips
        GROUP BY year, month, start_station_name
    ''')
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    create_gold_tables(conn)
    conn.close()
    print("Gold tables created in database.sql.")

if __name__ == "__main__":
    main()
