import polars as pl, os, sqlite3
from pathlib import Path
from datetime import datetime

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# -------------------------------------------------
#  PATHS & CONSTANTS
# -------------------------------------------------
SILVER_DIR = Path("/app/data/silver")
DB_PATH    = Path("/app/data/database.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

CSV_COLS = ["trip_duration", "start_time", "start_station_name", "user_type"]

# -------------------------------------------------
#  SCHEMA (DDL)
# -------------------------------------------------
DDL_TRIPS = """
CREATE TABLE IF NOT EXISTS trips (
    trip_duration       INTEGER,
    start_time          TEXT,
    start_station_name  TEXT,
    user_type           TEXT
);
"""

DDL_QUALITY = """
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    year                     INTEGER,
    file_name                TEXT,
    total_records            INTEGER,
    null_duration            INTEGER,
    null_start_time          INTEGER,
    null_station             INTEGER,
    null_user_type           INTEGER,
    pct_null_duration        REAL,
    pct_null_start_time      REAL,
    pct_null_station         REAL,
    pct_null_user_type       REAL,
    records_loaded           INTEGER,
    load_timestamp           TEXT
);
"""

DDL_CONTROL = """
CREATE TABLE IF NOT EXISTS load_control (
    file_name     TEXT PRIMARY KEY,
    rows_loaded   INTEGER,
    load_timestamp TEXT
);
"""

# -------------------------------------------------
#  INSERT STATEMENTS
# -------------------------------------------------
INSERT_TRIPS = """
INSERT INTO trips (trip_duration, start_time, start_station_name, user_type)
VALUES (?, ?, ?, ?)
"""

INSERT_QUALITY = """
INSERT INTO data_quality_metrics (
    year, file_name, total_records,
    null_duration, null_start_time, null_station, null_user_type,
    pct_null_duration, pct_null_start_time, pct_null_station, pct_null_user_type,
    records_loaded, load_timestamp
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

INSERT_CONTROL = """
INSERT INTO load_control (file_name, rows_loaded, load_timestamp)
VALUES (?, ?, ?)
"""

# -------------------------------------------------
#  HELPERS
# -------------------------------------------------

def create_tables(conn: sqlite3.Connection):
    conn.executescript(DDL_TRIPS)
    conn.executescript(DDL_QUALITY)
    conn.executescript(DDL_CONTROL)
    conn.commit()


def is_file_loaded(conn: sqlite3.Connection, file_name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM load_control WHERE file_name = ?", (file_name,))
    return cur.fetchone() is not None

# -------------------------------------------------
#  LOADER CORE
# -------------------------------------------------

def load_csv(conn: sqlite3.Connection, csv_path: Path):
    if is_file_loaded(conn, csv_path.name):
        print(f"[SKIP] {csv_path.name}: already loaded")
        return

    print(f"Loading {csv_path.name} …")

    try:
        # quick schema validation
        sample_df = pl.read_csv(csv_path, n_rows=1_000, null_values=[""])
        if not set(CSV_COLS).issubset(sample_df.columns):
            missing = set(CSV_COLS) - set(sample_df.columns)
            print(f"[WARN] {csv_path.name}: missing {', '.join(missing)} – skipped")
            return

        reader = pl.read_csv_batched(csv_path, batch_size=50_000, null_values=[""])

        total_loaded = 0
        null_counts  = {c: 0 for c in CSV_COLS}

        while True:
            batches = reader.next_batches(1)
            if not batches:
                break
            batch = batches[0]
            batch = batch.select(CSV_COLS)

            # accumulate nulls
            for c in CSV_COLS:
                null_counts[c] += batch[c].null_count()

            conn.executemany(INSERT_TRIPS, batch.iter_rows())
            conn.commit()

            total_loaded += batch.height
            if total_loaded % 500_000 == 0:
                print(f"  {total_loaded} rows so far…")

        if total_loaded == 0:
            print(f"[WARN] {csv_path.name}: empty after filtering")
            return

        # percentages
        pct_null = {f"pct_null_{c.split('_')[0] if c!='start_station_name' else 'station'}":
                     null_counts[c] * 100.0 / total_loaded for c in CSV_COLS}

        # insert metrics row
        conn.execute(
            INSERT_QUALITY,
            (
                int(csv_path.name[:4]) if csv_path.name[:4].isdigit() else 0,
                csv_path.name,
                total_loaded,
                null_counts["trip_duration"],
                null_counts["start_time"],
                null_counts["start_station_name"],
                null_counts["user_type"],
                pct_null["pct_null_trip"],
                pct_null["pct_null_start"],
                pct_null["pct_null_station"],
                pct_null["pct_null_user"],
                total_loaded,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

        # control table
        conn.execute(
            INSERT_CONTROL,
            (csv_path.name, total_loaded, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        print(f"[OK] {csv_path.name}: {total_loaded} rows loaded")

    except Exception as exc:
        print(f"[ERROR] {csv_path.name}: {exc}")
        conn.rollback()

# -------------------------------------------------
#  MAIN ORCHESTRATOR
# -------------------------------------------------

def main():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")
        conn.execute("PRAGMA temp_store = MEMORY")
        print(f"Connected to {DB_PATH}")
    except Exception as exc:
        print(f"DB connection error: {exc}")
        raise

    create_tables(conn)

    if not SILVER_DIR.exists():
        print(f"[ERROR] {SILVER_DIR} does not exist")
        conn.close()
        return

    csv_files = [f for f in sorted(os.listdir(SILVER_DIR)) if f.endswith(".csv")]
    if not csv_files:
        print("[WARN] No CSV files found to load")

    loaded = skipped = 0
    for fname in csv_files:
        if is_file_loaded(conn, fname):
            skipped += 1
        else:
            load_csv(conn, SILVER_DIR / fname)
            loaded += 1

    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(rows_loaded), 0) FROM load_control")
    approx_total = cur.fetchone()[0]

    print("\nLoad summary")
    print(f"Files loaded:  {loaded}")
    print(f"Files skipped: {skipped}")
    print(f"Total files:   {len(csv_files)}")
    print(f"Approx trips in DB: {approx_total}")

    conn.close()
    print("Load finished")

# -------------------------------------------------
#  MAGE WRAPPER
# -------------------------------------------------

@data_exporter
def export_data(data, *args, **kwargs):
    try:
        main()
        return {
            "status": "completed",
            "database_path": str(DB_PATH),
            "message": "Load finished",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "database_path": str(DB_PATH),
        }

if __name__ == "__main__":
    main()