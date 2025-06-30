import polars as pl, os, sqlite3
from pathlib import Path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

SILVER_DIR = Path("data/silver")
DB_PATH = Path("data/database.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DDL = """
DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
    trip_duration INTEGER,
    start_time TEXT,
    start_station_name TEXT,
    user_type TEXT
);
"""

INSERT = """
INSERT INTO trips (trip_duration, start_time, start_station_name, user_type)
VALUES (?, ?, ?, ?)
"""

def create_table(conn: sqlite3.Connection):
    conn.executescript(DDL)
    conn.commit()

def load_csv(conn: sqlite3.Connection, csv_path: Path):
    df = pl.read_csv(csv_path)
    expected = {"trip_duration", "start_time", "start_station_name", "user_type"}
    if not expected.issubset(df.columns):
        missing = expected - set(df.columns)
        print(f"[WARN] {csv_path.name}: falta/n {', '.join(missing)} – OMITIDO")
        return
    
    # Procesar en lotes para evitar problemas de memoria
    df_selected = df.select("trip_duration", "start_time", "start_station_name", "user_type")
    total_rows = len(df_selected)
    batch_size = 10000
    
    for i in range(0, total_rows, batch_size):
        batch = df_selected.slice(i, batch_size)
        rows = batch.to_numpy().tolist()
        conn.executemany(INSERT, rows)
        conn.commit()
    
    print(f"[OK] {csv_path.name}: {total_rows:,} filas insertadas")
    
    # Liberar memoria explícitamente
    del df, df_selected

def main():
    conn = sqlite3.connect(str(DB_PATH))
    create_table(conn)
    
    for fname in sorted(os.listdir(SILVER_DIR)):
        if fname.endswith(".csv"):
            load_csv(conn, SILVER_DIR / fname)
    
    conn.close()
    print("Todos los archivos cargados en database.db")

@data_exporter
def export_data(data, *args, **kwargs):
    main()
    return {"status": "completed", "database_path": str(DB_PATH), "message": "Todos los archivos cargados en database.db"}

if __name__ == "__main__":
    main()