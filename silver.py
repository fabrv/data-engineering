import polars as pl
import os
import re
from pathlib import Path

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")
SILVER_DIR.mkdir(parents=True, exist_ok=True)

# Helper functions
def to_snake_case(s):
    s = s.strip().lower().replace(" ", "_")
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip('_')

def standardize_columns(df):
    # Lowercase and snake_case
    df = df.rename({col: to_snake_case(col) for col in df.columns})
    # Standardize time columns
    time_map = {
        'starttime': 'started_time', 'start_time': 'started_time', 'started_at': 'started_time',
        'stoptime': 'stop_time', 'stop_time': 'stop_time', 'ended_at': 'stop_time'
    }
    for old, new in time_map.items():
        if old in df.columns:
            df = df.rename({old: new})
    # Standardize tripduration
    if 'tripduration' in df.columns:
        df = df.rename({'tripduration': 'trip_duration'})
    # Standardize bikeid
    if 'bikeid' in df.columns:
        df = df.rename({'bikeid': 'bike_id'})
    # convert end_station_id to int, if its string convert to 0 
    if 'end_station_id' in df.columns:
        df = df.with_columns([
            pl.col('end_station_id')
            .cast(pl.Float32, strict=False)
            .fill_null(0)
            .map_elements(lambda x: x if x is not None else 0, return_dtype=pl.Float32)
            .alias('end_station_id')
        ])
    # Standardize user_type
    user_type_col = None
    for c in ['usertype', 'user_type', 'member_casual']:
        if c in df.columns:
            user_type_col = c
            break
    if user_type_col:
        df = df.rename({user_type_col: 'user_type'})
    return df

def ensure_trip_duration(df):
    if 'trip_duration' not in df.columns:
        if 'started_time' in df.columns and 'stop_time' in df.columns:
            # convert stop_time and started_time to datetime with format and strict=False
            df = df.with_columns([
                pl.col('started_time').str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.3f", strict=False).alias('started_time'),
                pl.col('stop_time').str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.3f", strict=False).alias('stop_time')
            ])
            # calculate trip_duration in seconds
            df = df.with_columns(
                (pl.col('stop_time') - pl.col('started_time')).dt.total_seconds().alias('trip_duration')
            )
    return df

def process_year(year, files):
    chunk_size = 15
    if len(files) > chunk_size:
        for i, chunk_start in enumerate(range(0, len(files), chunk_size), 1):
            chunk_files = files[chunk_start:chunk_start+chunk_size]
            out_path = SILVER_DIR / f"{year}-citibike_{i}.csv"
            if out_path.exists():
                print(f"Skipping chunk {i} for year {year}, {out_path} already exists.")
                continue
            print(f"Processing year {year} chunk {i} with {len(chunk_files)} files")
            _process_and_save(chunk_files, out_path)
    else:
        out_path = SILVER_DIR / f"{year}-citibike.csv"
        if out_path.exists():
            print(f"Skipping year {year}, {out_path} already exists.")
            return
        print(f"Processing year {year} with {len(files)} files")
        _process_and_save(files, out_path)

def _process_and_save(files, out_path):
    dfs = []
    for f in files:
        try:
            df = pl.read_csv(
                f,
                null_values=["\\N", "NULL"],
                schema_overrides={
                    "start_station_id": pl.Utf8,
                    "end_station_id": pl.Utf8
                }
            )
            df = standardize_columns(df)
            df = ensure_trip_duration(df)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
    if not dfs:
        return
    df = pl.concat(dfs, how='vertical_relaxed')
    keep = ['trip_duration', 'started_time', 'start_station_name', 'user_type']
    df = df.select([c for c in keep if c in df.columns])
    df = df.drop_nulls()
    df = df.unique()
    df.write_csv(out_path)
    print(f"Saved {out_path}")

def main():
    print("Starting processing of Citibike data")
    YEARS_TO_PROCESS = [str(y) for y in range(2013, 2024)]  # Change this range as needed
    files_by_year = {}
    for fname in os.listdir(BRONZE_DIR):
        if fname.endswith('.csv'):
            year = fname[:4]
            if year.isdigit() and year in YEARS_TO_PROCESS:
                files_by_year.setdefault(year, []).append(BRONZE_DIR / fname)
    for year, files in sorted(files_by_year.items()):
        out_path = SILVER_DIR / f"{year}-citibike.csv"
        if out_path.exists():
            print(f"Skipping year {year}, {out_path} already exists.")
            continue
        print(f"Processing year {year} with {len(files)} files")
        process_year(year, files)

if __name__ == "__main__":
    main()
