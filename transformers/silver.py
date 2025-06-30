import polars as pl, re
from pathlib import Path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

BRONZE = Path("data/bronze")
SILVER = Path("data/silver"); SILVER.mkdir(parents=True, exist_ok=True)
NULLS = ["\\N", "NULL", ""]

# ───────── helpers notebook + sinónimos 2020+ ─────────
SYNONYMS = {
    "tripduration": "trip_duration",
    "trip_duration_seconds": "trip_duration",
    "duration_sec": "trip_duration",
    "ride_length": "trip_duration",
    "bikeid": "bike_id",
    "ride_id": "ride_id",
    "starttime": "start_time", "started_at": "start_time",
    "stoptime":  "stop_time",  "ended_at":   "stop_time",
    "usertype": "user_type", "member_casual": "user_type",
    "birth year": "birth_year",
}
def to_snake(s): return s.strip().lower().replace(" ", "_")
def normalize(df):
    df = df.rename({c: to_snake(c) for c in df.columns})
    return df.rename({c: SYNONYMS.get(c, c) for c in df.columns})
# ------------------------------------------------------

def parse_dates(df):
    if {"start_time","stop_time"}.issubset(df.columns):
        df = df.with_columns([
            pl.coalesce([
                pl.col("start_time").cast(pl.Utf8, strict=False)
                                     .str.strptime(pl.Datetime,"%Y-%m-%d %H:%M:%S",
                                                   strict=False, exact=False),
                pl.col("start_time").cast(pl.Utf8, strict=False)
                                     .str.strptime(pl.Datetime,"%m/%d/%Y %H:%M:%S",
                                                   strict=False, exact=False),
                pl.col("start_time").cast(pl.Utf8, strict=False)
                                     .str.strptime(pl.Datetime,"%Y-%m-%d %H:%M:%S.%f",
                                                   strict=False, exact=False),
            ]).alias("start_time"),
            pl.coalesce([
                pl.col("stop_time").cast(pl.Utf8, strict=False)
                                   .str.strptime(pl.Datetime,"%Y-%m-%d %H:%M:%S",
                                                 strict=False, exact=False),
                pl.col("stop_time").cast(pl.Utf8, strict=False)
                                   .str.strptime(pl.Datetime,"%m/%d/%Y %H:%M:%S",
                                                 strict=False, exact=False),
                pl.col("stop_time").cast(pl.Utf8, strict=False)
                                   .str.strptime(pl.Datetime,"%Y-%m-%d %H:%M:%S.%f",
                                                 strict=False, exact=False),
            ]).alias("stop_time"),
        ])
    return df

# ───────── nuevo: calcula trip_duration si hace falta ─────────
def ensure_trip_duration(df):
    if "trip_duration" not in df.columns and {"start_time","stop_time"}.issubset(df.columns):
        df = df.with_columns(
            (pl.col("stop_time")-pl.col("start_time")).dt.total_seconds().alias("trip_duration")
        )
    return df
# -------------------------------------------------------------

def cast_birth_year(df):
    if "birth_year" in df.columns:
        df = df.with_columns(
            pl.col("birth_year").cast(pl.Float64, strict=False).cast(pl.Int64, strict=False)
        )
    return df

def trim_and_null(df):
    txt = [c for c,t in df.schema.items() if t == pl.Utf8]
    return df.with_columns([
        pl.when(pl.col(c).cast(pl.Utf8, strict=False).str.strip() == "")
          .then(None)
          .otherwise(pl.col(c).cast(pl.Utf8, strict=False).str.strip())
          .alias(c)
        for c in txt
    ])

def drop_duplicates(df):
    key_id = "bike_id" if "bike_id" in df.columns else "ride_id" if "ride_id" in df.columns else None
    subset = ["start_time","start_station_id","stop_time"]
    if key_id: subset.insert(1,key_id)
    return df.unique(subset=subset, keep="first")

def quality(df):
    return (df
            .filter(pl.col("trip_duration") > 0)
            .filter(pl.col("stop_time") >= pl.col("start_time")))

KEEP = ["trip_duration","start_time","start_station_name","user_type"]
ID_COLS = ["start_station_id","end_station_id","bike_id","ride_id"]

def process_year(year, files):
    out = SILVER / f"{year}-citibike.csv"
    if out.exists():
        print("[SKIP]", out); return
    dfs=[]
    for f in files:
        df = pl.read_csv(
                f, null_values=NULLS, infer_schema_length=10_000,
                ignore_errors=True)
        df = normalize(df)
        for c in ID_COLS:
            if c in df.columns:
                df = df.with_columns(pl.col(c).cast(pl.Int64, strict=False))
        df = (parse_dates(df)
              .pipe(ensure_trip_duration)
              .pipe(cast_birth_year)
              .pipe(trim_and_null)
              .pipe(drop_duplicates)
              .pipe(quality))
        dfs.append(df)

    if dfs:
        final = pl.concat(dfs, how="vertical_relaxed")
        final.select([c for c in KEEP if c in final.columns]).write_csv(out)
        print(f"[OK] {year}: filas {len(final)} → {out}")
    else:
        print(f"[WARN] {year}: sin datos válidos")

def main():
    years={str(y) for y in range(2013,2024)}
    by={}
    for csv in BRONZE.glob("*.csv"):
        y=csv.name[:4]
        if y in years: by.setdefault(y,[]).append(csv)
    for y,files in sorted(by.items()):
        process_year(y,files)

@transformer
def transform_data(data, *args, **kwargs):
    main()
    return {"status": "completed"}