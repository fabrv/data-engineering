import polars as pl, re, gc, sqlite3
from pathlib import Path
from datetime import datetime
import sys
sys.path.append('/app/citibike_project')
from utils.slack_notifier import notify_failure, notify_success

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

BRONZE = Path("data/bronze")
SILVER = Path("data/silver"); SILVER.mkdir(parents=True, exist_ok=True)
DB_PATH = Path("/app/data/database.db")  # Same path as Load
NULLS = ["\\N", "NULL", ""]

# Helpers and synonyms for 2020+ data
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


def to_snake(s):
    return s.strip().lower().replace(" ", "_")


def normalize(df):
    df = df.rename({c: to_snake(c) for c in df.columns})
    return df.rename({c: SYNONYMS.get(c, c) for c in df.columns})


def parse_dates(df):
    if {"start_time", "stop_time"}.issubset(df.columns):
        # Clean corrupted values before parsing dates
        df = df.with_columns([
            pl.when(
                pl.col("start_time").cast(pl.Utf8, strict=False).str.contains("^0+$") |
                pl.col("start_time").cast(pl.Utf8, strict=False).str.lengths() < 10 |
                pl.col("start_time").is_null()
            )
            .then(None)
            .otherwise(pl.col("start_time"))
            .alias("start_time"),

            pl.when(
                pl.col("stop_time").cast(pl.Utf8, strict=False).str.contains("^0+$") |
                pl.col("stop_time").cast(pl.Utf8, strict=False).str.lengths() < 10 |
                pl.col("stop_time").is_null()
            )
            .then(None)
            .otherwise(pl.col("stop_time"))
            .alias("stop_time")
        ])

        # Parse cleaned date strings to datetime
        df = df.with_columns([
            pl.coalesce([
                pl.col("start_time").cast(pl.Utf8, strict=False)
                                     .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False, exact=False),
                pl.col("start_time").cast(pl.Utf8, strict=False)
                                     .str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False, exact=False),
                pl.col("start_time").cast(pl.Utf8, strict=False)
                                     .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=False, exact=False),
            ]).alias("start_time"),
            pl.coalesce([
                pl.col("stop_time").cast(pl.Utf8, strict=False)
                                   .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False, exact=False),
                pl.col("stop_time").cast(pl.Utf8, strict=False)
                                   .str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False, exact=False),
                pl.col("stop_time").cast(pl.Utf8, strict=False)
                                   .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=False, exact=False),
            ]).alias("stop_time"),
        ])
    return df


def ensure_trip_duration(df):
    if "trip_duration" not in df.columns and {"start_time", "stop_time"}.issubset(df.columns):
        df = df.with_columns(
            ((pl.col("stop_time") - pl.col("start_time")).dt.nanoseconds() / 1_000_000_000).alias("trip_duration")
        )
    return df


def cast_birth_year(df):
    if "birth_year" in df.columns:
        df = df.with_columns(
            pl.col("birth_year").cast(pl.Float64, strict=False).cast(pl.Int64, strict=False)
        )
    return df


def trim_and_null(df):
    txt = [c for c, t in df.schema.items() if t == pl.Utf8]
    return df.with_columns([
        pl.when(pl.col(c).cast(pl.Utf8, strict=False).str.strip() == "")
          .then(None)
          .otherwise(pl.col(c).cast(pl.Utf8, strict=False).str.strip())
          .alias(c)
        for c in txt
    ])


def drop_duplicates(df):
    """Deduplicate with fallbacks for older data"""
    if "ride_id" in df.columns and df["ride_id"].null_count() < df.height * 0.5:
        return df.unique(subset=["ride_id"], keep="first")

    available_cols = [c for c in [
        "start_time", "stop_time", "bike_id", "start_station_id"
    ] if c in df.columns]

    if not available_cols:
        return df

    key_parts = []
    if "start_time" in available_cols:
        key_parts.append(
            pl.when(pl.col("start_time").is_null()).then(pl.lit("NULL_START")).otherwise(pl.col("start_time").cast(pl.Utf8))
        )
    if "stop_time" in available_cols:
        key_parts.append(
            pl.when(pl.col("stop_time").is_null()).then(pl.lit("NULL_STOP")).otherwise(pl.col("stop_time").cast(pl.Utf8))
        )
    if "bike_id" in available_cols:
        key_parts.append(
            pl.when(pl.col("bike_id").is_null()).then(pl.lit("NULL_BIKE")).otherwise(pl.col("bike_id").cast(pl.Utf8))
        )
    if "start_station_id" in available_cols:
        key_parts.append(
            pl.when(pl.col("start_station_id").is_null()).then(pl.lit("NULL_STATION")).otherwise(pl.col("start_station_id").cast(pl.Utf8))
        )

    df = df.with_columns(pl.concat_str(key_parts, separator="|").alias("_dedup_key"))
    return df.unique(subset=["_dedup_key"], keep="first").drop("_dedup_key")


def quality(df):
    return (
        df.filter((pl.col("trip_duration").is_null()) | (pl.col("trip_duration") > 0))
          .filter((pl.col("start_time").is_null()) |
                  (pl.col("stop_time").is_null()) |
                  (pl.col("stop_time") >= pl.col("start_time")))
    )


KEEP = ["trip_duration", "start_time", "start_station_name", "user_type"]
ID_COLS = ["start_station_id", "end_station_id", "bike_id", "ride_id"]


def process_year(year, files):
    out = SILVER / f"{year}-citibike.csv"
    if out.exists():
        print(f"Already processed {year}")
        return None

    combined_df = None
    total_raw = total_after_parse = total_after_dedup = total_after_quality = 0
    nulls_final = {}

    print(f"Processing {year} ({len(files)} files)...")

    for f in files:
        try:
            df = pl.read_csv(f, null_values=NULLS, infer_schema_length=5_000, ignore_errors=True)
            if df.height == 0:
                continue
            total_raw += df.height

            df = normalize(df)

            for c in ID_COLS:
                if c in df.columns:
                    df = df.with_columns(pl.col(c).cast(pl.Int64, strict=False))

            df = (
                trim_and_null(df)
                .pipe(parse_dates)
                .pipe(ensure_trip_duration)
                .pipe(cast_birth_year)
            )
            total_after_parse += df.height

            df = drop_duplicates(df)
            total_after_dedup += df.height

            df = quality(df)
            total_after_quality += df.height

            df = df.select([c for c in KEEP if c in df.columns])

            if combined_df is None:
                combined_df = df
            else:
                combined_df = pl.concat([combined_df, df], how="vertical_relaxed")

            del df
            gc.collect()

        except Exception as e:
            print(f"Error processing {f.name}: {e}")
            continue

    if combined_df is None or combined_df.height == 0:
        print(f"No valid data for {year}")
        return None

    for col in KEEP:
        if col in combined_df.columns:
            nulls_final[col] = combined_df[col].null_count()

    combined_df.write_csv(out)
    print(
        f"Completed {year}: {combined_df.height}/{total_raw} rows "
        f"({combined_df.height/total_raw*100:.1f}% retained)"
    )

    # --- NEW QUALITY METRICS -------------------------------------------------
    final_count = combined_df.height
    def pct(x): 
        return round((x / final_count) * 100, 4) if final_count else 0.0

    null_station_final = nulls_final.get("start_station_name", 0)
    null_user_type_final = nulls_final.get("user_type", 0)

    pct_trip_duration_final      = pct(nulls_final.get("trip_duration", 0))
    pct_start_time_final         = pct(nulls_final.get("start_time", 0))
    pct_start_station_name_final = pct(null_station_final)
    pct_user_type_final          = pct(null_user_type_final)
    # ------------------------------------------------------------------------

    metrics = {
        "year": int(year),
        "raw_records": total_raw,
        "after_parsing": total_after_parse,
        "after_dedup": total_after_dedup,
        "after_quality": total_after_quality,
        "final_records": combined_df.height,
        "duplicates_removed": total_after_parse - total_after_dedup,
        "quality_filtered": total_after_dedup - total_after_quality,
        "null_duration_final": nulls_final.get("trip_duration", 0),
        "null_start_time_final": nulls_final.get("start_time", 0),

        # --- NEW METRICS ADDED TO DICT -------
        "null_station_final": null_station_final,
        "null_user_type_final": null_user_type_final,
        "pct_trip_duration_final": pct_trip_duration_final,
        "pct_start_time_final": pct_start_time_final,
        "pct_start_station_name_final": pct_start_station_name_final,
        "pct_user_type_final": pct_user_type_final,
    }

    del combined_df
    gc.collect()

    return metrics


def save_quality_metrics(metrics_list):
    if not metrics_list:
        print("No metrics to save")
        return

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_quality_metrics (
                year INTEGER PRIMARY KEY,
                raw_records INTEGER,
                after_parsing INTEGER,
                after_dedup INTEGER,
                after_quality INTEGER,
                final_records INTEGER,
                duplicates_removed INTEGER,
                quality_filtered INTEGER,
                null_duration_final INTEGER,
                null_start_time_final INTEGER,
                null_station_final INTEGER,
                null_user_type_final INTEGER,
                pct_trip_duration_final REAL,
                pct_start_time_final REAL,
                pct_start_station_name_final REAL,
                pct_user_type_final REAL,
                process_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        for m in metrics_list:
            conn.execute(
                """
                INSERT OR REPLACE INTO silver_quality_metrics 
                (year, raw_records, after_parsing, after_dedup, after_quality, 
                 final_records, duplicates_removed, quality_filtered,
                 null_duration_final, null_start_time_final,
                 null_station_final, null_user_type_final,
                 pct_trip_duration_final, pct_start_time_final,
                 pct_start_station_name_final, pct_user_type_final)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    m["year"], m["raw_records"], m["after_parsing"], m["after_dedup"],
                    m["after_quality"], m["final_records"], m["duplicates_removed"],
                    m["quality_filtered"], m["null_duration_final"], m["null_start_time_final"],
                    m["null_station_final"], m["null_user_type_final"],
                    m["pct_trip_duration_final"], m["pct_start_time_final"],
                    m["pct_start_station_name_final"], m["pct_user_type_final"],
                ),
            )

        conn.commit()
        print("Quality metrics saved to database")

    except Exception as e:
        print(f"Error saving metrics: {e}")
        raise
    finally:
        conn.close()


def main():
    years = {str(y) for y in range(2013, 2024)}
    files_by_year = {}

    for csv in BRONZE.glob("*.csv"):
        y = csv.name[:4]
        if y in years:
            files_by_year.setdefault(y, []).append(csv)

    all_metrics = []

    for y, files in sorted(files_by_year.items()):
        metrics = process_year(y, files)
        if metrics:
            all_metrics.append(metrics)

        gc.collect()

    if all_metrics:
        save_quality_metrics(all_metrics)

    return all_metrics


@transformer
def transform_data(data, *args, **kwargs):
    try:
        metrics = main()
        notify_success("Silver Processing", {
            "years_processed": len(metrics) if metrics else 0,
            "destination": "data/silver",
            "quality_metrics": "saved to database"
        })
        return {
            "status": "completed",
            "metrics_saved": len(metrics) if metrics else 0,
        }
    except Exception as e:
        notify_failure("Silver Processing", str(e), {
            "step": "clean_and_transform",
            "source": "data/bronze"
        })
        raise
