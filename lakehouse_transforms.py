"""Transformations et Data Quality du Lakehouse (Bronze -> Silver, regles DQ).

Fonctions "pures" sur des DataFrames: utilisees par les notebooks (Phases 3, 4, 6, 7)
et testees par pytest (Phase 10).
"""
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ============================================================== Bronze

def add_ingestion_metadata(df: DataFrame, batch_id: str | None = None) -> DataFrame:
    """Ajoute les colonnes techniques de tracabilite (Bronze)."""
    batch_id = batch_id or str(uuid.uuid4())
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


# ============================================================== Bronze -> Silver

# colonne Bronze (TLC)  ->  (colonne Silver, type)
COLUMN_MAPPING = {
    "VendorID": ("vendor_id", "int"),
    "tpep_pickup_datetime": ("pickup_datetime", "timestamp"),
    "tpep_dropoff_datetime": ("dropoff_datetime", "timestamp"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "RatecodeID": ("ratecode_id", "int"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "string"),
    "PULocationID": ("pu_location_id", "int"),
    "DOLocationID": ("do_location_id", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "decimal(10,2)"),
    "extra": ("extra", "decimal(10,2)"),
    "mta_tax": ("mta_tax", "decimal(10,2)"),
    "tip_amount": ("tip_amount", "decimal(10,2)"),
    "tolls_amount": ("tolls_amount", "decimal(10,2)"),
    "improvement_surcharge": ("improvement_surcharge", "decimal(10,2)"),
    "total_amount": ("total_amount", "decimal(10,2)"),
    "congestion_surcharge": ("congestion_surcharge", "decimal(10,2)"),
    "Airport_fee": ("airport_fee", "decimal(10,2)"),
}
METADATA_COLUMNS = ["_source_file", "_ingestion_timestamp", "_batch_id"]

MIN_LOCATION_ID, MAX_LOCATION_ID = 1, 265
MAX_DISTANCE_MILES = 100
MAX_DURATION_HOURS = 24

REJECT_REASONS = [
    "missing_timestamp", "non_positive_duration", "duration_over_24h", "non_positive_distance",
    "distance_too_large", "negative_amount", "invalid_location", "pickup_outside_file_period",
]


def standardize_trips(df: DataFrame) -> DataFrame:
    """Type casting + renommage snake_case."""
    cols = [F.col(src).cast(typ).alias(dst) for src, (dst, typ) in COLUMN_MAPPING.items()]
    cols += [F.col(c) for c in METADATA_COLUMNS if c in df.columns]
    return df.select(*cols)


def handle_nulls(df: DataFrame) -> DataFrame:
    """Strategie Nulls (lignes Flex Fare): imputation + flag pour garder la trace."""
    zero = F.lit(0).cast("decimal(10,2)")
    return (
        df.withColumn("passenger_count_imputed", F.col("passenger_count").isNull())
        .withColumn("passenger_count", F.coalesce("passenger_count", F.lit(1)))
        .withColumn("ratecode_id", F.coalesce("ratecode_id", F.lit(99)))  # 99 = inconnu (TLC)
        .withColumn("store_and_fwd_flag", F.coalesce("store_and_fwd_flag", F.lit("N")))
        .withColumn("congestion_surcharge", F.coalesce("congestion_surcharge", zero))
        .withColumn("airport_fee", F.coalesce("airport_fee", zero))
    )


def add_reject_reason(df: DataFrame) -> DataFrame:
    """Ajoute `reject_reason` (chaine vide = ligne valide, sinon motifs separes par '; ')."""
    duration_s = F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")
    if "_source_file" in df.columns:
        source_month = F.regexp_extract(F.coalesce("_source_file", F.lit("")), r"(\d{4}-\d{2})\.parquet$", 1)
    else:
        source_month = F.lit("")

    checks = [
        (F.col("pickup_datetime").isNull() | F.col("dropoff_datetime").isNull(), "missing_timestamp"),
        (duration_s <= 0, "non_positive_duration"),
        (duration_s > MAX_DURATION_HOURS * 3600, "duration_over_24h"),
        (F.col("trip_distance") <= 0, "non_positive_distance"),
        (F.col("trip_distance") > MAX_DISTANCE_MILES, "distance_too_large"),
        ((F.col("fare_amount") < 0) | (F.col("total_amount") < 0), "negative_amount"),
        (~F.col("pu_location_id").between(MIN_LOCATION_ID, MAX_LOCATION_ID)
         | ~F.col("do_location_id").between(MIN_LOCATION_ID, MAX_LOCATION_ID), "invalid_location"),
        ((source_month != "") & (F.date_format("pickup_datetime", "yyyy-MM") != source_month),
         "pickup_outside_file_period"),
    ]
    reasons = [F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for cond, name in checks]
    return df.withColumn("reject_reason", F.concat_ws("; ", *reasons))


TRIP_KEY_COLUMNS = ["vendor_id", "pickup_datetime", "dropoff_datetime", "pu_location_id",
                    "do_location_id", "trip_distance", "total_amount", "passenger_count"]


def add_trip_id(df: DataFrame) -> DataFrame:
    """Cle technique deterministe (hash SHA-256) -> deduplication et MERGE."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in TRIP_KEY_COLUMNS]
    return df.withColumn("trip_id", F.sha2(F.concat_ws("||", *parts), 256))


def enrich_trips(df: DataFrame) -> DataFrame:
    """Colonnes derivees utiles pour Gold."""
    return (
        df.withColumn(
            "trip_duration_min",
            F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60, 2),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
    )


def bronze_to_silver(bronze_df: DataFrame):
    """Pipeline complet Bronze -> Silver. Retourne (valid_df, rejected_df).

    valid_df est dedoublonne sur trip_id.
    """
    df = enrich_trips(add_trip_id(add_reject_reason(handle_nulls(standardize_trips(bronze_df)))))
    valid = df.filter(F.col("reject_reason") == "").drop("reject_reason").dropDuplicates(["trip_id"])
    rejected = df.filter(F.col("reject_reason") != "").withColumn("_rejected_at", F.current_timestamp())
    return valid, rejected


def clean_zones(df: DataFrame) -> DataFrame:
    """Silver des zones: snake_case + normalisation des valeurs inconnues."""
    def norm(c):
        return F.when(F.trim(c).isin("N/A", "Unknown") | F.col(c).isNull(), F.lit("Unknown")).otherwise(F.trim(c))

    return df.select(
        F.col("LocationID").cast("int").alias("location_id"),
        norm("Borough").alias("borough"),
        norm("Zone").alias("zone"),
        norm("service_zone").alias("service_zone"),
    ).dropDuplicates(["location_id"])


# ============================================================== Data Quality

class DataQualityError(Exception):
    """Levee quand une regle `critical` echoue."""


def build_dq_rules(silver_trips: str, silver_zones: str, gold_daily: str) -> list:
    """Regles DQ. `condition` = ce qui doit etre VRAI pour chaque ligne."""
    return [
        # --- Silver trips
        {"table": silver_trips, "rule": "trip_id_unique", "type": "unique", "column": "trip_id", "severity": "critical"},
        {"table": silver_trips, "rule": "trip_id_not_null", "condition": "trip_id IS NOT NULL", "severity": "critical"},
        {"table": silver_trips, "rule": "positive_duration", "condition": "trip_duration_min > 0", "severity": "critical"},
        {"table": silver_trips, "rule": "positive_distance", "condition": "trip_distance > 0", "severity": "critical"},
        {"table": silver_trips, "rule": "non_negative_total", "condition": "total_amount >= 0", "severity": "critical"},
        {"table": silver_trips, "rule": "valid_location", "condition": "pu_location_id BETWEEN 1 AND 265 AND do_location_id BETWEEN 1 AND 265", "severity": "critical"},
        {"table": silver_trips, "rule": "valid_payment_type", "condition": "payment_type BETWEEN 0 AND 6", "severity": "warning"},
        {"table": silver_trips, "rule": "reasonable_passengers", "condition": "passenger_count BETWEEN 1 AND 6", "severity": "warning"},
        {"table": silver_trips, "rule": "tip_le_3x_fare", "condition": "tip_amount <= GREATEST(fare_amount, 1) * 3", "severity": "warning"},
        # --- Silver zones
        {"table": silver_zones, "rule": "location_id_unique", "type": "unique", "column": "location_id", "severity": "critical"},
        {"table": silver_zones, "rule": "zone_not_null", "condition": "zone IS NOT NULL", "severity": "critical"},
        # --- Gold
        {"table": gold_daily, "rule": "daily_trips_positive", "condition": "total_trips > 0", "severity": "critical"},
        {"table": gold_daily, "rule": "daily_revenue_positive", "condition": "total_revenue > 0", "severity": "warning"},
    ]


def evaluate_rules(df: DataFrame, rules: list) -> list:
    """Calcule failed_rows pour chaque regle sur un DataFrame (une seule agregation)."""
    aggs = [F.count(F.lit(1)).alias("__total")]
    for i, r in enumerate(rules):
        if r.get("type") == "unique":
            aggs.append((F.count(F.lit(1)) - F.countDistinct(r["column"])).alias(f"r{i}"))
        else:
            ok = F.coalesce(F.expr(r["condition"]), F.lit(False))
            aggs.append(F.sum(F.when(~ok, 1).otherwise(0)).alias(f"r{i}"))
    row = df.agg(*aggs).collect()[0]
    total = int(row["__total"])
    out = []
    for i, r in enumerate(rules):
        failed = int(row[f"r{i}"] or 0)
        out.append({
            "table_name": r["table"],
            "rule_name": r["rule"],
            "severity": r["severity"],
            "total_rows": total,
            "failed_rows": failed,
            "pass_rate": round(100.0 * (total - failed) / total, 4) if total else 0.0,
            "status": "PASS" if failed == 0 else "FAIL",
        })
    return out


DQ_SCHEMA = ("run_id string, run_ts timestamp, table_name string, rule_name string, severity string, "
             "total_rows long, failed_rows long, pass_rate double, status string")
_DQ_COLS = ["run_id", "run_ts", "table_name", "rule_name", "severity", "total_rows", "failed_rows", "pass_rate", "status"]


def run_dq_checks(spark, rules: list, run_id: str) -> DataFrame:
    """Execute toutes les regles. Une table illisible donne un statut ERROR (sans arreter le run)."""
    results = []
    for table in dict.fromkeys(r["table"] for r in rules):
        table_rules = [r for r in rules if r["table"] == table]
        try:
            results += evaluate_rules(spark.table(table), table_rules)
        except Exception as e:
            msg = str(e).splitlines()[0][:200]
            results += [{"table_name": table, "rule_name": r["rule"], "severity": r["severity"], "total_rows": 0,
                         "failed_rows": 0, "pass_rate": 0.0, "status": f"ERROR: {msg}"} for r in table_rules]
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    for r in results:
        r.update(run_id=run_id, run_ts=run_ts)
    return spark.createDataFrame([tuple(r[c] for c in _DQ_COLS) for r in results], DQ_SCHEMA)


def assert_no_critical_failures(results_df: DataFrame) -> None:
    """Leve DataQualityError si une regle critical n'est pas PASS."""
    bad = results_df.filter((F.col("severity") == "critical") & (F.col("status") != "PASS")).collect()
    if bad:
        details = ", ".join(f"{r.table_name}.{r.rule_name} ({r.status}, {r.failed_rows} lignes)" for r in bad)
        raise DataQualityError(f"{len(bad)} regle(s) critical en echec: {details}")
