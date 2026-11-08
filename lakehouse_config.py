"""Config mchtarka bin ga3 les phases dyal Lakehouse-Databricks.

Kol notebook kaybda b:
    from lakehouse_config import *
    spark = get_spark()
"""
import glob
import os
import urllib.request

# ---------------------------------------------------------------- Connexion
PROFILE = "marouane mrf"
CATALOG = "workspace"

# ---------------------------------------------------------------- Schemas (Medallion)
SCHEMA_RAW = f"{CATALOG}.raw"        # Volume dyal fichiers bruts
SCHEMA_BRONZE = f"{CATALOG}.bronze"
SCHEMA_SILVER = f"{CATALOG}.silver"
SCHEMA_GOLD = f"{CATALOG}.gold"
SCHEMA_OPS = f"{CATALOG}.ops"        # logs, data quality, historique

# ---------------------------------------------------------------- Fichiers bruts
RAW_VOLUME = f"/Volumes/{CATALOG}/raw/landing"
TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONES_FILE = "taxi_zone_lookup.csv"

# ---------------------------------------------------------------- Tables
T_DATA_DICTIONARY = f"{SCHEMA_OPS}.data_dictionary"

T_BRONZE_TRIPS = f"{SCHEMA_BRONZE}.yellow_trips"
T_BRONZE_ZONES = f"{SCHEMA_BRONZE}.taxi_zones"

T_SILVER_TRIPS = f"{SCHEMA_SILVER}.yellow_trips"
T_SILVER_ZONES = f"{SCHEMA_SILVER}.taxi_zones"
T_SILVER_REJECTED = f"{SCHEMA_SILVER}.yellow_trips_rejected"

T_GOLD_DIM_ZONE = f"{SCHEMA_GOLD}.dim_zone"
T_GOLD_DIM_DATE = f"{SCHEMA_GOLD}.dim_date"
T_GOLD_FACT_TRIPS = f"{SCHEMA_GOLD}.fact_trips"
T_GOLD_DAILY_KPIS = f"{SCHEMA_GOLD}.kpi_daily"
T_GOLD_ZONE_KPIS = f"{SCHEMA_GOLD}.kpi_pickup_zone"

T_DQ_RESULTS = f"{SCHEMA_OPS}.dq_results"
T_INGESTION_LOG = f"{SCHEMA_OPS}.ingestion_log"


def in_databricks() -> bool:
    """True ila l code kaykhdem f Databricks (notebook f site wla Job)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _add_cli_to_path():
    # Databricks CLI (dyal extension VS Code) — lazem l authentification mn PC
    cli = glob.glob(os.path.expanduser(r"~\.vscode\extensions\databricks.databricks-*\bin"))
    if cli and cli[-1] not in os.environ["PATH"]:
        os.environ["PATH"] = cli[-1] + os.pathsep + os.environ["PATH"]


def get_spark():
    """SparkSession mconnecti m3a Databricks (Serverless)."""
    if in_databricks():
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    from databricks.connect import DatabricksSession
    if os.environ.get("DATABRICKS_HOST"):  # CI (GitHub Actions): auth b variables d'environnement
        return DatabricksSession.builder.serverless().getOrCreate()
    _add_cli_to_path()
    return DatabricksSession.builder.profile(PROFILE).serverless().getOrCreate()


def get_workspace_client():
    """Client dyal Databricks SDK (Files, Jobs, ...)."""
    from databricks.sdk import WorkspaceClient
    if in_databricks() or os.environ.get("DATABRICKS_HOST"):
        return WorkspaceClient()
    _add_cli_to_path()
    return WorkspaceClient(profile=PROFILE)


def setup_schemas(spark):
    """Kaysayeb les schemas w l Volume ila ma kanouch."""
    for s in [SCHEMA_RAW, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD, SCHEMA_OPS]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_RAW}.landing")


def upload_to_volume(source: str, filename: str, overwrite: bool = False) -> str:
    """Kat-uploadi fichier (URL wla chemin local) l Volume `landing`.

    Kat-rjje3 chemin dyal fichier f Volume.
    """
    target = f"{RAW_VOLUME}/{filename}"

    if in_databricks():  # f Databricks, Volume mounté k dossier local (/Volumes/...)
        import shutil
        if os.path.exists(target) and not overwrite:
            print(f"Deja kayn: {target}")
        elif source.startswith("http"):
            print(f"Telechargement: {source}")
            urllib.request.urlretrieve(source, target)
        else:
            shutil.copy(source, target)
        return target

    w = get_workspace_client()

    if not overwrite:
        try:
            w.files.get_metadata(target)
            print(f"Deja kayn: {target}")
            return target
        except Exception:
            pass

    if source.startswith("http"):
        local = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", filename)
        os.makedirs(os.path.dirname(local), exist_ok=True)
        if not os.path.exists(local):
            print(f"Telechargement: {source}")
            urllib.request.urlretrieve(source, local)
        source = local

    with open(source, "rb") as f:
        w.files.upload(target, f, overwrite=True)
    print(f"Upload: {target}")
    return target
