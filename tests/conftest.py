import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _has_databricks_credentials() -> bool:
    return bool(os.environ.get("DATABRICKS_HOST")) or (Path.home() / ".databrickscfg").exists()


@pytest.fixture(scope="session")
def spark():
    """Session Spark distante (Databricks Connect, Serverless). Skip si aucune authentification."""
    if not _has_databricks_credentials():
        pytest.skip("Pas d'authentification Databricks (DATABRICKS_HOST ou ~/.databrickscfg)")
    from lakehouse_config import get_spark

    return get_spark()
