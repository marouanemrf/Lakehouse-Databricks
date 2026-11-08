"""Tests sans Spark: structure de la configuration et des regles DQ."""
import lakehouse_config as cfg
from lakehouse_transforms import COLUMN_MAPPING, REJECT_REASONS, build_dq_rules


def test_tables_are_fully_qualified():
    tables = [v for k, v in vars(cfg).items() if k.startswith("T_")]
    assert tables, "aucune table declaree"
    for t in tables:
        assert t.count(".") == 2 and t.startswith(f"{cfg.CATALOG}."), t


def test_medallion_schemas():
    assert cfg.T_BRONZE_TRIPS.split(".")[1] == "bronze"
    assert cfg.T_SILVER_TRIPS.split(".")[1] == "silver"
    assert cfg.T_GOLD_FACT_TRIPS.split(".")[1] == "gold"


def test_column_mapping_is_snake_case():
    for _, (dst, _typ) in COLUMN_MAPPING.items():
        assert dst == dst.lower() and " " not in dst, dst


def test_dq_rules_are_well_formed():
    rules = build_dq_rules("c.s.trips", "c.s.zones", "c.g.daily")
    assert len({r["rule"] for r in rules}) == len(rules), "noms de regles en double"
    for r in rules:
        assert r["severity"] in {"critical", "warning"}
        assert ("condition" in r) ^ (r.get("type") == "unique"), r


def test_reject_reasons_unique():
    assert len(REJECT_REASONS) == len(set(REJECT_REASONS))
