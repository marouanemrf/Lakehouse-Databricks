"""Tests des transformations sur de petits DataFrames (necessite Databricks Connect)."""
from datetime import datetime
from decimal import Decimal

import pytest

from lakehouse_transforms import (
    DataQualityError,
    add_reject_reason,
    add_trip_id,
    assert_no_critical_failures,
    clean_zones,
    evaluate_rules,
    handle_nulls,
)

pytestmark = pytest.mark.spark

T0, T1 = datetime(2024, 1, 15, 10, 0), datetime(2024, 1, 15, 10, 20)
TRIP_SCHEMA = ("pickup_datetime timestamp, dropoff_datetime timestamp, trip_distance double, "
               "fare_amount decimal(10,2), total_amount decimal(10,2), pu_location_id int, do_location_id int")


def reasons(spark, row):
    r = add_reject_reason(spark.createDataFrame([row], TRIP_SCHEMA)).collect()[0].reject_reason
    return set(r.split("; ")) if r else set()


def test_valid_trip_has_no_reject_reason(spark):
    assert reasons(spark, (T0, T1, 2.5, Decimal("12.00"), Decimal("15.00"), 10, 20)) == set()


@pytest.mark.parametrize("row, expected", [
    ((T0, T1, 0.0, Decimal("10.00"), Decimal("12.00"), 10, 20), {"non_positive_distance"}),
    ((T0, T1, 150.0, Decimal("10.00"), Decimal("12.00"), 10, 20), {"distance_too_large"}),
    ((T0, T1, 2.0, Decimal("-5.00"), Decimal("-5.00"), 10, 20), {"negative_amount"}),
    ((T1, T0, 2.0, Decimal("10.00"), Decimal("12.00"), 10, 20), {"non_positive_duration"}),
    ((T0, T1, 2.0, Decimal("10.00"), Decimal("12.00"), 300, 20), {"invalid_location"}),
    ((T0, T1, 0.0, Decimal("-5.00"), Decimal("-5.00"), 10, 20), {"non_positive_distance", "negative_amount"}),
])
def test_reject_reasons(spark, row, expected):
    assert reasons(spark, row) == expected


def test_handle_nulls_imputes_and_flags(spark):
    df = spark.createDataFrame([(None, None, None, None, None)],
                               "passenger_count int, ratecode_id int, store_and_fwd_flag string, "
                               "congestion_surcharge decimal(10,2), airport_fee decimal(10,2)")
    r = handle_nulls(df).collect()[0]
    assert (r.passenger_count, r.passenger_count_imputed, r.ratecode_id, r.store_and_fwd_flag) == (1, True, 99, "N")
    assert r.congestion_surcharge == 0 and r.airport_fee == 0


def test_trip_id_is_deterministic(spark):
    schema = ("vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, pu_location_id int, "
              "do_location_id int, trip_distance double, total_amount decimal(10,2), passenger_count int")
    row = (1, T0, T1, 10, 20, 2.0, Decimal("12.00"), 1)
    other = (1, T0, T1, 10, 20, 2.0, Decimal("13.00"), 1)
    ids = [r.trip_id for r in add_trip_id(spark.createDataFrame([row, row, other], schema)).collect()]
    assert ids[0] == ids[1] != ids[2]
    assert len(ids[0]) == 64


def test_clean_zones_normalizes_unknown(spark):
    df = spark.createDataFrame([(264, "Unknown", "N/A", "N/A"), (1, "EWR", "Newark Airport", "EWR")],
                               "LocationID int, Borough string, Zone string, service_zone string")
    rows = {r.location_id: r for r in clean_zones(df).collect()}
    assert (rows[264].borough, rows[264].zone, rows[264].service_zone) == ("Unknown", "Unknown", "Unknown")
    assert rows[1].zone == "Newark Airport"


def test_evaluate_rules_counts_failures(spark):
    df = spark.createDataFrame([(1, 5.0), (1, -1.0), (2, 3.0)], "id int, amount double")
    res = evaluate_rules(df, [
        {"table": "t", "rule": "id_unique", "type": "unique", "column": "id", "severity": "critical"},
        {"table": "t", "rule": "amount_pos", "condition": "amount > 0", "severity": "critical"},
        {"table": "t", "rule": "amount_lt_10", "condition": "amount < 10", "severity": "warning"},
    ])
    assert [(r["failed_rows"], r["status"]) for r in res] == [(1, "FAIL"), (1, "FAIL"), (0, "PASS")]


def test_assert_no_critical_failures_raises(spark):
    df = spark.createDataFrame([("critical", "FAIL", "t", "r", 3)],
                               "severity string, status string, table_name string, rule_name string, failed_rows long")
    with pytest.raises(DataQualityError):
        assert_no_critical_failures(df)
