"""The feature_contract check: the contract's own table passes, leakage and drift are caught."""

import pytest

from labpipes.contracts import features_contract
from labpipes.defs.feature_check import column_problems, expected_columns, value_conditions, value_problems

# The table exactly as sql/trip_features.sql creates it, as Redshift's information_schema reports it.
GOOD = [
    ("trip_month", "character"),
    ("pickup_zone_id", "integer"),
    ("dropoff_zone_id", "integer"),
    ("pickup_hour", "smallint"),
    ("pickup_dow", "smallint"),
    ("duration_min", "double precision"),
]


@pytest.fixture
def contract():
    return features_contract()


def test_the_table_the_sql_creates_matches_the_contract(contract):
    assert column_problems(GOOD, contract) == []
    assert set(expected_columns(contract)) == {n for n, _ in GOOD}


@pytest.mark.parametrize("leak", ["trip_distance", "fare_amount", "total_amount", "dropoff_at", "tip_amount", "payment_type", "airport_fee"])
def test_every_forbidden_column_is_caught_as_leakage(contract, leak):
    problems = column_problems([*GOOD, (leak, "double precision")], contract)
    assert len(problems) == 1 and leak in problems[0] and "leakage" in problems[0]


def test_an_unknown_column_is_caught_but_is_not_called_leakage(contract):
    (p,) = column_problems([*GOOD, ("weather_code", "integer")], contract)
    assert "weather_code" in p and "not in the contract" in p


def test_a_missing_column_and_a_changed_type_are_caught(contract):
    assert any("pickup_hour is missing" in p for p in column_problems([c for c in GOOD if c[0] != "pickup_hour"], contract))
    drifted = [(n, "bigint" if n == "pickup_zone_id" else t) for n, t in GOOD]
    assert any("pickup_zone_id is bigint" in p for p in column_problems(drifted, contract))


def test_column_names_compare_case_insensitively(contract):
    assert column_problems([(n.upper(), t) for n, t in GOOD], contract) == []


def test_value_conditions_follow_the_contract_ranges(contract):
    cond = value_conditions(contract)
    assert set(cond) == {"pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow", "duration_min"}
    assert "pickup_zone_id < 1 or pickup_zone_id > 263" in cond["pickup_zone_id"]
    assert "pickup_hour < 0 or pickup_hour > 23" in cond["pickup_hour"]
    assert "pickup_dow < 0 or pickup_dow > 6" in cond["pickup_dow"]
    assert "duration_min <= 0" in cond["duration_min"] and "duration_min > 180" in cond["duration_min"]  # (0, 180]


def test_out_of_range_values_and_an_empty_month_are_caught():
    assert value_problems({"pickup_hour": 0, "duration_min": 0}, 3_000_000) == []
    assert value_problems({"pickup_hour": 5, "duration_min": 0}, 3_000_000) == ["5 rows have pickup_hour null or out of range"]
    assert value_problems({"pickup_hour": 0}, 0) == ["the month has no rows"]
