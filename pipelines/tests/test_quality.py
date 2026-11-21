"""The data-quality checks against real numbers (measured in Redshift on 2024-01..04) and against
corrupt months. The point of each case: a real month must never be blocked, a corrupt one must be."""

import pytest

from labpipes.defs.quality import (
    BLOCKING,
    NAMES,
    RATE_CHECKS,
    judge_rate,
    judge_rows_complete,
    judge_rows_sane,
)

# Per month, as measured on the loaded data: rows, then bad rows per rate check (names are the checks').
REAL = {
    "2024-01": dict(rows=2964624, critical_nulls=0, dates_in_month=18, bad_duration=870, very_long_trips=1983,
                    unknown_zones=31527, null_passengers=140162, negative_fare=37448, zero_distance=60371, no_double_load=0),
    "2024-02": dict(rows=3007526, critical_nulls=0, dates_in_month=15, bad_duration=803, very_long_trips=1948,
                    unknown_zones=28974, null_passengers=185610, negative_fare=40655, zero_distance=68225, no_double_load=1),
    "2024-03": dict(rows=3582628, critical_nulls=0, dates_in_month=23, bad_duration=1128, very_long_trips=2172,
                    unknown_zones=35374, null_passengers=426190, negative_fare=58464, zero_distance=87168, no_double_load=0),
    "2024-04": dict(rows=3514289, critical_nulls=0, dates_in_month=9, bad_duration=1089, very_long_trips=2263,
                    unknown_zones=35759, null_passengers=408576, negative_fare=57568, zero_distance=45813, no_double_load=0),
}


def failed(month: dict, file_rows: int | None = None) -> set[str]:
    """The names of the checks that fail for a month's counts."""
    rows = month["rows"]
    out = {"rows_complete": judge_rows_complete(rows, rows if file_rows is None else file_rows), "rows_sane": judge_rows_sane(rows)}
    out |= {name: judge_rate(name, month[name], rows) for name in RATE_CHECKS}
    return {name for name, o in out.items() if not o.passed}


@pytest.mark.parametrize("month", REAL)
def test_a_real_month_fails_nothing_not_even_a_warning(month):
    assert failed(REAL[month]) == set()


def test_the_blocking_checks_are_the_decided_eight_and_every_check_is_named():
    assert BLOCKING == {"rows_complete", "rows_sane", "no_double_load", "critical_nulls", "dates_in_month",
                        "bad_duration", "very_long_trips", "unknown_zones"}
    assert len(NAMES) == 11 and set(NAMES) >= BLOCKING


def corrupt(**changes) -> dict:
    return {**REAL["2024-03"], **changes}


def test_a_month_loaded_twice_is_caught():
    n = REAL["2024-03"]["rows"]
    assert {"rows_complete", "no_double_load"} <= failed(corrupt(rows=2 * n, no_double_load=n), file_rows=n)


def test_a_partial_copy_is_caught_even_when_the_count_looks_plausible():
    assert failed(corrupt(rows=3_000_000), file_rows=3_582_628) >= {"rows_complete"}
    assert "rows_sane" not in failed(corrupt(rows=3_000_000), file_rows=3_582_628)


def test_half_a_file_is_caught_by_the_row_range():
    f = failed(corrupt(rows=1_200_000))  # the count matches the file, so only the range can notice
    assert "rows_sane" in f and "rows_complete" not in f


def test_an_empty_month_fails_the_rate_checks_instead_of_dividing_by_zero():
    f = failed({**{k: 0 for k in REAL["2024-03"]}}, file_rows=0)
    assert "rows_sane" in f and "bad_duration" in f and "unknown_zones" in f


def test_shifted_timestamps_are_caught():
    assert "dates_in_month" in failed(corrupt(dates_in_month=REAL["2024-03"]["rows"]))


def test_negative_durations_are_caught():
    assert "bad_duration" in failed(corrupt(bad_duration=REAL["2024-03"]["rows"] // 3))


def test_a_shifted_zone_column_is_caught():
    assert "unknown_zones" in failed(corrupt(unknown_zones=REAL["2024-03"]["rows"] // 2))


def test_a_single_null_in_a_column_the_mart_needs_is_caught():
    assert "critical_nulls" in failed(corrupt(critical_nulls=1))


def test_passenger_fare_and_distance_problems_fail_but_do_not_block():
    n = REAL["2024-03"]["rows"]
    f = failed(corrupt(null_passengers=n // 2, negative_fare=n // 5, zero_distance=n // 5))
    assert f == {"null_passengers", "negative_fare", "zero_distance"}
    assert not (f & BLOCKING)


def test_a_month_that_is_not_a_month_never_reaches_redshift():
    from types import SimpleNamespace

    from fakes import LOG, fake_redshift

    from labpipes.defs.quality import _rate

    rs = fake_redshift()
    _rate(SimpleNamespace(partition_key="2024-03"), rs, "bad_duration")
    assert LOG and all("2024-03" in sql for sql in LOG)

    LOG.clear()
    with pytest.raises(ValueError):
        _rate(SimpleNamespace(partition_key="2024-03'; drop table x; --"), rs, "bad_duration")
    assert LOG == []  # rejected before any statement was built


def test_the_code_location_has_all_eleven_checks_with_the_decided_blocking_flags(monkeypatch):
    for k in ("REDSHIFT_CLUSTER_ID", "REDSHIFT_DATABASE", "REDSHIFT_DB_USER", "LAB_DATA_BUCKET", "LAB_ARTIFACT_BUCKET", "MLFLOW_TRACKING_URI", "LLM_HOST"):
        monkeypatch.setenv(k, "x")
    from labpipes.definitions import defs

    graph = defs.resolve_asset_graph()
    on_stg = {k.name: graph.get_check_spec(k) for k in graph.asset_check_keys if k.asset_key.to_user_string() == "stg_trips"}
    assert set(on_stg) == set(NAMES)
    assert {name for name, spec in on_stg.items() if spec.blocking} == BLOCKING
