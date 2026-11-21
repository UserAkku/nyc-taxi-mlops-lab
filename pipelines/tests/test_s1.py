"""Tests for what S1 builds: schema_contract, raw_trips and the S3 sensor. Fakes only, no AWS."""

import dagster as dg
import pytest
from fakes import fake_bucket, schema_2023, schema_2025

from labpipes.contracts import Column, trips_schema
from labpipes.defs.raw_trips import raw_trips_statements
from labpipes.defs.schema_check import schema_problems
from labpipes.defs.sensor import trip_files_sensor
from labpipes.partitions import trip_months

# --- schema_contract ---


def test_the_contract_matches_itself():
    assert schema_problems(trips_schema(), trips_schema()) == []


def test_names_compare_case_insensitively():
    renamed = [Column("airport_fee" if c.name == "Airport_fee" else c.name, c.type) for c in trips_schema()]
    assert schema_problems(trips_schema(), renamed) == []


def test_an_appended_column_is_a_mismatch():
    problems = schema_problems(trips_schema(), schema_2025())
    assert any("20 columns" in p for p in problems) and any("cbd_congestion_fee" in p for p in problems)


def test_changed_types_are_mismatches():
    problems = schema_problems(trips_schema(), schema_2023())
    assert any("VendorID" in p and "INT64" in p for p in problems)
    assert any("passenger_count" in p and "DOUBLE" in p for p in problems)


def test_a_dropped_column_and_a_reordering_are_mismatches():
    assert any("missing" in p for p in schema_problems(trips_schema(), trips_schema()[:-1]))
    swapped = trips_schema()
    swapped[1], swapped[2] = swapped[2], swapped[1]
    assert schema_problems(trips_schema(), swapped)


# --- raw_trips ---


def test_reload_is_delete_then_insert_for_the_months_file_only():
    sqls = raw_trips_statements("2024-03", "s3://data/trips/yellow_tripdata_2024-03.parquet")
    text = "\n".join(sqls)
    assert "diststyle even sortkey (source_file)" in text
    assert "copy landing from 's3://data/trips/yellow_tripdata_2024-03.parquet' iam_role default format as parquet" in text
    delete = next(i for i, s in enumerate(sqls) if s.startswith("delete from ingest.raw_trips"))
    insert = next(i for i, s in enumerate(sqls) if s.startswith("insert into ingest.raw_trips"))
    assert delete < insert
    assert "where source_file = 'yellow_tripdata_2024-03.parquet'" in sqls[delete]
    assert sqls[insert].startswith("insert into ingest.raw_trips select 'yellow_tripdata_2024-03.parquet', vendorid,")
    assert sqls[insert].count(",") == len(trips_schema())  # source_file + every column of the file


def test_a_month_that_is_not_a_month_never_reaches_sql():
    with pytest.raises(ValueError):
        raw_trips_statements("2024-03'; drop table x; --", "s3://x")


# --- the sensor ---


def tick(instance, files, cursor=None) -> dg.SensorResult:
    """Call the sensor function itself, with a fake bucket and a real (test) instance."""
    ctx = dg.build_sensor_context(instance=instance, cursor=cursor, resources={"data_bucket": fake_bucket(files)})
    return trip_files_sensor(ctx)


def test_sensor_requests_new_and_changed_files_once():
    with dg.instance_for_test() as instance:
        files = {"2024-01": ("a", trips_schema()), "2024-02": ("b", trips_schema())}
        first = tick(instance, files)
        assert [r.partition_key for r in first.run_requests] == ["2024-01", "2024-02"]
        assert [r.run_key for r in first.run_requests] == ["2024-01:a", "2024-02:b"]

        instance.add_dynamic_partitions(trip_months.name, ["2024-01", "2024-02"])
        quiet = tick(instance, files, cursor=first.cursor)
        assert isinstance(quiet, dg.SkipReason)

        files["2024-02"] = ("b2", trips_schema())  # the file was replaced upstream
        again = tick(instance, files, cursor=first.cursor)
        assert [r.partition_key for r in again.run_requests] == ["2024-02"]
        assert again.run_requests[0].run_key == "2024-02:b2"
        assert not again.dynamic_partitions_requests  # the partition already exists


def test_sensor_adds_each_new_month_as_a_partition():
    with dg.instance_for_test() as instance:
        first = tick(instance, {"2024-01": ("a", trips_schema())})
        (add,) = first.dynamic_partitions_requests
        assert add.partitions_def_name == trip_months.name and list(add.partition_keys) == ["2024-01"]


def test_the_ingest_job_runs_from_trip_files_to_the_mart(monkeypatch):
    for k in ("REDSHIFT_CLUSTER_ID", "REDSHIFT_DATABASE", "REDSHIFT_DB_USER", "LAB_DATA_BUCKET", "LAB_ARTIFACT_BUCKET", "MLFLOW_TRACKING_URI", "LLM_HOST"):
        monkeypatch.setenv(k, "x")
    from labpipes.definitions import defs

    job = defs.resolve_job_def("ingest_month")
    assert {k.to_user_string() for k in job.asset_layer.selected_asset_keys} == {
        "taxi_zones", "trip_files", "raw_trips", "stg_trips", "zone_hour_stats", "trip_features"
    }
    graph = defs.resolve_asset_graph()
    (key,) = [k for k in graph.asset_check_keys if k.name == "schema_contract"]
    assert key.asset_key.to_user_string() == "trip_files"
    assert graph.get_check_spec(key).blocking is True  # contracts/platform.yaml: it must block
