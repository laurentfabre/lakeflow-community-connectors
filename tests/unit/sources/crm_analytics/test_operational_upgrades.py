"""Independent operational contracts using synthetic HTTP, never live credentials."""

# pylint: disable=redefined-outer-name,unused-import
import copy
import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from databricks.labs.community_connector.sources.crm_analytics import (
    crm_analytics_performance as perf,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_operations import (
    DIAGNOSTICS_TABLE,
    connection_checks,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_schema import (
    compare_schema,
    schema_baseline,
    validate_schema_baseline,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_setup import (
    run_diagnostics,
    validate_pipeline,
)
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowBatchReader
from tests.unit.sources.crm_analytics.test_crm_analytics_lakeflow_connect import (
    CONFIG,
    DATASET,
    NEXT_VERSION,
    OPTIONS,
    ROWS,
    VERSION,
    CrmAnalyticsError,
    CrmAnalyticsLakeflowConnect,
    connector,
    exhaust,
    query_text,
    replace_rows,
    response_body,
    simulator,
    wire,
)
from tests.unit.sources.crm_analytics.test_response_contracts import source, source_module

STRICT = {**OPTIONS, "selected_columns": ["Account.Id", "Amount", "Region"]}
SQLITE = {**STRICT, "key_validation_mode": "sqlite", "key_validation_cache_kib": 64}
RAW = {
    "supported_scope": OPTIONS["supported_scope"],
    "scope_fingerprint": OPTIONS["scope_fingerprint"],
    "selected_columns": ["Account.Id", "Amount", "Region"],
    "auto_increment_column": "_ingestion_row_id",
}


def posts(wire):
    return [r for r in wire[0] if r.method == "POST"]


@pytest.mark.parametrize(
    "status,code,action",
    [
        (401, "AUTHENTICATION", "Reauthorize"),
        (403, "PERMISSION_DENIED", "permissions"),
        (404, "RESOURCE_NOT_FOUND", "dataset/version"),
        (429, "RATE_LIMITED", "quotas"),
    ],
)
def test_health_classifies_safe_source_errors(source, wire, status, code, action):
    sentinel = "private-body-value-must-not-be-returned"

    def transform(prepared, response):
        response.status_code = status
        return response_body(response, {"message": sentinel, "token": CONFIG["access_token"]})

    wire[1]["transform"] = transform
    report = source.check_connection(DATASET, STRICT)
    assert report["ready"] is False
    issue = next(c for c in report["checks"] if c["name"] == code)
    assert issue["status"] == "error" and action in issue["action"]
    assert sentinel not in json.dumps(report)
    assert CONFIG["access_token"] not in json.dumps(report)
    assert not posts(wire)


def test_health_inspection_plan_are_metadata_only_and_count_is_explicit(source, wire):
    report = source.check_connection(DATASET, STRICT)
    assert report["ready"] and not report["snapshot_completed"]
    assert report["schema_baseline"]["dataset_id"] == DATASET
    inspection = source.inspect_dataset(DATASET)
    assert inspection["kind"] == "crm_analytics_inspection"
    assert inspection["key_suggestions_verified"] is False
    assert inspection["row_values_checked"] is False
    plan = source.get_extraction_plan(DATASET, STRICT)
    assert plan["expected_row_count"] == 7
    assert plan["data_pages"] == 4 and plan["minimum_query_requests"] == 7
    assert plan["max_offset"] == 7 and plan["parallel_extraction"] is False
    assert not posts(wire)
    report = source.check_connection(DATASET, STRICT, check_count=True)
    assert report["ready"] and len(posts(wire)) == 1
    assert "count()" in query_text(posts(wire)[0])
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, STRICT)


def test_discovery_and_allowlist_expiration_checks(source, wire):
    report = source.check_connection()
    assert report["ready"] and report["datasets"] == [DATASET]
    checks = connection_checks(
        {
            "options": {
                "externalOptionsAllowList": "source_primary_keys,scope_fingerprint,supported_scope",
                "access_token_expiration": "2000-01-01T00:00:00Z",
                "refresh_token_expiration": "2999-01-01T00:00:00Z",
                "client_secret": "must-not-appear",
            }
        },
        {**STRICT, "skip_unchanged_versions": True, "auto_increment_column": "row_id"},
    )
    allowlist = next(c for c in checks if c["name"] == "connection_allowlist")
    assert allowlist["status"] == "error"
    assert "auto_increment_column" in allowlist["message"]
    assert "expected_snapshot_fingerprint" in allowlist["message"]
    assert "selected_columns" in allowlist["message"]
    assert "skip_unchanged_versions" not in allowlist["message"]
    assert next(c for c in checks if c["name"] == "access_token_expiration")["status"] == "warning"
    assert next(c for c in checks if c["name"] == "refresh_token_expiration")["status"] == "ok"
    assert "must-not-appear" not in json.dumps(checks)
    assert not posts(wire)


def test_inspection_exposes_unsupported_fields_without_losing_supported_columns(
    source, simulator, wire
):
    xmd = simulator.corpus.tables["versions"][0]["xmdMain"]
    xmd["derivedMeasures"] = [{"field": "Calculated"}]
    xmd["dimensions"].extend([{"field": "NAME"}, {"field": "unsafe'field"}])
    report = source.inspect_dataset(DATASET)
    columns = {c["source_name"]: c for c in report["columns"]}
    assert columns["Amount"]["supported"] and columns["Amount"]["type"] == "decimal(38,18)"
    assert not columns["Calculated"]["supported"]
    assert not columns["unsafe'field"]["supported"]
    assert not columns["NAME"]["supported"] and not columns["Name"]["supported"]
    assert columns["Account.Id"]["destination_name"] == "Account__Id"
    assert "Account.Id" in report["key_suggestions"]
    assert not posts(wire)


@pytest.mark.parametrize("dataset_id", [None, "../datasets", "bad", "0Fb000000000001?token=x"])
def test_inspection_rejects_bad_ids_before_http(source, wire, dataset_id):
    with pytest.raises(RuntimeError, match="Salesforce ID"):
        source.inspect_dataset(dataset_id)
    assert not wire[0]


def test_spark_virtual_report_and_notebook_options(source, wire):
    options = {
        "tableName": DIAGNOSTICS_TABLE,
        "diagnostic_action": "inspect",
        "diagnostic_dataset_id": DATASET,
    }
    schema = source.get_table_schema(DIAGNOSTICS_TABLE, options)
    reader = LakeflowBatchReader(options, schema, source)
    records = list(reader.read(reader.partitions()[0]))
    report = json.loads(records[0]["report"])
    assert report["dataset_id"] == DATASET and report["kind"] == "crm_analytics_inspection"
    assert not posts(wire)

    class FakeRead:
        def __init__(self):
            self.kwargs = {}

        def format(self, value):
            assert value == "lakeflow_connect"
            return self

        def option(self, name, value):
            self.kwargs[name] = value
            return self

        def options(self, **kwargs):
            self.kwargs.update(kwargs)
            return self

        def load(self):
            return self

        def collect(self):
            return [{"report": json.dumps(report)}]

    fake = FakeRead()
    actual = run_diagnostics(
        SimpleNamespace(read=fake),
        connection_name="existing-uc",
        action="inspect",
        dataset_id=DATASET,
    )
    assert {key: value for key, value in actual.items() if key != "oauth_lifecycle"} == {
        key: value for key, value in report.items() if key != "oauth_lifecycle"
    }
    assert actual["oauth_lifecycle"] == {
        **report["oauth_lifecycle"],
        "fresh_read_attempts": 1,
        "credential_reinjections": 0,
        "recovered": False,
        "reauthorization_required": False,
    }
    assert fake.kwargs["databricks.connection"] == "existing-uc"
    assert fake.kwargs["tableName"] == DIAGNOSTICS_TABLE
    assert fake.kwargs["diagnostic_dataset_id"] == DATASET
    assert "access_token" not in fake.kwargs


def test_notebook_initialization_errors_are_sanitized():
    class FailedRead:
        def format(self, _):
            raise RuntimeError("access_token=private-token-value")

    report = run_diagnostics(SimpleNamespace(read=FailedRead()), connection_name="existing-uc")
    assert report["ready"] is False
    assert "private-token-value" not in json.dumps(report)
    assert "USE CONNECTION" in report["checks"][0]["action"]


def test_strict_baseline_roundtrip_and_column_reordering(source):
    pin = source.pin_table(DATASET, STRICT)
    baseline = schema_baseline(pin)
    assert validate_schema_baseline(json.dumps(baseline)) == baseline
    assert compare_schema(baseline, pin)["compatible"]
    reordered = copy.deepcopy(pin)
    reordered["schema_json"]["fields"].reverse()
    assert compare_schema(baseline, reordered)["compatible"]
    options = {**STRICT, "schema_baseline": baseline}
    assert len(exhaust(source, options)) == 7


@pytest.mark.parametrize("mode,compatible", [("strict", False), ("additive", True)])
def test_source_schema_addition_is_explicit_and_reported(
    source, source_module, simulator, wire, mode, compatible
):
    baseline = schema_baseline(source.pin_table(DATASET, OPTIONS))
    simulator.corpus.tables["versions"][0]["xmdMain"]["dimensions"].append({"field": "NewNullable"})
    for row in simulator.corpus.tables[ROWS]:
        row["NewNullable"] = "new-source-value"
    options = {**OPTIONS, "schema_baseline": baseline, "schema_evolution_mode": mode}
    fresh = type(source)(CONFIG)
    report = fresh.check_connection(DATASET, options)
    assert report["schema_migration"]["compatible"] is compatible
    assert report["schema_migration"]["added_columns"] == ["NewNullable"]
    assert not posts(wire)
    if compatible:
        rows = exhaust(fresh, options)
        assert all(row["NewNullable"] == "new-source-value" for row in rows)
    else:
        with pytest.raises(RuntimeError, match="Schema evolution policy"):
            exhaust(fresh, options)
        assert not posts(wire)
    fresh.close()


@pytest.mark.parametrize("mode", ["strict", "additive"])
@pytest.mark.parametrize(
    "mutation", ["removed", "type", "keys", "mapping", "security", "scope", "nullable"]
)
def test_breaking_schema_contracts_are_never_additive(source, mode, mutation):
    pin = source.pin_table(DATASET, STRICT)
    baseline = schema_baseline(pin)
    changed = copy.deepcopy(pin)
    if mutation == "removed":
        changed["schema_json"]["fields"] = [
            f for f in changed["schema_json"]["fields"] if f["name"] != "Amount"
        ]
        changed["column_mapping"].pop("Amount")
    elif mutation == "type":
        next(f for f in changed["schema_json"]["fields"] if f["name"] == "Amount")[
            "type"
        ] = "string"
    elif mutation == "keys":
        changed["primary_keys"] = ["Account__Id"]
        changed["source_primary_keys"] = ["Account.Id"]
    elif mutation == "mapping":
        changed["column_mapping"]["Region"] = "Account__Id"
    elif mutation == "security":
        changed["security_fingerprint"] = "a" * 64
    elif mutation == "scope":
        changed["scope_fingerprint"] = "different-approved-user"
    else:
        next(f for f in changed["schema_json"]["fields"] if f["name"] == "Amount")[
            "nullable"
        ] = False
    assert compare_schema(baseline, changed, mode)["compatible"] is False


@pytest.mark.parametrize("mode", ["strict", "additive"])
@pytest.mark.parametrize("mutation", ["removed", "type", "keys", "mapping", "security", "scope"])
def test_schema_guards_block_real_capture_changes_before_row_queries(
    source, simulator, wire, mode, mutation
):
    baseline = schema_baseline(source.pin_table(DATASET, OPTIONS))
    options = {**OPTIONS, "schema_baseline": baseline, "schema_evolution_mode": mode}
    xmd = simulator.corpus.tables["versions"][0]["xmdMain"]
    if mutation in {"removed", "type"}:
        xmd["measures"] = [m for m in xmd["measures"] if m["field"] != "Amount"]
        if mutation == "type":
            xmd["dimensions"].append({"field": "Amount"})
    elif mutation == "keys":
        options["source_primary_keys"] = ["Account.Id"]
    elif mutation == "mapping":
        options["column_name_mode"] = "legacy_hex"
    elif mutation == "security":
        simulator.corpus.tables["datasets"][0]["visibility"] = "different-visible-scope"
    else:
        options["scope_fingerprint"] = "different-approved-user"
    fresh = type(source)(CONFIG)
    try:
        with pytest.raises(RuntimeError, match="Schema evolution policy"):
            exhaust(fresh, options)
        assert not posts(wire)
        report = fresh.check_connection(DATASET, options)
        assert report["ready"] is False
        assert report["schema_migration"]["compatible"] is False
        assert not posts(wire)
    finally:
        fresh.close()


@pytest.mark.parametrize(
    "options",
    [
        {**STRICT, "schema_evolution_mode": "additive"},
        {**STRICT, "key_validation_mode": "hash"},
        {**STRICT, "key_validation_cache_kib": 63},
        {**STRICT, "key_validation_cache_kib": 65537},
        {**STRICT, "key_validation_cache_kib": True},
    ],
)
def test_invalid_operational_settings_fail_before_http(source, wire, options):
    report = source.check_connection(DATASET, options)
    assert report["ready"] is False
    assert not wire[0]


def test_pipeline_validation_uses_uc_for_every_configured_table(monkeypatch):
    from databricks.labs.community_connector.sources.crm_analytics import (
        crm_analytics_setup as setup,
    )

    calls = []

    def diagnose(spark, **kwargs):
        calls.append(kwargs)
        return {"ready": kwargs["dataset_id"] == DATASET}

    monkeypatch.setattr(setup, "run_diagnostics", diagnose)
    spec = {
        "connection_name": "existing-uc",
        "objects": [
            {"table": {"source_table": DATASET, "table_configuration": STRICT}},
            {"table": {"source_table": NEXT_VERSION, "table_configuration": RAW}},
        ],
    }
    result = validate_pipeline(object(), spec, check_count=True)
    assert result["ready"] is False and len(result["tables"]) == 2
    assert [c["dataset_id"] for c in calls] == [DATASET, NEXT_VERSION]
    assert all(c["connection_name"] == "existing-uc" and c["check_count"] is True for c in calls)
    assert calls[0]["table_options"] == STRICT and calls[1]["table_options"] == RAW


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "fields_type",
        "field_missing",
        "unknown_type",
        "nullability",
        "duplicate",
        "extra",
        "root_type",
        "field_extra",
        "metadata",
    ],
)
def test_malformed_schema_baselines_are_sanitized(source, mutation):
    baseline = schema_baseline(source.pin_table(DATASET, STRICT))
    if mutation == "missing":
        baseline.pop("schema_json")
    elif mutation == "fields_type":
        baseline["schema_json"]["fields"] = "not-a-field-list"
    elif mutation == "field_missing":
        baseline["schema_json"]["fields"][0].pop("type")
    elif mutation == "unknown_type":
        baseline["schema_json"]["fields"][0]["type"] = "secret-value-that-must-not-appear"
    elif mutation == "nullability":
        baseline["schema_json"]["fields"][0]["nullable"] = "not-a-bool"
    elif mutation == "duplicate":
        baseline["schema_json"]["fields"].append(
            copy.deepcopy(baseline["schema_json"]["fields"][0])
        )
    elif mutation == "root_type":
        baseline["schema_json"]["type"] = "array"
    elif mutation == "field_extra":
        baseline["schema_json"]["fields"][0]["credential"] = "secret-value-that-must-not-appear"
    elif mutation == "metadata":
        baseline["schema_json"]["fields"][0]["metadata"] = "not-an-object"
    else:
        baseline["credential"] = "secret-value-that-must-not-appear"
    with pytest.raises(CrmAnalyticsError) as error:
        validate_schema_baseline(baseline)
    assert "secret-value-that-must-not-appear" not in str(error.value)


@pytest.fixture
def private_tmp(monkeypatch, tmp_path):
    monkeypatch.setattr(perf.tempfile, "tempdir", str(tmp_path))
    return tmp_path


@pytest.mark.parametrize("completion", ["exhaust", "close", "failure"])
def test_sqlite_private_storage_cleanup_and_evidence(connector, simulator, private_tmp, completion):
    iterator, _ = connector.read_table(DATASET, None, SQLITE)
    assert next(iterator)
    dirs = list(private_tmp.glob("crm-analytics-keys-*"))
    assert len(dirs) == 1 and os.stat(dirs[0]).st_mode & 0o777 == 0o700
    databases = list(dirs[0].glob("*.sqlite"))
    assert len(databases) == 1 and os.stat(databases[0]).st_mode & 0o777 == 0o600
    if completion == "exhaust":
        assert len(list(iterator)) == 6
        evidence = connector.get_read_evidence(DATASET, SQLITE)
        assert evidence["unique_key_count"] == 7
        assert evidence["key_validation_storage"] == "sqlite"
    elif completion == "close":
        iterator.close()
    else:
        simulator.corpus.tables[ROWS][-1]["Account.Id"] = simulator.corpus.tables[ROWS][0][
            "Account.Id"
        ]
        simulator.corpus.tables[ROWS][-1]["Region"] = simulator.corpus.tables[ROWS][0]["Region"]
        with pytest.raises(RuntimeError, match="duplicate"):
            list(iterator)
    assert not list(private_tmp.iterdir())
    if completion != "exhaust":
        with pytest.raises(RuntimeError, match="not completed"):
            connector.get_read_evidence(DATASET, SQLITE)


def test_sqlite_distinguishes_composite_unicode_and_separator_keys(
    connector, simulator, private_tmp
):
    rows = replace_rows(simulator, 7)
    pairs = [
        ("a|b", "c"),
        ("a", "b|c"),
        ("é", "x"),
        ("e\u0301", "x"),
        ("A", "x"),
        ("a", "x"),
        ("a", ""),
    ]
    for row, (account, region) in zip(rows, pairs):
        row.update({"Account.Id": account, "Region": region})
    actual = exhaust(connector, SQLITE)
    assert {(r["Account__Id"], r["Region"]) for r in actual} == set(pairs)
    assert not list(private_tmp.iterdir())


@pytest.mark.parametrize("where", ["initialize", "write"])
def test_sqlite_storage_errors_abort_without_evidence(connector, monkeypatch, private_tmp, where):
    if where == "initialize":

        def fail(*args, **kwargs):
            raise sqlite3.OperationalError("private-key-value")

        monkeypatch.setattr(perf.sqlite3, "connect", fail)
    else:
        original = perf.sqlite3.connect

        class FailedWrite:
            def __init__(self, *args, **kwargs):
                self.connection = original(*args, **kwargs)

            def execute(self, query, *args):
                if query.startswith("INSERT"):
                    raise sqlite3.OperationalError("private-key-value")
                return self.connection.execute(query, *args)

            def close(self):
                self.connection.close()

        monkeypatch.setattr(perf.sqlite3, "connect", FailedWrite)
    with pytest.raises(RuntimeError, match="storage") as error:
        exhaust(connector, SQLITE)
    assert "private-key-value" not in str(error.value)
    assert not list(private_tmp.iterdir())
    with pytest.raises(RuntimeError, match="not completed"):
        connector.get_read_evidence(DATASET, SQLITE)


def test_generated_keys_allocate_no_sqlite_index(connector, monkeypatch, private_tmp):
    def forbidden(*args, **kwargs):
        raise AssertionError("generated keys must not allocate SQLite storage")

    monkeypatch.setattr(perf.sqlite3, "connect", forbidden)
    options = {**RAW, "key_validation_mode": "sqlite"}
    rows = exhaust(connector, options)
    assert [r["_ingestion_row_id"] for r in rows] == list(range(1, 8))
    assert (
        connector.get_read_evidence(DATASET, options)["key_validation_storage"]
        == "generated_counter"
    )
    assert not list(private_tmp.iterdir())


def test_large_snapshot_crosses_high_offsets_without_truncation(simulator, wire, private_tmp):
    replace_rows(simulator, 10007)
    source = CrmAnalyticsLakeflowConnect({**CONFIG, "page_size": "1000"})
    try:
        rows = exhaust(source, SQLITE)
        assert len(rows) == 10007 and len({r["Account__Id"] for r in rows}) == 10007
        assert any("offset q 10000;" in query_text(r) for r in posts(wire))
        assert any("offset q 10007;" in query_text(r) for r in posts(wire))
        evidence = source.get_read_evidence(DATASET, SQLITE)
        assert evidence["data_pages"] == 11 and evidence["page_queries"] == 12
        assert evidence["minimum_query_requests"] == evidence["query_requests"] == 14
        assert evidence["peak_page_rows"] == 1000
        assert evidence["rows_per_second"] > 0
        assert evidence["terminal_empty_page"] and evidence["metadata_revalidated"]
        assert not list(private_tmp.iterdir())
    finally:
        source.close()


def test_plan_marks_limits_before_row_queries(source, wire):
    plan = source.get_extraction_plan(DATASET, {**STRICT, "max_query_requests": 6, "max_keys": 6})
    assert not plan["within_query_budget"] and not plan["within_key_capacity"]
    report = source.check_connection(DATASET, {**STRICT, "max_query_requests": 6, "max_keys": 6})
    assert not report["ready"]
    assert {c["name"] for c in report["checks"] if c["status"] == "error"} == {
        "query_budget",
        "key_capacity",
    }
    assert not posts(wire)


@pytest.mark.parametrize("generated", [False, True])
def test_registered_merged_reader_operational_features(simulator, generated):
    """Exercise the deployed registration closure, not just modular helpers."""
    from types import SimpleNamespace

    from databricks.labs.community_connector.sources.crm_analytics import (
        _generated_crm_analytics_python_source as merged,
    )

    registered = []
    merged.register_lakeflow_source(
        SimpleNamespace(dataSource=SimpleNamespace(register=registered.append))
    )
    assert len(registered) == 1
    settings = {
        **OPTIONS,
        "selected_columns": ["Account.Id", "Region", "Amount"],
        "key_validation_mode": "sqlite",
    }
    if generated:
        settings["auto_increment_column"] = "_ingestion_row_id"
    data_source = registered[0]({**CONFIG, "tableName": DATASET})
    source = data_source.lakeflow_connect
    try:
        report = source.check_connection(DATASET, settings)
        assert report["ready"] is True
        assert source.inspect_dataset(DATASET)["kind"] == "crm_analytics_inspection"
        guarded = {
            **settings,
            "schema_baseline": report["schema_baseline"],
            "schema_evolution_mode": "strict",
        }
        rows = list(source.read_table(DATASET, None, guarded)[0])
        assert len(rows) == 7
        evidence = source.get_read_evidence(DATASET, guarded)
        assert evidence["key_validation_storage"] == (
            "generated_counter" if generated else "sqlite"
        )
        assert evidence["extracted_row_count"] == evidence["unique_key_count"] == 7
    finally:
        source.close()
