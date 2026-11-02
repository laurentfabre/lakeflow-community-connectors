"""OAuth and durable monitoring contracts using synthetic HTTP and local files.

The local filesystem redirects stand in for Volume I/O. Collector tests assert
the Spark API contract; they do not execute Auto Loader or Delta transactions.
"""

# pylint: disable=redefined-outer-name,unused-import
import builtins
import copy
import json
import os
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from databricks.labs.community_connector.sources.crm_analytics import (
    crm_analytics_monitoring as monitoring,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_operations import (
    connection_checks,
    oauth_lifecycle_report,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_setup import (
    ensure_connection,
    run_diagnostics,
    validate_pipeline,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_support import (
    CrmAnalyticsError,
)
from tests.unit.sources.crm_analytics.test_crm_analytics_lakeflow_connect import (
    CONFIG,
    DATASET,
    OPTIONS,
    ROWS,
    VERSION,
    exhaust,
    query_text,
    replace_rows,
    response_body,
    simulator,
    wire,
)
from tests.unit.sources.crm_analytics.test_response_contracts import source, source_module

VOLUME = "/Volumes/synthetic-catalog/crm_analytics/monitoring/events"
SETTINGS = {**OPTIONS, "selected_columns": ["Account.Id", "Region", "Amount"]}
MONITORED = {**SETTINGS, "monitoring_volume_path": VOLUME}


class SparkReports:
    """A new read builder on every access; reports stand in for UC injection."""

    def __init__(self, reports):
        self.reports = list(reports)
        self.builders = []

    @property
    def read(self):
        parent = self

        class Builder:
            def __init__(self):
                self.options_map = {}

            def format(self, name):
                assert name == "lakeflow_connect"
                return self

            def option(self, name, value):
                self.options_map[name] = value
                return self

            def options(self, **options):
                self.options_map.update(options)
                return self

            def load(self):
                return self

            def collect(self):
                value = parent.reports.pop(0)
                if isinstance(value, Exception):
                    raise value
                if callable(value):
                    value = value(self.options_map)
                return [{"report": json.dumps(value)}]

        builder = Builder()
        self.builders.append(builder)
        return builder


def report(code=None, *, verified=False, ready=None):
    return {
        "format_version": 1,
        "kind": "crm_analytics_preflight",
        "ready": not code if ready is None else ready,
        "checks": [{"name": code, "status": "error"}] if code else [],
        "oauth_lifecycle": {"source_access_verified": verified},
    }


@pytest.mark.parametrize("action", ["health", "discover", "inspect"])
def test_fresh_uc_read_recovers_once_without_passing_credentials(action):
    failed = report("AUTHENTICATION")
    if action == "inspect":
        failed = {"format_version": 1, "code": "AUTHENTICATION", "ready": False}
    spark = SparkReports([failed, report(verified=True)])
    result = run_diagnostics(
        spark, connection_name="existing-uc", action=action, dataset_id=DATASET
    )
    assert result["ready"] and len(spark.builders) == 2
    assert spark.builders[0] is not spark.builders[1]
    assert result["oauth_lifecycle"] == {
        "source_access_verified": True,
        "managed_by": "unity_catalog",
        "fresh_read_attempts": 2,
        "credential_reinjections": 1,
        "recovered": True,
        "reauthorization_required": False,
    }
    for builder in spark.builders:
        assert builder.options_map["databricks.connection"] == "existing-uc"
        assert not {"access_token", "refresh_token", "client_secret"} & builder.options_map.keys()


def test_persistent_401_stops_after_two_and_ensure_connection_is_actionable():
    spark = SparkReports([report("AUTHENTICATION")] * 3)
    result = run_diagnostics(spark, connection_name="existing-uc")
    assert not result["ready"] and len(spark.builders) == 2 and len(spark.reports) == 1
    assert result["oauth_lifecycle"]["reauthorization_required"]
    assert not result["oauth_lifecycle"]["recovered"]
    spark = SparkReports([report("AUTHENTICATION")] * 3)
    with pytest.raises(CrmAnalyticsError, match="same Salesforce integration user") as error:
        ensure_connection(spark, connection_name="existing-uc")
    assert error.value.code == "AUTHENTICATION" and len(spark.builders) == 2
    assert "refresh_token scopes" in str(error.value)


@pytest.mark.parametrize("action", ["health", "discover", "inspect"])
def test_new_uc_reader_consumes_new_bearer_and_real_diagnostic_report(source, wire, action):
    def transform(prepared, response):
        if prepared.headers.get("Authorization") == "Bearer synthetic-stale-token":
            response.status_code = 401
            return response_body(response, {"error": "private-auth-response"})
        return response

    wire[1]["transform"] = transform

    def uc_read(token):
        def diagnostics(options):
            instance = type(source)({**CONFIG, "access_token": token})
            try:
                rows, _ = instance.read_table("_crm_analytics_diagnostics", None, options)
                return json.loads(list(rows)[0]["report"])
            finally:
                instance.close()

        return diagnostics

    spark = SparkReports([uc_read("synthetic-stale-token"), uc_read("synthetic-renewed-token")])
    result = run_diagnostics(
        spark,
        connection_name="existing-uc",
        action=action,
        dataset_id=DATASET,
        table_options=SETTINGS,
    )
    assert result["oauth_lifecycle"]["recovered"]
    assert result["oauth_lifecycle"]["source_access_verified"]
    assert len(spark.builders) == 2
    assert wire[0][0].headers["Authorization"] == "Bearer synthetic-stale-token"
    assert all(
        request.headers["Authorization"] == "Bearer synthetic-renewed-token"
        for request in wire[0][1:]
    )
    assert all(
        request.method == "GET" and "/oauth2/token" not in request.url for request in wire[0]
    )
    assert "private-auth-response" not in json.dumps(result)


@pytest.mark.parametrize(
    "option,value",
    [
        ("retry_authentication", 1),
        ("retry_authentication", "true"),
        ("check_count", 0),
        ("check_count", "false"),
    ],
)
def test_diagnostic_boolean_controls_reject_ambiguous_values(option, value):
    spark = SparkReports([report(verified=True)])
    with pytest.raises(CrmAnalyticsError, match="boolean"):
        run_diagnostics(spark, connection_name="existing-uc", **{option: value})
    assert not spark.builders


@pytest.mark.parametrize(
    "code", ["PERMISSION_DENIED", "RESOURCE_NOT_FOUND", "RATE_LIMITED", "CONNECTOR_ERROR"]
)
def test_non_authentication_failures_never_request_reinjection(code):
    spark = SparkReports([report(code), report(verified=True)])
    result = run_diagnostics(spark, connection_name="existing-uc")
    assert not result["ready"] and len(spark.builders) == 1
    assert result["oauth_lifecycle"]["credential_reinjections"] == 0
    spark = SparkReports([report(code)])
    with pytest.raises(CrmAnalyticsError) as error:
        ensure_connection(spark, connection_name="existing-uc")
    assert error.value.code == "PREFLIGHT_FAILED"


def test_auth_retry_optout_and_unverified_recovery_are_reported_conservatively():
    spark = SparkReports([report("AUTHENTICATION"), report(verified=True)])
    result = run_diagnostics(spark, connection_name="existing-uc", retry_authentication=False)
    assert len(spark.builders) == 1 and not result["oauth_lifecycle"]["reauthorization_required"]
    spark = SparkReports([report("AUTHENTICATION"), report(ready=True)])
    result = run_diagnostics(spark, connection_name="existing-uc")
    assert result["ready"] and not result["oauth_lifecycle"]["recovered"]


def test_execution_errors_are_sanitized_without_guessing_authentication():
    secret = "credential-from-platform-must-never-be-shown"
    spark = SparkReports([RuntimeError(secret), report(verified=True)])
    result = run_diagnostics(spark, connection_name="existing-uc")
    assert len(spark.builders) == 1 and not result["ready"]
    assert secret not in json.dumps(result)
    assert not result["oauth_lifecycle"]["reauthorization_required"]


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {"format_version": 2},
        {"format_version": 1, "checks": "bad"},
        {"format_version": 1, "oauth_lifecycle": []},
    ],
)
def test_malformed_diagnostic_reports_do_not_trigger_retry(value):
    spark = SparkReports([value, report(verified=True)])
    with pytest.raises(CrmAnalyticsError, match="Malformed"):
        run_diagnostics(spark, connection_name="existing-uc")
    assert len(spark.builders) == 1


def test_validate_pipeline_uses_bounded_auth_retry_for_each_dataset():
    spark = SparkReports([report("AUTHENTICATION"), report(verified=True)])
    spec = {
        "connection_name": "existing-uc",
        "objects": [{"table": {"source_table": DATASET, "table_configuration": SETTINGS}}],
    }
    result = validate_pipeline(spark, spec)
    assert result["ready"] and result["tables"][0]["oauth_lifecycle"]["recovered"]
    assert len(spark.builders) == 2


@pytest.mark.parametrize(
    "seconds,state", [(-3600, "expired"), (120, "expiring_soon"), (3600, "future")]
)
def test_expiration_states_are_advisory_for_valid_source(source, wire, monkeypatch, seconds, state):
    expiry = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
    metadata = {"ACCESS_TOKEN_EXPIRATION": expiry, "refresh_token_expiration": expiry}
    lifecycle = oauth_lifecycle_report(metadata, source_access_verified=True)
    assert lifecycle["access_token_expiration"] == lifecycle["refresh_token_expiration"] == state
    assert lifecycle["expiration_metadata_advisory"]
    checks = connection_checks(metadata)
    expiry_checks = [check for check in checks if "expiration" in check["name"]]
    assert len(expiry_checks) == 2
    assert {check["status"] for check in expiry_checks} == (
        {"ok"} if seconds > 300 else {"warning"}
    )
    monkeypatch.setattr(source, "options", {**source.options, **metadata})
    result = source.check_connection(DATASET, SETTINGS)
    assert result["ready"] and result["oauth_lifecycle"]["source_access_verified"]
    assert not [request for request in wire[0] if request.method == "POST"]


def test_unknown_expiration_metadata_never_exposes_values():
    secret = "not-a-date-secret-token-value"
    result = oauth_lifecycle_report({"access_token_expiration": secret})
    assert result["access_token_expiration"] == result["refresh_token_expiration"] == "unknown"
    assert secret not in json.dumps(connection_checks({"access_token_expiration": secret}))


@pytest.fixture
def event_sink(tmp_path, monkeypatch):
    """Redirect only approved Volume I/O; keep the production path validator."""
    original_open, original_makedirs = builtins.open, os.makedirs

    def mapped(path):
        text = str(path)
        return (
            tmp_path / text[len(VOLUME) :].lstrip("/")
            if text == VOLUME or text.startswith(VOLUME + "/")
            else path
        )

    def open_file(path, *args, **kwargs):
        return original_open(mapped(path), *args, **kwargs)

    def make_directory(path, *args, **kwargs):
        return original_makedirs(mapped(path), *args, **kwargs)

    monkeypatch.setattr(builtins, "open", open_file)
    monkeypatch.setattr(os, "makedirs", make_directory)

    def read_events():
        events = [json.loads(path.read_text()) for path in tmp_path.glob("*.json")]
        return sorted(
            events,
            key=lambda event: (
                event["event_time"],
                {"started": 0, "completed": 1, "cancelled": 2, "failed": 3}[event["status"]],
            ),
        )

    return read_events


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/events",
        "/Volumes/a/b",
        "/Volumes/a/b/c/../events",
        "/Volumes/a/b/c/./events",
        "/Volumes/a/b/c//events",
        "/Volumes/a/b/c?token=x",
        "/Volumes/a/b/c\\events",
        "https://example.com/events",
        None,
        "",
    ],
)
def test_monitoring_path_validation_rejects_unsafe_locations(path):
    with pytest.raises(CrmAnalyticsError):
        monitoring.validate_monitoring_path(path)


def test_monitoring_valid_path_and_schema_are_explicit_without_spark():
    assert monitoring.validate_monitoring_path(VOLUME + "/") == VOLUME
    schema = monitoring.monitoring_schema()
    assert schema["event_time"].dataType.simpleString() == "timestamp"
    assert schema["extracted_row_count"].dataType.simpleString() == "bigint"
    assert schema["target_committed"].dataType.simpleString() == "boolean"
    assert len(schema.names) == len(set(schema.names)) == 22


def test_disabled_monitoring_and_monitoring_tuning_preserve_identity(source, event_sink):
    baseline = source.pin_table(DATASET, SETTINGS)
    assert source.pin_table(DATASET, MONITORED) == baseline
    assert (
        source.pin_table(DATASET, {**MONITORED, "monitoring_volume_path": VOLUME + "/other"})
        == baseline
    )
    rows = exhaust(source, SETTINGS)
    assert len(rows) == 7 and event_sink() == []
    evidence = source.get_read_evidence(DATASET, SETTINGS)
    assert source.get_read_evidence(DATASET, MONITORED) == evidence


@pytest.mark.parametrize("row_count", [0, 7])
def test_complete_events_match_validated_extraction_not_target_commit(
    source, simulator, wire, event_sink, row_count
):
    replace_rows(simulator, row_count)
    rows = exhaust(source, MONITORED)
    events = event_sink()
    assert [event["status"] for event in events] == ["started", "completed"]
    assert len({event["event_id"] for event in events}) == 2
    assert len({event["attempt_id"] for event in events}) == 1
    for event in events:
        UUID(event["event_id"])
        UUID(event["attempt_id"])
        assert set(event) == set(monitoring.monitoring_schema().names)
        assert event["format_version"] == 1 and event["target_committed"] is False
    end = events[-1]
    assert end["dataset_id"] == DATASET and end["version_id"] == VERSION
    assert end["expected_row_count"] == end["extracted_row_count"] == len(rows) == row_count
    assert end["data_pages"] == (row_count + 1) // 2
    assert end["page_queries"] == end["data_pages"] + 1
    assert end["query_requests"] == end["data_pages"] + 3
    assert end["http_requests"] == len(wire[0])
    assert end["source_age_seconds"] >= 0 and end["source_created_at"]
    assert end["elapsed_seconds"] >= 0 and end["error_code"] is None
    serialized = json.dumps(events)
    for private in [
        CONFIG["access_token"],
        OPTIONS["scope_fingerprint"],
        "Account.Id",
        "fixture-region",
    ]:
        assert private not in serialized
    assert source.get_read_evidence(DATASET, MONITORED)["extracted_row_count"] == row_count


@pytest.mark.parametrize("when", ["metadata", "rows"])
def test_authentication_failure_is_durable_without_source_body(source, wire, event_sink, when):
    sentinel = "private-source-body-and-credential"

    def transform(prepared, response):
        if when == "metadata" or prepared.method == "POST":
            response.status_code = 401
            return response_body(response, {"message": sentinel, "token": CONFIG["access_token"]})
        return response

    wire[1]["transform"] = transform
    with pytest.raises(RuntimeError, match="authentication"):
        exhaust(source, MONITORED)
    events = event_sink()
    assert [event["status"] for event in events] == ["started", "failed"]
    assert events[-1]["error_code"] == "AUTHENTICATION"
    assert events[-1]["http_requests"] == len(wire[0])
    assert sentinel not in json.dumps(events) and CONFIG["access_token"] not in json.dumps(events)
    # Permit the evidence helper's independent metadata lookup after a cold
    # metadata failure, while still requiring the failed attempt to have none.
    wire[1]["transform"] = None
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, MONITORED)


def test_invalid_read_configuration_is_recorded_before_any_source_request(source, wire, event_sink):
    with pytest.raises(RuntimeError, match="scope_fingerprint"):
        exhaust(source, {**MONITORED, "scope_fingerprint": ""})
    assert [event["status"] for event in event_sink()] == ["started", "failed"]
    assert event_sink()[-1]["error_code"] == "CONNECTOR_ERROR"
    assert not wire[0]


def test_monitoring_write_failure_never_masks_original_source_failure(
    source, wire, event_sink, monkeypatch
):
    original_dump = json.dump

    def dump(value, handle, **kwargs):
        if value.get("status") == "failed":
            raise OSError("synthetic-storage-failure")
        return original_dump(value, handle, **kwargs)

    def transform(_prepared, response):
        response.status_code = 401
        return response

    monkeypatch.setattr(json, "dump", dump)
    wire[1]["transform"] = transform
    with pytest.raises(RuntimeError, match="authentication") as error:
        exhaust(source, MONITORED)
    assert error.value.code == "AUTHENTICATION"


@pytest.mark.parametrize("failure", ["short_page", "duplicate", "invalid_value"])
def test_integrity_failures_emit_failed_without_business_values(
    source, simulator, wire, event_sink, failure
):
    sentinel = "private-business-key-do-not-log"
    if failure == "duplicate":
        simulator.corpus.tables[ROWS][1] = copy.deepcopy(simulator.corpus.tables[ROWS][0])
    elif failure == "invalid_value":
        simulator.corpus.tables[ROWS][0]["Amount"] = {"secret": sentinel}
    else:

        def transform(prepared, response):
            if prepared.method == "POST" and "count()" not in query_text(prepared):
                body = json.loads(response.text)
                body["results"]["records"] = []
                return response_body(response, body)
            return response

        wire[1]["transform"] = transform
    with pytest.raises(RuntimeError):
        exhaust(source, MONITORED)
    assert [event["status"] for event in event_sink()] == ["started", "failed"]
    assert sentinel not in json.dumps(event_sink())
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, MONITORED)


@pytest.mark.parametrize("consume", [0, 1])
def test_close_is_cancelled_even_before_first_row(source, event_sink, consume):
    iterator, _ = source.read_table(DATASET, None, MONITORED)
    if consume:
        next(iterator)
    iterator.close()
    iterator.close()
    assert list(iterator) == []
    assert [event["status"] for event in event_sink()] == ["started", "cancelled"]
    assert event_sink()[-1]["extracted_row_count"] == consume
    assert event_sink()[-1]["error_code"] == "CANCELLED"
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, MONITORED)


def test_unconsumed_attempt_has_no_completion_event(source, event_sink):
    iterator, _ = source.read_table(DATASET, None, MONITORED)
    assert [event["status"] for event in event_sink()] == ["started"]
    # No completion can be inferred from this state; emulate process loss by
    # inspecting before explicit cleanup rather than sending an actual signal.
    iterator.close()


@pytest.mark.parametrize("terminate_old", ["close", "error"])
def test_stale_iterator_cannot_erase_newer_success_evidence(source, event_sink, terminate_old):
    old, _ = source.read_table(DATASET, None, MONITORED)
    next(old)
    assert len(exhaust(source, MONITORED)) == 7
    newer = source.get_read_evidence(DATASET, MONITORED)
    if terminate_old == "close":
        old.close()
    else:
        with pytest.raises(RuntimeError, match="Concurrent"):
            next(old)
    assert source.get_read_evidence(DATASET, MONITORED) == newer
    assert len(event_sink()) == 4


@pytest.mark.parametrize("stage", ["started", "completed"])
def test_monitoring_persistence_failure_aborts_and_invalidates_old_evidence(
    source, event_sink, monkeypatch, stage
):
    exhaust(source, SETTINGS)
    original_dump = json.dump

    def dump(value, handle, **kwargs):
        if isinstance(value, dict) and value.get("status") == stage:
            raise OSError("storage-secret-that-must-not-escape")
        return original_dump(value, handle, **kwargs)

    monkeypatch.setattr(json, "dump", dump)
    with pytest.raises(RuntimeError, match="persist") as error:
        exhaust(source, MONITORED)
    assert "storage-secret" not in str(error.value)
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, MONITORED)


class Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_deadline_during_completion_write_finishes_with_failure(source, event_sink, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    original_dump = json.dump

    def dump(value, handle, **kwargs):
        result = original_dump(value, handle, **kwargs)
        if value.get("status") == "completed":
            clock.now += 2
        return result

    monkeypatch.setattr(json, "dump", dump)
    settings = {**MONITORED, "max_read_seconds": 1}
    with pytest.raises(RuntimeError, match="max_read_seconds"):
        exhaust(source, settings)
    assert [event["status"] for event in event_sink()] == ["started", "completed", "failed"]
    assert event_sink()[-1]["error_code"] == "READ_DEADLINE"
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, settings)


def test_elapsed_deadline_and_retry_counters_include_metadata_and_queries(
    source, wire, event_sink, monkeypatch
):
    clock = Clock()
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    source._retries = 1
    failed = set()

    def transform(prepared, response):
        category = prepared.method
        if category not in failed:
            failed.add(category)
            response.status_code = 503 if category == "GET" else 429
        return response

    wire[1]["transform"] = transform
    assert len(exhaust(source, MONITORED)) == 7
    end = event_sink()[-1]
    assert end["http_requests"] == len(wire[0]) == 13
    assert end["query_requests"] == 8 and end["retry_count"] == 2
    assert 2 <= end["retry_wait_seconds"] < 4
    assert end["elapsed_seconds"] == end["retry_wait_seconds"]


def test_runtime_query_budget_counts_only_sent_attempts(source, wire, event_sink, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    source._retries = 1
    seen = False

    def transform(prepared, response):
        nonlocal seen
        if prepared.method == "POST" and not seen:
            seen = True
            response.status_code = 503
        return response

    wire[1]["transform"] = transform
    settings = {**MONITORED, "max_query_requests": 7}
    with pytest.raises(RuntimeError, match="max_query_requests"):
        exhaust(source, settings)
    end = event_sink()[-1]
    assert end["status"] == "failed" and end["error_code"] == "QUERY_BUDGET"
    assert end["http_requests"] == len(wire[0])
    assert end["query_requests"] == len([r for r in wire[0] if r.method == "POST"]) == 7


class Collector:
    """Fluent Spark contract spy, deliberately not a Spark execution engine."""

    def __init__(self, failure=None):
        self.calls, self.failure = [], failure
        self.readStream = self.writeStream = self

    def __getattr__(self, method):
        def call(*args, **kwargs):
            self.calls.append((method, args, kwargs))
            if method == "awaitTermination" and self.failure:
                raise self.failure
            return self

        return call


def test_collector_uses_explicit_schema_durable_checkpoint_and_strict_contract(monkeypatch):
    monkeypatch.setattr(monitoring, "expr", lambda expression: expression)
    spark = Collector()
    checkpoint = "/Volumes/synthetic-catalog/crm_analytics/monitoring/checkpoints/events"
    result = monitoring.collect_monitoring_events(
        spark,
        monitoring_volume_path=VOLUME,
        monitoring_table="synthetic-catalog.crm_analytics.metrics",
        checkpoint_path=checkpoint,
    )
    assert result is spark
    calls = spark.calls
    assert ("format", ("cloudFiles",), {}) in calls
    assert ("format", ("delta",), {}) in calls
    assert ("option", ("mode", "FAILFAST"), {}) in calls
    assert ("option", ("checkpointLocation", checkpoint), {}) in calls
    assert ("trigger", (), {"availableNow": True}) in calls
    assert ("toTable", ("`synthetic-catalog`.`crm_analytics`.`metrics`",), {}) in calls
    assert calls[-1] == ("awaitTermination", (), {})
    schema = next(args[0] for method, args, _ in calls if method == "schema")
    assert schema.names == monitoring.monitoring_schema().names + ["_rescued_data"]
    expression = next(args[1] for method, args, _ in calls if method == "withColumn")
    for requirement in [
        "_rescued_data IS NULL",
        "format_version = 1",
        "target_committed = false",
        "expected_row_count = extracted_row_count",
        "raise_error('Invalid CRM Analytics monitoring event contract')",
    ]:
        assert requirement in expression
    assert not any(method in ("filter", "where") for method, _, _ in calls)


@pytest.mark.parametrize(
    "checkpoint",
    [VOLUME, VOLUME + "/checkpoint", "/Volumes/synthetic-catalog/crm_analytics/monitoring"],
)
def test_collector_rejects_overlapping_state_without_spark_actions(checkpoint):
    spark = Collector()
    with pytest.raises(CrmAnalyticsError, match="disjoint"):
        monitoring.collect_monitoring_events(
            spark,
            monitoring_volume_path=VOLUME,
            monitoring_table="main.crm_analytics.metrics",
            checkpoint_path=checkpoint,
        )
    assert not spark.calls


@pytest.mark.parametrize(
    "table", ["metrics", "main.metrics", "a.b.c.d", "a.b.c;DROP TABLE x", "`a`.b.c", None]
)
def test_collector_rejects_malformed_target_identifiers(table):
    spark = Collector()
    with pytest.raises(CrmAnalyticsError, match="three-part"):
        monitoring.collect_monitoring_events(
            spark,
            monitoring_volume_path=VOLUME,
            monitoring_table=table,
            checkpoint_path="/Volumes/a/b/c/checkpoint",
        )
    assert not spark.calls


def test_collector_propagates_failure_without_claiming_collection_success(monkeypatch):
    monkeypatch.setattr(monitoring, "expr", lambda expression: expression)
    spark = Collector(RuntimeError("synthetic-collector-failure"))
    with pytest.raises(RuntimeError, match="synthetic-collector-failure"):
        monitoring.collect_monitoring_events(
            spark,
            monitoring_volume_path=VOLUME,
            monitoring_table="a.b.metrics",
            checkpoint_path="/Volumes/a/b/c/checkpoint",
        )
