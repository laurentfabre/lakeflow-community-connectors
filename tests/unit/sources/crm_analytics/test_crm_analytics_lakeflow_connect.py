"""Offline, synthetic CRM Analytics source-contract tests; no live proof implied."""

import copy
import json
import os
import pickle
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

import pytest
import requests

from databricks.labs.community_connector.source_simulator import Simulator
from databricks.labs.community_connector.libs.spec_parser import SpecParser
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics import (
    CrmAnalyticsError,
    CrmAnalyticsLakeflowConnect,
    decode_column_name,
    encode_column_name,
)
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowBatchReader
from tests.unit.sources.test_suite import LakeflowConnectTests

DATASET = "0Fb000000000001"
VERSION = "0Fc000000000001"
NEXT_VERSION = "0Fc000000000002"
ROWS = f"rows_{DATASET}_{VERSION}"
SPEC_DIR = Path(__file__).resolve().parents[4] / (
    "src/databricks/labs/community_connector/source_simulator/specs/crm_analytics"
)
CONFIG = {
    "instance_url": "https://synthetic-fixture.salesforce.com",
    "access_token": "synthetic-test-token-never-valid",
    "api_version": "65.0",
    "page_size": "2",
    "max_retries": "0",
}
OPTIONS = {
    "source_primary_keys": '["Account.Id", "Region"]',
    "supported_scope": "scalar_full_visibility",
    "scope_fingerprint": "synthetic-full-visibility-fixture-v1",
}
LEGACY_OPTIONS = {**OPTIONS, "column_name_mode": "legacy_hex"}


class TestCrmAnalyticsConnector(LakeflowConnectTests):
    connector_class = CrmAnalyticsLakeflowConnect
    simulator_source = "crm_analytics"
    replay_config = CONFIG
    sample_records = 5

    @classmethod
    def _load_table_configs(cls):
        if os.environ.get("CONNECTOR_TEST_MODE", "simulate") in ("", "simulate", "replay"):
            # The committed synthetic corpus deliberately includes Name/name.
            # Preserve the legacy fixture and verify its existing target schema.
            return {DATASET: dict(LEGACY_OPTIONS)}
        # Live validation must supply real IDs and attestations, never fixture IDs.
        return super()._load_table_configs()


@pytest.fixture
def simulator():
    with Simulator(
        mode="simulate", spec_path=SPEC_DIR / "endpoints.yaml", corpus_dir=SPEC_DIR / "corpus"
    ) as sim:
        # Most tests exercise readable mode. Rename just the synthetic case-only
        # collision in memory, without changing the source-named golden corpus.
        for field in sim.corpus.tables["versions"][0]["xmdMain"]["dimensions"]:
            if field["field"] == "name":
                field["field"] = "lowercase_name"
        for row in sim.corpus.tables[ROWS]:
            row["lowercase_name"] = row.pop("name")
        yield sim


@pytest.fixture
def connector(simulator):
    del simulator
    source = CrmAnalyticsLakeflowConnect(CONFIG)
    yield source
    source.close()


@pytest.fixture
def wire(simulator, monkeypatch):
    """Observe/mutate real serialized simulator HTTP, never connector internals."""
    del simulator
    original = requests.Session.send
    calls = []
    state = {"transform": None}

    def send(session, prepared, **kwargs):
        calls.append(prepared)
        response = original(session, prepared, **kwargs)
        if state["transform"] is not None:
            return state["transform"](prepared, response)
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    return calls, state


def exhaust(connector, options=None):
    records, offset = connector.read_table(DATASET, None, options or OPTIONS)
    assert offset is None
    return list(records)


def response_body(response, value):
    response._content = json.dumps(value, ensure_ascii=False).encode("utf-8")
    return response


def query_text(prepared):
    return json.loads(prepared.body)["query"] if prepared.method == "POST" else ""


def replace_rows(simulator, count):
    """Build a deterministic stress scenario from the committed synthetic row."""
    seed = simulator.corpus.tables[ROWS][0]
    rows = []
    for index in range(count):
        row = dict(seed)
        row["Account.Id"] = f"account-{index:06}"
        row["Region"] = "fixture-region"
        rows.append(row)
    simulator.corpus.tables[ROWS] = rows
    simulator.corpus.tables["versions"][0]["totalRowCount"] = count
    return rows


@pytest.mark.parametrize("mode", ["readable", "legacy_hex"])
def test_full_iterator_and_framework_batch_reader_over_200_rows(simulator, wire, mode):
    replace_rows(simulator, 407)
    config = {**CONFIG, "page_size": "101"}
    options = {
        **config,
        **OPTIONS,
        "column_name_mode": mode,
        "tableName": DATASET,
        "version_id": VERSION,
        "databricks.connection": "synthetic_uc_connection",
        "access_token_expiration": "2026-10-30T17:00:00Z",
        "community_oauth_flow": "u2m",
        "client_id": "synthetic-client",
        "client_secret": "synthetic-client-secret",
        "refresh_token": "synthetic-refresh",
        "token_endpoint": "https://login.salesforce.com/services/oauth2/token",
        "authorization_endpoint": "https://login.salesforce.com/services/oauth2/authorize",
        "oauth_scope": "api refresh_token",
        "pkce_verifier": "synthetic-pkce",
        "authorization_code": "synthetic-code",
        "oauth_redirect_uri": "https://example.invalid",
        "oauth_provider": "salesforce",
        "oauth_credential_exchange_method": "body",
        "expires_in_secs": "3600",
        "source_name": "crm_analytics",
    }
    schema_connector = CrmAnalyticsLakeflowConnect(config)
    schema = schema_connector.get_table_schema(DATASET, options)
    reader_connector = CrmAnalyticsLakeflowConnect(config)
    reader = LakeflowBatchReader(options, schema, reader_connector)
    rows = list(reader.read(reader.partitions()[0]))
    assert len(rows) == 407
    assert isinstance(rows[0][encode_column_name("Amount", mode)], Decimal)
    evidence = reader_connector.get_read_evidence(DATASET, options)
    assert evidence["extracted_row_count"] == evidence["unique_key_count"] == 407
    assert evidence["terminal_empty_page"] and evidence["metadata_revalidated"]
    queries = [query_text(p) for p in wire[0] if p.method == "POST"]
    assert any("offset q 407;" in q for q in queries)
    assert sum("count()" in q for q in queries) == 2


def test_unchanged_version_is_full_snapshot_every_time(connector):
    first = exhaust(connector)
    assert first and exhaust(connector) == first
    assert connector.get_read_evidence(DATASET, OPTIONS)["version_id"] == VERSION


def test_new_instance_sees_refresh_explicit_pin_keeps_old_version(simulator, connector):
    old = connector.describe_snapshot(DATASET, OPTIONS)
    second = copy.deepcopy(simulator.corpus.tables["versions"][0])
    second.update(id=NEXT_VERSION, createdDate="2026-09-02T00:00:00Z")
    simulator.corpus.tables["versions"].append(second)
    simulator.corpus.tables[f"rows_{DATASET}_{NEXT_VERSION}"] = copy.deepcopy(
        simulator.corpus.tables[ROWS]
    )
    simulator.corpus.tables["datasets"][0]["currentVersionId"] = NEXT_VERSION
    fresh = CrmAnalyticsLakeflowConnect(CONFIG)
    assert fresh.describe_snapshot(DATASET, OPTIONS)["source_version"] == NEXT_VERSION
    pinned = {**OPTIONS, "version_id": old["source_version"]}
    assert (
        CrmAnalyticsLakeflowConnect(CONFIG).describe_snapshot(DATASET, pinned)["source_version"]
        == VERSION
    )
    assert exhaust(connector) == exhaust(CrmAnalyticsLakeflowConnect(CONFIG), pinned)


def test_expired_pinned_version_fails_without_fallback(simulator, connector):
    connector.pin_table(DATASET, OPTIONS)
    simulator.corpus.tables["versions"] = []
    with pytest.raises(CrmAnalyticsError, match="HTTP 404"):
        exhaust(connector)
    with pytest.raises(CrmAnalyticsError, match="not completed"):
        connector.get_read_evidence(DATASET, OPTIONS)


def test_evidence_requires_exhaustion_and_new_read_invalidates_it(connector):
    iterator, _ = connector.read_table(DATASET, None, OPTIONS)
    next(iterator)
    with pytest.raises(CrmAnalyticsError, match="not completed"):
        connector.get_read_evidence(DATASET, OPTIONS)
    list(iterator)
    assert connector.get_read_evidence(DATASET, OPTIONS)["terminal_empty_page"]
    connector.read_table(DATASET, None, OPTIONS)
    with pytest.raises(CrmAnalyticsError, match="not completed"):
        connector.get_read_evidence(DATASET, OPTIONS)


def test_stale_iterator_cannot_publish_over_new_read(connector):
    old, _ = connector.read_table(DATASET, None, OPTIONS)
    next(old)
    fresh, _ = connector.read_table(DATASET, None, OPTIONS)
    with pytest.raises(CrmAnalyticsError, match="Concurrent"):
        list(old)
    with pytest.raises(CrmAnalyticsError, match="not completed"):
        connector.get_read_evidence(DATASET, OPTIONS)
    list(fresh)
    assert connector.get_read_evidence(DATASET, OPTIONS)["metadata_revalidated"]


def test_superseded_unstarted_iterator_rejected(connector):
    old, _ = connector.read_table(DATASET, None, OPTIONS)
    connector.read_table(DATASET, None, OPTIONS)
    with pytest.raises(CrmAnalyticsError, match="Concurrent"):
        next(old)


@pytest.mark.parametrize("count", [1, 2, 3, 10])
def test_exact_and_partial_last_page(simulator, connector, count):
    replace_rows(simulator, count)
    assert len(exhaust(connector)) == count
    evidence = connector.get_read_evidence(DATASET, OPTIONS)
    assert evidence["visible_count_before"] == evidence["visible_count_after"] == count
    assert evidence["extracted_row_count"] == count and evidence["terminal_empty_page"]


@pytest.mark.parametrize(
    "mutation", ["count_before", "count_after", "short_page", "terminal_extra", "empty_count"]
)
def test_incomplete_snapshot_never_publishes(connector, wire, mutation):
    counts = 0

    def transform(prepared, response):
        nonlocal counts
        query = query_text(prepared)
        if not query:
            return response
        body = json.loads(response.text)
        if "count()" in query:
            counts += 1
            if mutation == "empty_count":
                body["results"]["records"] = []
            elif (mutation == "count_before" and counts == 1) or (
                mutation == "count_after" and counts == 2
            ):
                body["results"]["records"][0]["__row_count"] += 1
        elif mutation == "short_page" and "offset q 0;" in query:
            body["results"]["records"] = body["results"]["records"][:-1]
        elif mutation == "terminal_extra" and not body["results"]["records"]:
            body["results"]["records"] = [{"unexpected": "row"}]
        return response_body(response, body)

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError):
        exhaust(connector)
    with pytest.raises(CrmAnalyticsError, match="not completed"):
        connector.get_read_evidence(DATASET, OPTIONS)


@pytest.mark.parametrize("mutation", ["null", "duplicate", "multivalue"])
def test_invalid_composite_keys_rejected_globally(simulator, connector, mutation):
    rows = replace_rows(simulator, 8)
    if mutation == "null":
        rows[-1]["Region"] = None
    elif mutation == "duplicate":
        # Sorted duplicate spans the two-row page boundary (positions 1 and 2).
        rows[2]["Account.Id"] = rows[1]["Account.Id"]
    else:
        rows[-1]["Region"] = ["a", "b"]
    with pytest.raises(CrmAnalyticsError, match="Null or duplicate|Non-scalar"):
        exhaust(connector)


def test_composite_keys_allow_repeated_individual_component(simulator, connector):
    rows = replace_rows(simulator, 7)
    for index, row in enumerate(rows):
        row["Account.Id"] = "same-account"
        row["Region"] = f"region-{index}"
    assert len(exhaust(connector)) == 7


def test_readable_names_preserve_exact_source_metadata(connector):
    schema = connector.get_table_schema(DATASET, OPTIONS)
    mapping = {field.metadata["source_name"]: field.name for field in schema}
    assert mapping == {
        "Account.Id": "Account__Id",
        "Amount": "Amount",
        "CloseDate": "CloseDate",
        "CloseDate_Year": "CloseDate_Year",
        "CloseDate_epochSecond": "CloseDate_epochSecond",
        "Name": "Name",
        "Region": "Region",
        "lowercase_name": "lowercase_name",
        "État, rôle": "_u00c9_tat_u002c__u0020_r_u00f4_le",
    }
    assert len({name.lower() for name in schema.fieldNames()}) == len(schema.fields)
    assert connector.read_table_metadata(DATASET, OPTIONS)["primary_keys"] == [
        "Account__Id",
        "Region",
    ]
    rows = exhaust(connector)
    assert all(set(row) == set(schema.fieldNames()) for row in rows)


@pytest.mark.parametrize(
    "number",
    [
        "12345678901234567890.123456789012345678",
        "0.000000000000000001",
        "-12345678901234567890.123456789012345678",
        "12345678901234567890",
        "0E-100",
    ],
)
def test_exact_raw_json_measure_precision(connector, wire, number):
    def transform(prepared, response):
        if prepared.method == "POST" and "count()" not in query_text(prepared):
            body = json.loads(response.text)
            for row in body["results"]["records"]:
                row[encode_column_name("Amount")] = "__RAW_NUMBER__"
            response._content = json.dumps(body).replace('"__RAW_NUMBER__"', number).encode()
        return response

    wire[1]["transform"] = transform
    rows = exhaust(connector)
    assert all(Decimal(row[encode_column_name("Amount")]) == Decimal(number) for row in rows)
    reader = LakeflowBatchReader(
        {**CONFIG, **OPTIONS, "tableName": DATASET},
        connector.get_table_schema(DATASET, OPTIONS),
        connector,
    )
    parsed = list(reader.read(reader.partitions()[0]))
    assert all(row[encode_column_name("Amount")] == Decimal(number) for row in parsed)


@pytest.mark.parametrize("value", ["1e21", "1e-19", "NaN", "Infinity", "no-number", True, [], {}])
def test_invalid_or_lossy_measure_rejected(simulator, connector, value):
    simulator.corpus.tables[ROWS][0]["Amount"] = value
    with pytest.raises(CrmAnalyticsError, match="overflow|numeric"):
        exhaust(connector)


@pytest.mark.parametrize(
    "value,valid",
    [(-(2**63), True), (2**63 - 1, True), (-(2**63) - 1, False), (2**63, False), ("1.25", False)],
)
def test_epoch_long_boundaries(simulator, connector, value, valid):
    simulator.corpus.tables[ROWS][0]["CloseDate_epochSecond"] = value
    if valid:
        assert any(
            row[encode_column_name("CloseDate_epochSecond")] == value for row in exhaust(connector)
        )
    else:
        with pytest.raises(CrmAnalyticsError, match="overflows LongType"):
            exhaust(connector)


def test_discovery_over_200_preserves_all_ids(simulator, connector, wire):
    template = simulator.corpus.tables["datasets"][0]
    simulator.corpus.tables["datasets"] = [
        {**template, "id": f"0Fb{index:012}"} for index in range(407)
    ]
    assert len(connector.list_tables()) == 407
    assert len(wire[0]) == 3
    assert all("pageSize=200" in request.url for request in wire[0])


@pytest.mark.parametrize(
    "link",
    [
        "https://evil.example/services/data/v65.0/wave/datasets?page=x",
        "//evil.example/path",
        "/services/data/v65.0/wave/query?page=x",
        "/services/data/v65.0/wave/datasets?page=x&filter=foo",
        "/services/data/v65.0/wave/datasets?page=x&page=y",
        "/services/data/v65.0/wave/datasets?page=x#frag",
        "/services/data/v65.0/wave/datasets?page=x&pageSize=201",
        "/services/data/v65.0/wave/datasets?page=x&datasetTypes=Live",
        "https://user:password@synthetic-fixture.salesforce.com/services/data/v65.0/wave/datasets?page=x",
    ],
)
def test_unsafe_discovery_link_rejected_before_request(connector, wire, link):
    def transform(_prepared, response):
        body = json.loads(response.text)
        body["totalSize"] = 2
        body["nextPageUrl"] = link
        return response_body(response, body)

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError, match="Unsafe|discovery pageSize"):
        connector.list_tables()
    assert len(wire[0]) == 1


def test_discovery_cycle_rejected(connector, wire):
    def transform(_prepared, response):
        # Replace even the invalid-cursor response: this adversarial server
        # scenario intentionally violates the independent simulator's cursor.
        response.status_code = 200
        body = {
            "datasets": [
                {
                    "id": f"0Fb{len(wire[0]):012}",
                    "datasetType": "Default",
                    "currentVersionId": VERSION,
                }
            ],
            "totalSize": 10,
            "nextPageUrl": "/services/data/v65.0/wave/datasets?page=same",
        }
        return response_body(response, body)

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError, match="Cycle"):
        connector.list_tables()
    assert len(wire[0]) == 2


@pytest.mark.parametrize("status", [301, 302, 307, 401, 403, 404, 500, 503])
def test_http_failures_are_sanitized_and_do_not_redirect(connector, wire, status):
    def transform(_prepared, response):
        response.status_code = status
        response.headers["Location"] = "https://evil.example/secret"
        return response_body(response, {"secret": CONFIG["access_token"]})

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError) as error:
        connector.list_tables()
    assert CONFIG["access_token"] not in str(error.value)
    assert "evil.example" not in str(error.value)
    assert len(wire[0]) == 1


@pytest.mark.parametrize("retry_after", ["2", "999999", "malformed"])
def test_bounded_retry_after_policy(simulator, wire, monkeypatch, retry_after):
    del simulator
    sleeps = []
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.crm_analytics.crm_analytics.time.sleep",
        sleeps.append,
    )

    def transform(_prepared, response):
        if len(wire[0]) < 3:
            response.status_code = 429
            response.headers["Retry-After"] = retry_after
        return response

    wire[1]["transform"] = transform
    source = CrmAnalyticsLakeflowConnect(
        {**CONFIG, "max_retries": "2", "max_retry_delay_seconds": "3"}
    )
    if retry_after == "999999":
        with pytest.raises(CrmAnalyticsError, match="Retry-After exceeds"):
            source.list_tables()
        assert not sleeps and len(wire[0]) == 1
    else:
        assert source.list_tables() == [DATASET]
        assert len(sleeps) == 2 and all(0 <= delay <= 3 for delay in sleeps)
        if retry_after == "2":
            assert all(delay >= 2 for delay in sleeps)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"datasets": [], "datasets": []}',
        b'{"number": NaN}',
        b'{"number": Infinity}',
        b"not-json",
        b"[]",
    ],
)
def test_malformed_json_is_fail_closed(connector, wire, raw):
    def transform(_prepared, response):
        response._content = raw
        return response

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError, match="JSON"):
        connector.list_tables()


def test_query_warnings_fail_closed(connector, wire):
    def transform(prepared, response):
        if prepared.method == "POST":
            body = json.loads(response.text)
            body["warnings"] = [{"message": CONFIG["access_token"]}]
            return response_body(response, body)
        return response

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError, match="warnings") as error:
        exhaust(connector)
    assert CONFIG["access_token"] not in str(error.value)


@pytest.mark.parametrize("mutation", ["schema", "security", "created", "row_count", "folder"])
def test_metadata_drift_invalidates_completed_prefix(simulator, connector, mutation):
    iterator, _ = connector.read_table(DATASET, None, OPTIONS)
    next(iterator)
    version = simulator.corpus.tables["versions"][0]
    if mutation == "schema":
        version["xmdMain"]["dimensions"].append({"field": "new_field"})
    elif mutation == "security":
        version["predicate"] = "Region == restricted"
    elif mutation == "created":
        version["createdDate"] = "2026-09-03T00:00:00Z"
    elif mutation == "folder":
        simulator.corpus.tables["datasets"][0]["folder"]["id"] = "other-folder"
    else:
        version["totalRowCount"] += 1
    with pytest.raises(CrmAnalyticsError, match="metadata changed"):
        list(iterator)
    with pytest.raises(CrmAnalyticsError, match="not completed"):
        connector.get_read_evidence(DATASET, OPTIONS)


@pytest.mark.parametrize(
    "options",
    [
        {"max_records_per_batch": "1"},
        {"filter": "Region=US"},
        {"source_primary_keys": "[]"},
        {"source_primary_keys": '["Amount"]'},
        {"primary_keys": '["Account.Id", "Region"]'},
        {"supported_scope": "partial"},
        {"scope_fingerprint": ""},
        {"decimal_scale": "39"},
        {"max_keys": "1"},
    ],
)
def test_unsafe_table_options_fail_instead_of_truncating(connector, options):
    with pytest.raises(CrmAnalyticsError):
        exhaust(connector, {**OPTIONS, **options})


def test_matching_destination_key_override_is_optional_and_equivalent(connector):
    options = {
        **OPTIONS,
        "primary_keys": json.dumps(
            [encode_column_name("Account.Id"), encode_column_name("Region")]
        ),
    }
    assert connector.pin_table(DATASET, options) == connector.pin_table(DATASET, OPTIONS)


def test_common_pipeline_override_bypasses_source_validation_boundary(connector):
    """Document, NOT endorse, the shared pipeline's unchecked destination override.

    The source validates an override only if it actually receives that option.
    Shared SpecParser removes it, then ingestion_pipeline prefers its separate
    value. Operators must omit it or provide the exact encoded names. This test
    must never be cited as proof that source validation protects that path.
    """
    raw_keys = ["Account.Id", "Region"]
    spec = SpecParser(
        {
            "connection_name": "synthetic-connection",
            "objects": [
                {
                    "table": {
                        "source_table": DATASET,
                        "table_configuration": {**OPTIONS, "primary_keys": raw_keys},
                    }
                }
            ],
        }
    )
    forwarded = spec.get_table_configuration(DATASET)
    assert "primary_keys" not in forwarded
    metadata = connector.read_table_metadata(DATASET, forwarded)
    assert metadata["primary_keys"] == [encode_column_name(key) for key in raw_keys]
    assert spec.get_primary_keys(DATASET) == raw_keys
    assert spec.get_primary_keys(DATASET) != metadata["primary_keys"]


@pytest.mark.parametrize(
    "origin",
    [
        "http://example.salesforce.com",
        "https://salesforce.com.evil.example",
        "https://user:pass@example.salesforce.com",
        "https://example.salesforce.com/path",
        "https://example.salesforce.com:8443",
        "https://example.salesforce.com?secret=yes",
    ],
)
def test_untrusted_origin_rejected(origin):
    with pytest.raises(CrmAnalyticsError, match="trusted HTTPS"):
        CrmAnalyticsLakeflowConnect({**CONFIG, "instance_url": origin})


def test_incremental_offset_rejected(connector):
    with pytest.raises(CrmAnalyticsError, match="incremental offsets"):
        connector.read_table(DATASET, {"offset": 2}, OPTIONS)


@pytest.mark.parametrize("exception", [requests.Timeout, requests.ConnectionError])
def test_transport_retries_have_explicit_timeouts_and_sanitized_exhaustion(
    simulator, monkeypatch, exception
):
    del simulator
    attempts = []
    sleeps = []

    def request(session, _method, _url, **kwargs):
        assert not session.trust_env
        assert kwargs["allow_redirects"] is False
        assert kwargs["timeout"] == (30, 30)
        attempts.append(kwargs)
        raise exception(f"sensitive source exception: {CONFIG['access_token']}")

    monkeypatch.setattr(requests.Session, "request", request)
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.crm_analytics.crm_analytics.time.sleep",
        sleeps.append,
    )
    source = CrmAnalyticsLakeflowConnect({**CONFIG, "max_retries": "2"})
    with pytest.raises(CrmAnalyticsError, match="bounded retries") as error:
        source.list_tables()
    assert len(attempts) == 3 and len(sleeps) == 2
    assert CONFIG["access_token"] not in str(error.value)


def test_successful_http_uses_oauth_without_ambient_auth(connector, simulator, monkeypatch):
    del simulator
    original = requests.Session.request
    calls = []

    def request(session, method, url, **kwargs):
        calls.append(kwargs)
        assert not session.trust_env
        assert kwargs["allow_redirects"] is False
        assert kwargs["timeout"] == (30, 30)
        assert kwargs["headers"]["Authorization"] == f"Bearer {CONFIG['access_token']}"
        assert "synthetic-test-token" not in url
        return original(session, method, url, **kwargs)

    monkeypatch.setattr(requests.Session, "request", request)
    assert exhaust(connector)
    assert calls


def test_retry_after_http_date_above_bound_aborts(connector, wire, monkeypatch):
    source = CrmAnalyticsLakeflowConnect(
        {**CONFIG, "max_retries": "2", "max_retry_delay_seconds": "1"}
    )
    sleeps = []
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.crm_analytics.crm_analytics.time.sleep",
        sleeps.append,
    )

    def transform(_prepared, response):
        response.status_code = 503
        response.headers["Retry-After"] = format_datetime(
            datetime.now(timezone.utc) + timedelta(hours=1)
        )
        return response

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError, match="Retry-After exceeds"):
        source.list_tables()
    assert len(wire[0]) == 1 and not sleeps


@pytest.mark.parametrize(
    "mutation", ["duplicate", "count_changes", "missing_last", "empty_progress"]
)
def test_discovery_integrity_failure(connector, wire, mutation):
    def transform(_prepared, response):
        body = {
            "datasets": [{"id": DATASET, "datasetType": "Default", "currentVersionId": VERSION}],
            "totalSize": 2,
            "nextPageUrl": None,
        }
        response.status_code = 200
        if mutation in {"duplicate", "count_changes"}:
            if len(wire[0]) == 1:
                body["nextPageUrl"] = "/services/data/v65.0/wave/datasets?page=next"
            elif mutation == "count_changes":
                body["totalSize"] = 3
        elif mutation == "empty_progress":
            body["datasets"] = []
            body["nextPageUrl"] = "/services/data/v65.0/wave/datasets?page=next"
        return response_body(response, body)

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError, match="Duplicate|changed|Incomplete|Non-progressing"):
        connector.list_tables()


@pytest.mark.parametrize("mutation", ["extra", "missing", "wrong_records", "wrong_results"])
def test_projection_and_wrapper_must_match_exactly(connector, wire, mutation):
    def transform(prepared, response):
        if prepared.method != "POST" or "count()" in query_text(prepared):
            return response
        body = json.loads(response.text)
        if mutation == "extra":
            body["results"]["records"][0]["unexpected"] = "extra"
        elif mutation == "missing":
            body["results"]["records"][0].pop(encode_column_name("Amount"))
        elif mutation == "wrong_records":
            body["results"]["records"] = {}
        else:
            body["results"] = []
        return response_body(response, body)

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError, match="projection|Malformed SAQL"):
        exhaust(connector)


@pytest.mark.parametrize(
    "mutation",
    [
        "derived",
        "unsafe_name",
        "missing_xmd",
        "bad_date",
        "conflicting_type",
        "invalid_created",
        "wrong_identity",
    ],
)
def test_unsupported_xmd_or_version_rejected_before_rows(simulator, connector, wire, mutation):
    version = simulator.corpus.tables["versions"][0]
    if mutation == "derived":
        version["xmdMain"]["derivedMeasures"] = [{"field": "calculated"}]
    elif mutation == "unsafe_name":
        version["xmdMain"]["dimensions"].append({"field": "unsafe'quote"})
    elif mutation == "missing_xmd":
        version.pop("xmdMain")
    elif mutation == "bad_date":
        version["xmdMain"]["dates"][0]["fields"]["unknownPart"] = "date"
    elif mutation == "conflicting_type":
        version["xmdMain"]["measures"].append({"field": "Account.Id"})
    elif mutation == "invalid_created":
        version["createdDate"] = "2026-09-01T00:00:00"
    else:

        def transform(prepared, response):
            if "/versions/" in prepared.url:
                body = json.loads(response.text)
                body["dataset"]["id"] = "0Fb000000000002"
                return response_body(response, body)
            return response

        wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError):
        exhaust(connector)
    assert not any(p.method == "POST" for p in wire[0])


@pytest.mark.parametrize(
    "source,target",
    [
        ("Account.Id", "Account__Id"),
        ("plain_123", "plain_123"),
        ("c_4162", "c_4162"),
        ("État, rôle", "_u00c9_tat_u002c__u0020_r_u00f4_le"),
        ("é", "_u00e9_"),
        ("😀", "_u1f600_"),
        ("1st", "c_1st"),
        ("none", "c_none"),
        ("NONE", "c_NONE"),
        ("A B", "A_u0020_B"),
        ("A-B", "A_u002d_B"),
    ],
)
def test_readable_names_have_independent_literal_contract(source, target):
    assert encode_column_name(source) == target
    assert encode_column_name(source, "readable") == target


def test_readable_names_cannot_be_misdecoded_as_legacy_hex(simulator, connector):
    simulator.corpus.tables["versions"][0]["xmdMain"]["dimensions"].append({"field": "c_4162"})
    for row in simulator.corpus.tables[ROWS]:
        row["c_4162"] = "literal-source-value"
    schema = connector.get_table_schema(DATASET, OPTIONS)
    assert schema["c_4162"].metadata == {"source_name": "c_4162"}
    assert all(row["c_4162"] == "literal-source-value" for row in exhaust(connector))
    with pytest.raises(ValueError, match="source_name field metadata"):
        decode_column_name("c_4162")
    assert decode_column_name("c_4162", mode="legacy_hex") == "Ab"


@pytest.mark.parametrize(
    "first,second",
    [
        ("Collision", "collision"),
        ("a.b", "a__b"),
        ("é", "_u00e9_"),
        ("1st", "c_1st"),
        ("none", "c_none"),
    ],
)
def test_readable_whole_schema_collisions_fail_before_query(
    simulator, connector, wire, first, second
):
    dimensions = simulator.corpus.tables["versions"][0]["xmdMain"]["dimensions"]
    dimensions.extend([{"field": first}, {"field": second}])
    with pytest.raises(CrmAnalyticsError, match="Case-insensitive destination column collision"):
        exhaust(connector)
    assert not any(request.method == "POST" for request in wire[0])


def test_original_case_collision_keeps_legacy_schema_and_values(simulator, connector):
    for field in simulator.corpus.tables["versions"][0]["xmdMain"]["dimensions"]:
        if field["field"] == "lowercase_name":
            field["field"] = "name"
    for row in simulator.corpus.tables[ROWS]:
        row["name"] = row.pop("lowercase_name")
    with pytest.raises(CrmAnalyticsError, match="column collision"):
        connector.get_table_schema(DATASET, OPTIONS)
    schema = connector.get_table_schema(DATASET, LEGACY_OPTIONS)
    mapping = {field.metadata["source_name"]: field.name for field in schema}
    assert mapping["Name"] == "c_4e616d65"
    assert mapping["name"] == "c_6e616d65"
    assert mapping["Account.Id"] == "c_4163636f756e742e4964"
    assert mapping["Region"] == "c_526567696f6e"
    assert {decode_column_name(name, mode="legacy_hex") for name in schema.fieldNames()} == set(
        simulator.corpus.tables[ROWS][0]
    )
    actual = [
        {field.metadata["source_name"]: row[field.name] for field in schema}
        for row in exhaust(connector, LEGACY_OPTIONS)
    ]
    # Source preserves exact decimal text; LakeflowBatchReader parses Decimal.
    assert actual == simulator.corpus.tables[ROWS]
    assert connector.read_table_metadata(DATASET, LEGACY_OPTIONS)["primary_keys"] == [
        "c_4163636f756e742e4964",
        "c_526567696f6e",
    ]


def test_naming_mode_is_explicit_in_descriptor_and_changes_fingerprints(connector):
    readable = connector.pin_table(DATASET, OPTIONS)
    explicit = connector.pin_table(DATASET, {**OPTIONS, "column_name_mode": "readable"})
    legacy = connector.pin_table(DATASET, LEGACY_OPTIONS)
    assert readable == explicit
    assert readable["column_name_mode"] == "readable"
    assert legacy["column_name_mode"] == "legacy_hex"
    assert readable["column_mapping"]["Account.Id"] == "Account__Id"
    assert legacy["column_mapping"]["Account.Id"] == "c_4163636f756e742e4964"
    assert readable["configuration_fingerprint"] != legacy["configuration_fingerprint"]
    assert readable["schema_fingerprint"] != legacy["schema_fingerprint"]


@pytest.mark.parametrize("mode", ["hex", "READABLE", "", None, True])
def test_unknown_naming_mode_rejected(connector, mode):
    with pytest.raises(CrmAnalyticsError, match="column_name_mode"):
        connector.get_table_schema(DATASET, {**OPTIONS, "column_name_mode": mode})


def test_legacy_key_override_must_match_selected_mode(connector):
    keys = '["c_4163636f756e742e4964", "c_526567696f6e"]'
    assert connector.pin_table(DATASET, {**LEGACY_OPTIONS, "primary_keys": keys}) == (
        connector.pin_table(DATASET, LEGACY_OPTIONS)
    )
    with pytest.raises(CrmAnalyticsError, match="primary_keys"):
        connector.pin_table(DATASET, {**OPTIONS, "primary_keys": keys})


def test_corpus_bootstrap_reverses_metadata_and_preserves_golden_source_rows(tmp_path):
    # Bootstrap in temporary storage: never rewrite the committed source fixture
    # merely to make a changed naming implementation agree with itself.
    from tests.unit.sources.crm_analytics.bootstrap_corpus import main

    main(tmp_path)
    for name in ("datasets.json", "versions.json", f"{ROWS}.json"):
        assert (tmp_path / name).read_bytes() == (SPEC_DIR / "corpus" / name).read_bytes()


@pytest.mark.parametrize("shape", ["explicit_zero", "empty_aggregate"])
def test_provisional_empty_count_shapes_are_not_live_support_evidence(
    simulator, connector, wire, shape
):
    """These synthetic alternatives describe strict handling, not Salesforce fidelity."""
    replace_rows(simulator, 0)

    def transform(prepared, response):
        if "count()" in query_text(prepared):
            body = json.loads(response.text)
            body["results"]["records"] = [{"__row_count": 0}] if shape == "explicit_zero" else []
            return response_body(response, body)
        return response

    wire[1]["transform"] = transform
    if shape == "explicit_zero":
        assert exhaust(connector) == []
        evidence = connector.get_read_evidence(DATASET, OPTIONS)
        assert evidence["extracted_row_count"] == evidence["unique_key_count"] == 0
        assert evidence["terminal_empty_page"] and evidence["metadata_revalidated"]
    else:
        with pytest.raises(CrmAnalyticsError, match="count response"):
            exhaust(connector)
        with pytest.raises(CrmAnalyticsError, match="not completed"):
            connector.get_read_evidence(DATASET, OPTIONS)


@pytest.mark.parametrize("part", ["epochDay", "epochSecond"])
def test_provisional_main_xmd_epoch_measure_overlap_fails_conservatively(
    simulator, connector, wire, part
):
    """No live main-XMD overlap is established; do not guess numeric precedence."""
    xmd = simulator.corpus.tables["versions"][0]["xmdMain"]
    field = f"Date_{part}"
    xmd["dates"][0]["fields"][part] = field
    xmd["measures"].append({"field": field})
    with pytest.raises(CrmAnalyticsError, match="Conflicting physical column types"):
        exhaust(connector)
    assert not any(request.method == "POST" for request in wire[0])


def test_case_and_unicode_keys_across_pages_need_no_python_order_match(simulator, connector, wire):
    """Model another stable total source order, not an asserted SAQL collation."""
    # Case-distinct IDs occupy positions 1/2; composed/decomposed Unicode is
    # at positions 3/4. Both pairs therefore straddle the two-record pages.
    keys = ["prefix", "001A00000000001", "001a00000000001", "é", "e\u0301", "東京", "😀"]
    rows = replace_rows(simulator, len(keys))
    for row, key in zip(rows, keys):
        row["Account.Id"] = key
    source_by_key = {row["Account.Id"]: row for row in rows}

    def transform(prepared, response):
        query = query_text(prepared)
        if not query or "count()" in query:
            return response
        start = int(query.split("offset q ")[1].split(";")[0])
        body = json.loads(response.text)
        original_rows = body["results"]["records"]
        # Replace key/value pairs as a unit. The explicit fixture order differs
        # from Python's; connector validation must depend on identity, not order.
        for output, key in zip(original_rows, keys[start : start + 2]):
            for field in connector.get_table_schema(DATASET, OPTIONS):
                output[field.name] = source_by_key[key][field.metadata["source_name"]]
        return response_body(response, body)

    wire[1]["transform"] = transform
    result = exhaust(connector)
    assert [row["Account__Id"] for row in result] == keys
    assert len({(row["Account__Id"], row["Region"]) for row in result}) == len(keys)


def test_cross_page_repeated_record_cannot_publish_even_when_page_counts_match(connector, wire):
    first_page = []

    def transform(prepared, response):
        query = query_text(prepared)
        if "offset q 0;" in query:
            first_page.extend(json.loads(response.text)["results"]["records"])
        elif "offset q 2;" in query:
            body = json.loads(response.text)
            body["results"]["records"][0] = first_page[-1]
            return response_body(response, body)
        return response

    wire[1]["transform"] = transform
    with pytest.raises(CrmAnalyticsError, match="Null or duplicate"):
        exhaust(connector)
    with pytest.raises(CrmAnalyticsError, match="not completed"):
        connector.get_read_evidence(DATASET, OPTIONS)


def test_session_is_lazy_reused_across_pages_and_distinct_per_instance(connector, monkeypatch):
    created = []
    original = requests.Session.__init__

    def init(session, *args, **kwargs):
        created.append(session)
        original(session, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "__init__", init)
    assert created == []
    exhaust(connector)
    exhaust(connector)
    assert len(created) == 1
    first_session = created[0]
    assert first_session.trust_env is False
    other = CrmAnalyticsLakeflowConnect(CONFIG)
    assert len(created) == 1
    try:
        assert other.list_tables() == [DATASET]
        assert len(created) == 2 and created[1] is not first_session
    finally:
        other.close()


def test_close_releases_session_but_keeps_pin_and_lazily_reopens(connector, monkeypatch):
    closed = []
    original = requests.Session.close

    def close(session):
        closed.append(session)
        original(session)

    monkeypatch.setattr(requests.Session, "close", close)
    pin = connector.pin_table(DATASET, OPTIONS)
    original_session = connector._session
    original_session.cookies.set("stale", "do-not-forward")
    connector.close()
    connector.close()
    assert closed == [original_session]
    assert not original_session.cookies
    assert connector._session is None
    assert connector.pin_table(DATASET, OPTIONS) == pin
    assert connector._session is None  # Cached metadata does not reopen sockets.
    assert exhaust(connector)
    assert connector._session is not original_session
    assert closed == [original_session]


def test_pickle_drops_http_state_without_closing_original(connector, monkeypatch):
    pin = connector.pin_table(DATASET, OPTIONS)
    original_session = connector._session
    original_session.cookies.set("synthetic_cookie", "never-serialize-this-cookie")
    closed = []
    original_close = requests.Session.close

    def close(session):
        closed.append(session)
        original_close(session)

    monkeypatch.setattr(requests.Session, "close", close)
    serialized = pickle.dumps(connector)
    restored = pickle.loads(serialized)
    try:
        assert b"never-serialize-this-cookie" not in serialized
        assert closed == []
        assert connector._session is original_session
        assert restored._session is None
        assert restored.pin_table(DATASET, OPTIONS) == pin
        assert exhaust(restored)
        assert restored._session is not original_session
        assert exhaust(connector)
        assert connector._session is original_session
    finally:
        restored.close()


@pytest.mark.parametrize("scenario", ["success", "retry", "http_error", "bad_json", "warning"])
def test_every_response_is_closed_and_cookie_auth_never_crosses_requests(
    simulator, monkeypatch, scenario
):
    del simulator
    sessions, responses, closed = [], [], []
    original_send = requests.Session.send
    original_close = requests.Response.close

    def close(response):
        closed.append(response)
        original_close(response)

    def send(session, prepared, **kwargs):
        sessions.append(session)
        assert "Cookie" not in prepared.headers
        assert session.trust_env is False
        response = original_send(session, prepared, **kwargs)
        responses.append(response)
        session.cookies.set("source-cookie", "never-forward")
        if scenario == "retry" and len(responses) == 1:
            response.status_code = 503
        elif scenario == "http_error":
            response.status_code = 403
        elif scenario == "bad_json":
            response._content = b"not-json"
        elif scenario == "warning":
            response_body(response, {"warnings": ["synthetic-sensitive-row"]})
        return response

    def sleep(_delay):
        assert closed == responses  # Release connection before bounded backoff.
        assert not sessions[-1].cookies

    monkeypatch.setattr(requests.Session, "send", send)
    monkeypatch.setattr(requests.Response, "close", close)
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.crm_analytics.crm_analytics.time.sleep",
        sleep,
    )
    source = CrmAnalyticsLakeflowConnect({**CONFIG, "max_retries": "1"})
    try:
        if scenario in {"http_error", "bad_json", "warning"}:
            with pytest.raises(CrmAnalyticsError):
                source.list_tables()
        else:
            assert exhaust(source)
            assert len(responses) > 4
        assert closed == responses
        assert len({id(session) for session in sessions}) == 1
        assert all(not session.cookies for session in sessions)
    finally:
        source.close()


@pytest.mark.parametrize(
    "exception", [requests.Timeout, requests.ConnectionError, requests.RequestException]
)
def test_cookies_are_cleared_after_transport_failure(simulator, monkeypatch, exception):
    del simulator
    sessions = []

    def request(session, _method, _url, **_kwargs):
        assert not session.cookies
        sessions.append(session)
        session.cookies.set("error-cookie", "never-forward")
        raise exception("synthetic-private-error")

    monkeypatch.setattr(requests.Session, "request", request)
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.crm_analytics.crm_analytics.time.sleep",
        lambda _delay: None,
    )
    source = CrmAnalyticsLakeflowConnect({**CONFIG, "max_retries": "1"})
    try:
        with pytest.raises(CrmAnalyticsError) as error:
            source.list_tables()
        assert "synthetic-private-error" not in str(error.value)
        assert len(sessions) == (1 if exception is requests.RequestException else 2)
        assert len({id(session) for session in sessions}) == 1
        assert all(not session.cookies for session in sessions)
    finally:
        source.close()
