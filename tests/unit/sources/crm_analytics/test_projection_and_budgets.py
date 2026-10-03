"""Independent projection/admission contracts, using synthetic HTTP only.

Reuse the existing simulator fixtures without altering the golden corpus. Expected
columns and query counts below are literal contracts, not production helpers.
"""

# Import fixtures, not the inherited generic test class (which would duplicate it).
# pylint: disable=redefined-outer-name,unused-import

import copy
import json
import pickle

import pytest
import requests

from databricks.labs.community_connector.sources.crm_analytics import crm_analytics as crm
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowBatchReader
from tests.unit.sources.crm_analytics.test_crm_analytics_lakeflow_connect import (
    CONFIG,
    DATASET,
    OPTIONS,
    ROWS,
    VERSION,
    CrmAnalyticsError,
    CrmAnalyticsLakeflowConnect,
    connector,
    exhaust,
    query_text,
    replace_rows,
    simulator,
    wire,
)

SELECTED = {**OPTIONS, "selected_columns": '["Region", "Amount", "Account.Id"]'}
KEYS_ONLY = {**OPTIONS, "selected_columns": '["Region", "Account.Id"]'}


class Clock:
    """A deterministic cooperative clock: tests never actually wait."""

    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def advance(self, seconds):
        self.now += seconds

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance(seconds)


@pytest.fixture
def clock(monkeypatch):
    result = Clock()
    # Both production modules use this standard-library time object.
    monkeypatch.setattr(crm.time, "monotonic", lambda: result.now)
    monkeypatch.setattr(crm.time, "sleep", result.sleep)
    monkeypatch.setattr(crm.random, "random", lambda: 0.0)
    return result


def query_calls(wire):
    return [prepared for prepared in wire[0] if prepared.method == "POST"]


def assert_incomplete(source, options=SELECTED, name=DATASET):
    with pytest.raises(CrmAnalyticsError, match="not completed"):
        source.get_read_evidence(name, options)


def test_explicit_projection_selects_columns_not_rows(connector, wire):
    descriptor = connector.pin_table(DATASET, SELECTED)
    assert descriptor["selected_columns"] == ["Account.Id", "Amount", "Region"]
    assert descriptor["projection_mode"] == "explicit"
    assert descriptor["column_mapping"] == {
        "Account.Id": "Account__Id",
        "Amount": "Amount",
        "Region": "Region",
    }
    schema = connector.get_table_schema(DATASET, SELECTED)
    assert schema.fieldNames() == ["Account__Id", "Amount", "Region"]
    assert [field.metadata for field in schema] == [
        {"source_name": "Account.Id"},
        {"source_name": "Amount"},
        {"source_name": "Region"},
    ]
    rows = exhaust(connector, SELECTED)
    assert len(rows) == 7
    assert all(set(row) == {"Account__Id", "Amount", "Region"} for row in rows)
    assert len({(row["Account__Id"], row["Region"]) for row in rows}) == 7
    queries = [query_text(prepared) for prepared in query_calls(wire)]
    assert len(queries) == 7  # 4 pages, 1 empty probe, 2 counts.
    projection = (
        "q = foreach q generate 'Account.Id' as 'Account__Id', "
        "'Amount' as 'Amount', 'Region' as 'Region';"
    )
    for query in queries:
        if "count()" not in query:
            assert projection in query
            assert "'Name'" not in query and "'CloseDate'" not in query
            assert "q = order q by ('Account__Id' asc, 'Region' asc);" in query


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        "Account.Id",
        "[]",
        [],
        ["Account.Id"],
        ["Account.Id", "Region", "Region"],
        ["Account.Id", "Region", 1],
        ["Account.Id", "Region", "unknown"],
        ["Account.Id", "Region", "amount"],
        ["Account.Id", "Region", ""],
        ["Account.Id", "Region", "unsafe'quote"],
        ["Account.Id", "Region", "unsafe\\slash"],
        ["Account.Id", "Region", "unsafe;query"],
        ["Account.Id", "Region", "unsafe\nname"],
        ["Account.Id", "Region", "\ud800"],
        ["Account.Id", "Region", "x" * 1025],
    ],
)
def test_invalid_selection_fails_safely_before_query(connector, wire, value):
    options = {
        **OPTIONS,
        "selected_columns": json.dumps(value) if isinstance(value, list) else value,
    }
    with pytest.raises(CrmAnalyticsError) as error:
        exhaust(connector, options)
    assert not query_calls(wire)
    assert CONFIG["access_token"] not in str(error.value)


def test_selection_order_is_canonical_but_selection_mode_and_set_are_identity(connector):
    selected = connector.pin_table(DATASET, SELECTED)
    reordered = connector.pin_table(
        DATASET, {**SELECTED, "selected_columns": ["Amount", "Region", "Account.Id"]}
    )
    assert selected == reordered
    subset = connector.pin_table(DATASET, KEYS_ONLY)
    assert subset["configuration_fingerprint"] != selected["configuration_fingerprint"]
    all_columns = connector.pin_table(DATASET, OPTIONS)
    explicit_all = connector.pin_table(
        DATASET, {**OPTIONS, "selected_columns": list(all_columns["column_mapping"])}
    )
    assert explicit_all["schema_fingerprint"] == all_columns["schema_fingerprint"]
    assert explicit_all["configuration_fingerprint"] != all_columns["configuration_fingerprint"]
    assert all_columns["projection_mode"] == "all_physical"
    assert all_columns["selected_columns"] is None


def mutate_unselected(simulator, mutation):
    xmd = simulator.corpus.tables["versions"][0]["xmdMain"]
    if mutation == "multi_value":
        for row in simulator.corpus.tables[ROWS]:
            row["Name"] = ["red", "blue"]
    elif mutation == "derived":
        xmd["derivedDimensions"] = [{"field": "calculated"}]
    elif mutation == "unsafe":
        xmd["dimensions"].append({"field": "unsafe'quote"})
    elif mutation == "collision":
        xmd["dimensions"].append({"field": "Account__Id"})
    elif mutation == "conflict":
        xmd["dimensions"].append({"field": "Amount"})
    elif mutation == "unknown_date_part":
        xmd["dates"][0]["fields"]["unsupportedPart"] = "unknown_date_field"
    elif mutation == "unknown_date_type":
        xmd["dates"][0]["type"] = "UnprovenDateType"
    elif mutation == "remove":
        xmd["measures"] = []
    else:
        xmd["dimensions"].append({"field": "new_unselected_field"})


@pytest.mark.parametrize(
    "mutation",
    [
        "multi_value",
        "derived",
        "unsafe",
        "collision",
        "conflict",
        "unknown_date_part",
        "unknown_date_type",
    ],
)
def test_explicit_projection_excludes_unsupported_unselected_fields(
    simulator, connector, wire, mutation
):
    mutate_unselected(simulator, mutation)
    assert len(exhaust(connector, KEYS_ONLY)) == 7
    assert connector.get_read_evidence(DATASET, KEYS_ONLY)["extracted_row_count"] == 7
    assert all("'Name'" not in query_text(call) for call in query_calls(wire))
    # Omission still means strict all-physical mode, never automatic dropping.
    with pytest.raises(CrmAnalyticsError):
        exhaust(CrmAnalyticsLakeflowConnect(CONFIG), OPTIONS)


@pytest.mark.parametrize(
    "mutation", ["derived", "conflict", "unknown_date_part", "unknown_date_type", "remove", "add"]
)
@pytest.mark.parametrize("during_read", [False, True])
def test_unselected_metadata_change_keeps_projected_identity(
    simulator, connector, mutation, during_read
):
    descriptor = connector.pin_table(DATASET, KEYS_ONLY)
    if during_read:
        iterator, _ = connector.read_table(DATASET, None, KEYS_ONLY)
        first = next(iterator)
        mutate_unselected(simulator, mutation)
        assert len([first, *iterator]) == 7
    else:
        mutate_unselected(simulator, mutation)
        assert len(exhaust(connector, KEYS_ONLY)) == 7
    fresh = CrmAnalyticsLakeflowConnect(CONFIG).pin_table(DATASET, KEYS_ONLY)
    assert fresh["schema_fingerprint"] == descriptor["schema_fingerprint"]
    assert fresh["configuration_fingerprint"] == descriptor["configuration_fingerprint"]
    assert connector.get_read_evidence(DATASET, KEYS_ONLY)["metadata_revalidated"]


@pytest.mark.parametrize(
    "mutation",
    [
        "selected_multivalue",
        "selected_conflict",
        "selected_derived",
        "selected_date_part",
        "selected_date_type",
    ],
)
def test_projection_does_not_make_selected_unsupported_fields_safe(simulator, connector, mutation):
    xmd = simulator.corpus.tables["versions"][0]["xmdMain"]
    options = dict(SELECTED)
    if mutation == "selected_multivalue":
        simulator.corpus.tables[ROWS][0]["Region"] = ["not", "scalar"]
    elif mutation == "selected_conflict":
        xmd["dimensions"].append({"field": "Amount"})
    elif mutation == "selected_derived":
        xmd["derivedMeasures"] = [{"field": "calculated"}]
        options["selected_columns"] = ["Account.Id", "Region", "calculated"]
    elif mutation == "selected_date_part":
        xmd["dates"][0]["fields"]["unsupportedPart"] = "unknown_date_field"
        options["selected_columns"] = ["Account.Id", "Region", "unknown_date_field"]
    else:
        xmd["dates"][0]["type"] = "UnprovenDateType"
        options["selected_columns"] = ["Account.Id", "Region", "CloseDate"]
    with pytest.raises(CrmAnalyticsError):
        exhaust(connector, options)


@pytest.mark.parametrize(
    "mutation",
    [
        "group",
        "entry",
        "missing_name",
        "empty_name",
        "numeric_name",
        "derived_group",
        "derived_entry",
        "derived_name",
        "date_group",
        "date_entry",
        "date_type",
        "date_fields",
        "date_part",
        "date_name",
    ],
)
def test_selection_cannot_hide_malformed_unselected_metadata(simulator, connector, wire, mutation):
    xmd = simulator.corpus.tables["versions"][0]["xmdMain"]
    if mutation == "group":
        xmd["measures"] = {}
    elif mutation == "entry":
        xmd["measures"].append("not-an-entry")
    elif mutation in ("missing_name", "empty_name", "numeric_name"):
        xmd["measures"].append(
            {} if mutation == "missing_name" else {"field": "" if mutation == "empty_name" else 9}
        )
    elif mutation == "derived_group":
        xmd["derivedDimensions"] = "not-an-array"
    elif mutation == "derived_entry":
        xmd["derivedDimensions"] = [None]
    elif mutation == "derived_name":
        xmd["derivedDimensions"] = [{}]
    elif mutation == "date_group":
        xmd["dates"] = {}
    elif mutation == "date_entry":
        xmd["dates"] = [None]
    elif mutation == "date_type":
        xmd["dates"][0]["type"] = 7
    elif mutation == "date_fields":
        xmd["dates"][0]["fields"] = []
    elif mutation == "date_part":
        xmd["dates"][0]["fields"][""] = "unselected"
    else:
        xmd["dates"][0]["fields"]["fullField"] = 7
    with pytest.raises(CrmAnalyticsError, match="Malformed"):
        exhaust(connector, KEYS_ONLY)
    assert not query_calls(wire)


@pytest.mark.parametrize(
    "mutation", ["selected_schema", "security", "visible_count", "null_key", "duplicate_key"]
)
def test_projection_preserves_snapshot_integrity_guards(simulator, connector, mutation):
    iterator, _ = connector.read_table(DATASET, None, SELECTED)
    next(iterator)
    if mutation == "selected_schema":
        xmd = simulator.corpus.tables["versions"][0]["xmdMain"]
        xmd["measures"] = []
    elif mutation == "security":
        simulator.corpus.tables["versions"][0]["predicate"] = "operator scope changed"
    elif mutation == "visible_count":
        simulator.corpus.tables[ROWS].pop()
    elif mutation == "null_key":
        simulator.corpus.tables[ROWS][-1]["Region"] = None
    else:
        simulator.corpus.tables[ROWS][-1] = dict(simulator.corpus.tables[ROWS][-2])
    with pytest.raises(CrmAnalyticsError):
        list(iterator)
    assert_incomplete(connector)


def test_selected_framework_batch_reader_pins_across_distinct_instances(simulator, wire):
    replace_rows(simulator, 407)
    config = {**CONFIG, "page_size": "101"}
    options = {
        **config,
        **SELECTED,
        "version_id": VERSION,
        "tableName": DATASET,
        "source_name": "crm_analytics",
        "databricks.connection": "synthetic_uc",
        "community_oauth_flow": "u2m",
        "client_id": "synthetic-client",
        "access_token": CONFIG["access_token"],
        "max_query_requests": "8",
        "max_read_seconds": "120",
    }
    schema_source = CrmAnalyticsLakeflowConnect(config)
    read_source = CrmAnalyticsLakeflowConnect(config)
    schema = schema_source.get_table_schema(DATASET, options)
    reader = LakeflowBatchReader(options, schema, read_source)
    rows = list(reader.read(reader.partitions()[0]))
    assert len(rows) == 407
    assert all(set(row.asDict()) == {"Account__Id", "Amount", "Region"} for row in rows)
    evidence = read_source.get_read_evidence(DATASET, options)
    assert evidence["extracted_row_count"] == evidence["unique_key_count"] == 407
    assert evidence["query_requests"] == 8
    assert len(query_calls(wire)) == 8


@pytest.mark.parametrize(
    "name,maximum", [("max_query_requests", 100000), ("max_read_seconds", 86400)]
)
@pytest.mark.parametrize(
    "value", [0, -1, True, False, None, "1.5", 1.5, "inf", "", "+2", " 2", "too_large"]
)
def test_invalid_budget_fails_before_http(connector, wire, name, maximum, value):
    options = {**SELECTED, name: maximum + 1 if value == "too_large" else value}
    with pytest.raises(CrmAnalyticsError, match=name):
        exhaust(connector, options)
    assert not wire[0]


@pytest.mark.parametrize("count,minimum", [(0, 3), (1, 4), (2, 4), (3, 5), (7, 7), (8, 7)])
def test_query_preflight_and_exact_bound(simulator, connector, wire, clock, count, minimum):
    replace_rows(simulator, count)
    rejected = {**SELECTED, "max_query_requests": str(minimum - 1)}
    with pytest.raises(CrmAnalyticsError, match="max_query_requests"):
        connector.read_table(DATASET, None, rejected)
    assert not query_calls(wire)
    assert_incomplete(connector, rejected)
    accepted = {**SELECTED, "max_query_requests": str(minimum)}
    assert len(exhaust(connector, accepted)) == count
    assert len(query_calls(wire)) == minimum
    evidence = connector.get_read_evidence(DATASET, accepted)
    assert evidence["query_requests"] == minimum
    assert evidence["elapsed_seconds"] == 0.0
    assert evidence["max_query_requests"] == minimum
    assert evidence["max_read_seconds"] == 1800


def test_defaults_and_budget_changes_are_not_cached_identity(connector, wire, clock):
    descriptor = connector.pin_table(DATASET, SELECTED)
    assert len(exhaust(connector, SELECTED)) == 7
    assert connector.get_read_evidence(DATASET, SELECTED)["max_query_requests"] == 2000
    lower = {**SELECTED, "max_query_requests": "6", "max_read_seconds": "1"}
    assert connector.pin_table(DATASET, lower) == descriptor
    wire[0].clear()
    with pytest.raises(CrmAnalyticsError, match="max_query_requests"):
        connector.read_table(DATASET, None, lower)
    assert not wire[0]
    assert_incomplete(connector, SELECTED)
    higher = {**SELECTED, "max_query_requests": "8", "max_read_seconds": "9"}
    assert len(exhaust(connector, higher)) == 7
    evidence = connector.get_read_evidence(DATASET, higher)
    assert evidence["max_query_requests"] == 8 and evidence["max_read_seconds"] == 9
    assert evidence["query_requests"] == 7
    assert connector.pin_table(DATASET, higher) == descriptor


@pytest.mark.parametrize("query_limit,time_limit", [(1, 1), (100000, 86400)])
def test_inclusive_budget_configuration_bounds(connector, query_limit, time_limit):
    ordinary = connector.pin_table(DATASET, SELECTED)
    changed = connector.pin_table(
        DATASET,
        {**SELECTED, "max_query_requests": str(query_limit), "max_read_seconds": str(time_limit)},
    )
    assert ordinary == changed


def test_projected_unicode_source_name_and_legacy_names_remain_exact(connector):
    options = {**SELECTED, "selected_columns": ["État, rôle", "Region", "Account.Id"]}
    rows = exhaust(connector, options)
    assert len(rows) == 7
    assert all(
        set(row) == {"Account__Id", "Region", "_u00c9_tat_u002c__u0020_r_u00f4_le"} for row in rows
    )
    legacy = {**SELECTED, "column_name_mode": "legacy_hex"}
    legacy_rows = exhaust(connector, legacy)
    assert len(legacy_rows) == 7
    assert all(
        set(row) == {"c_4163636f756e742e4964", "c_416d6f756e74", "c_526567696f6e"}
        for row in legacy_rows
    )


@pytest.mark.parametrize("mutation", ["security", "selected_schema"])
def test_budget_tuning_does_not_repin_to_changed_metadata(simulator, connector, mutation):
    original = connector.pin_table(DATASET, SELECTED)
    if mutation == "security":
        simulator.corpus.tables["versions"][0]["predicate"] = "changed"
    else:
        simulator.corpus.tables["versions"][0]["xmdMain"]["dimensions"].append({"field": "Amount"})
    adjusted = {**SELECTED, "max_query_requests": "9", "max_read_seconds": "3600"}
    assert connector.pin_table(DATASET, adjusted) == original
    with pytest.raises(CrmAnalyticsError):
        exhaust(connector, adjusted)
    assert_incomplete(connector, adjusted)


@pytest.mark.parametrize("retry_stage", ["count", "page", "terminal", "final_count"])
@pytest.mark.parametrize("enough", [False, True])
def test_every_post_retry_consumes_query_budget(simulator, wire, clock, retry_stage, enough):
    source = CrmAnalyticsLakeflowConnect({**CONFIG, "max_retries": "1"})
    target = {"count": 1, "page": 2, "terminal": 6, "final_count": 7}[retry_stage]
    state = {"posts": 0}

    def transform(prepared, response):
        if prepared.method == "POST":
            state["posts"] += 1
            if state["posts"] == target:
                response.status_code = 503
        return response

    wire[1]["transform"] = transform
    options = {**SELECTED, "max_query_requests": "8" if enough else "7"}
    if enough:
        assert len(exhaust(source, options)) == 7
        evidence = source.get_read_evidence(DATASET, options)
        assert evidence["query_requests"] == 8 and evidence["elapsed_seconds"] == 1.0
        assert clock.sleeps == [1.0]
    else:
        with pytest.raises(CrmAnalyticsError, match="max_query_requests"):
            exhaust(source, options)
        assert_incomplete(source, options)
        # When the final allowed POST itself fails, do not spend a retry delay.
        assert clock.sleeps == ([] if retry_stage == "final_count" else [1.0])
    assert len(query_calls(wire)) == (8 if enough else 7)


@pytest.mark.parametrize("exception", [requests.Timeout, requests.ConnectionError])
def test_transport_retry_attempt_is_counted_before_response(
    simulator, wire, clock, monkeypatch, exception
):
    source = CrmAnalyticsLakeflowConnect({**CONFIG, "max_retries": "1"})
    original = requests.Session.send
    attempted = []

    def send(session, prepared, **kwargs):
        if prepared.method == "POST":
            attempted.append(prepared)
            if len(attempted) == 1:
                raise exception("synthetic-secret-row-details")
        return original(session, prepared, **kwargs)

    monkeypatch.setattr(requests.Session, "send", send)
    options = {**SELECTED, "max_query_requests": "8"}
    assert len(exhaust(source, options)) == 7
    assert source.get_read_evidence(DATASET, options)["query_requests"] == len(attempted) == 8
    assert clock.sleeps == [1.0]


def test_get_retries_take_time_but_no_query_allowance(simulator, wire, clock):
    source = CrmAnalyticsLakeflowConnect({**CONFIG, "max_retries": "1"})

    def transform(prepared, response):
        if len(wire[0]) == 1:
            assert prepared.method == "GET"
            response.status_code = 503
        return response

    wire[1]["transform"] = transform
    options = {**SELECTED, "max_query_requests": "7"}
    assert len(exhaust(source, options)) == 7
    evidence = source.get_read_evidence(DATASET, options)
    assert evidence["query_requests"] == 7 and evidence["elapsed_seconds"] == 1.0
    assert len(wire[0]) == 12  # 5 GETs (one retry), 7 POSTs.


@pytest.mark.parametrize("at_start", [True, False])
def test_elapsed_budget_starts_at_read_call_and_includes_lazy_consumer_pause(
    connector, wire, clock, at_start
):
    options = {**SELECTED, "max_read_seconds": "5"}
    iterator, _ = connector.read_table(DATASET, None, options)
    if not at_start:
        # Consume every row but not the final checks: there is no success yet.
        for _ in range(7):
            next(iterator)
    posts_before = len(query_calls(wire))
    clock.advance(5)
    with pytest.raises(CrmAnalyticsError, match="max_read_seconds"):
        next(iterator)
    assert len(query_calls(wire)) == posts_before
    assert_incomplete(connector, options)


@pytest.mark.parametrize(
    "slow_stage",
    [
        "cold_dataset",
        "cold_version",
        "count",
        "page",
        "terminal",
        "final_count",
        "final_dataset",
        "final_version",
    ],
)
def test_slow_http_response_at_every_stage_aborts_and_closes(connector, wire, clock, slow_stage):
    target = {
        "cold_dataset": 1,
        "cold_version": 2,
        "count": 3,
        "page": 4,
        "terminal": 8,
        "final_count": 9,
        "final_dataset": 10,
        "final_version": 11,
    }[slow_stage]
    closed = []

    def transform(_prepared, response):
        if len(wire[0]) == target:
            original_close = response.close

            def close():
                closed.append(True)
                original_close()

            response.close = close
            clock.advance(5)
        return response

    wire[1]["transform"] = transform
    options = {**SELECTED, "max_read_seconds": "5"}
    with pytest.raises(CrmAnalyticsError, match="max_read_seconds"):
        exhaust(connector, options)
    assert len(wire[0]) == target and closed == [True]
    assert_incomplete(connector, options)


@pytest.mark.parametrize("slow_part", ["json_text", "response_close"])
def test_elapsed_budget_includes_parsing_and_cleanup(
    connector, wire, clock, monkeypatch, slow_part
):
    if slow_part == "json_text":
        original_text = requests.Response.text.fget

        def text(response):
            # Response.text's property getter returns str; Pylint misses fget.
            # pylint: disable=assignment-from-no-return
            result = original_text(response)
            if response.request.method == "POST":
                clock.advance(5)
            return result

        monkeypatch.setattr(requests.Response, "text", property(text))
    else:
        original_close = requests.Response.close

        def close(response):
            original_close(response)
            if response.request.method == "POST":
                clock.advance(5)

        monkeypatch.setattr(requests.Response, "close", close)
    with pytest.raises(CrmAnalyticsError, match="max_read_seconds"):
        exhaust(connector, {**SELECTED, "max_read_seconds": "5"})
    assert len(query_calls(wire)) == 1
    assert_incomplete(connector)


def test_each_timeout_is_bounded_by_remaining_time(connector, simulator, clock, monkeypatch):
    del simulator
    original = requests.Session.send
    observed = []

    def send(session, prepared, **kwargs):
        observed.append(kwargs["timeout"])
        result = original(session, prepared, **kwargs)
        clock.advance(0.25)
        return result

    monkeypatch.setattr(requests.Session, "send", send)
    options = {**SELECTED, "max_read_seconds": "5"}
    assert len(exhaust(connector, options)) == 7
    assert observed == [(5.0 - index * 0.25,) * 2 for index in range(11)]
    assert connector.get_read_evidence(DATASET, options)["elapsed_seconds"] == 2.75


@pytest.mark.parametrize("after_sleep", [False, True])
def test_retry_delay_cannot_outlive_elapsed_budget(
    simulator, wire, clock, monkeypatch, after_sleep
):
    source = CrmAnalyticsLakeflowConnect({**CONFIG, "max_retries": "1"})

    def transform(prepared, response):
        if prepared.method == "POST":
            response.status_code = 503
            if not after_sleep:
                clock.advance(4)
        return response

    wire[1]["transform"] = transform
    if after_sleep:

        def oversleep(seconds):
            clock.sleeps.append(seconds)
            clock.advance(5)

        monkeypatch.setattr(crm.time, "sleep", oversleep)
    with pytest.raises(CrmAnalyticsError, match="max_read_seconds"):
        exhaust(source, {**SELECTED, "max_read_seconds": "5"})
    assert len(query_calls(wire)) == 1
    assert clock.sleeps == ([1.0] if after_sleep else [])
    assert_incomplete(source)


def test_new_reads_have_fresh_clock_and_old_iterator_cannot_clear_new_evidence(connector, clock):
    options = {**SELECTED, "max_read_seconds": "2", "max_query_requests": "7"}
    stale, _ = connector.read_table(DATASET, None, options)
    next(stale)
    clock.advance(3)
    assert len(exhaust(connector, options)) == 7
    evidence = connector.get_read_evidence(DATASET, options)
    assert evidence["elapsed_seconds"] == 0.0
    with pytest.raises(CrmAnalyticsError, match="Concurrent"):
        list(stale)
    assert connector.get_read_evidence(DATASET, options) == evidence


def test_pickled_connector_does_not_carry_budget_clock_or_allowance(connector, clock):
    small = {**SELECTED, "max_read_seconds": "1", "max_query_requests": "7"}
    iterator, _ = connector.read_table(DATASET, None, small)
    next(iterator)
    clock.advance(3)
    restored = pickle.loads(pickle.dumps(connector))
    larger = {**SELECTED, "max_read_seconds": "2", "max_query_requests": "9"}
    assert len(exhaust(restored, larger)) == 7
    evidence = restored.get_read_evidence(DATASET, larger)
    assert evidence["query_requests"] == 7 and evidence["elapsed_seconds"] == 0.0
    assert evidence["max_query_requests"] == 9 and evidence["max_read_seconds"] == 2
    with pytest.raises(CrmAnalyticsError, match="max_read_seconds"):
        list(iterator)
    assert_incomplete(connector)


def test_different_tables_have_independent_budgets_and_evidence(simulator, connector, clock):
    other = "0Fb000000000002"
    dataset = copy.deepcopy(simulator.corpus.tables["datasets"][0])
    dataset["id"] = other
    version = copy.deepcopy(simulator.corpus.tables["versions"][0])
    version["dataset"]["id"] = other
    simulator.corpus.tables["datasets"].append(dataset)
    simulator.corpus.tables["versions"].append(version)
    simulator.corpus.tables[f"rows_{other}_{VERSION}"] = copy.deepcopy(
        simulator.corpus.tables[ROWS]
    )
    options = {**SELECTED, "max_query_requests": "7", "max_read_seconds": "2"}
    old, _ = connector.read_table(DATASET, None, options)
    next(old)
    clock.advance(2)
    fresh, _ = connector.read_table(other, None, options)
    assert len(list(fresh)) == 7
    with pytest.raises(CrmAnalyticsError, match="max_read_seconds"):
        list(old)
    assert_incomplete(connector, options)
    assert connector.get_read_evidence(other, options)["query_requests"] == 7


def test_budget_errors_do_not_expose_token_or_source_values(connector, wire, clock):
    iterator, _ = connector.read_table(DATASET, None, {**SELECTED, "max_read_seconds": "1"})
    first = next(iterator)
    clock.advance(1)
    with pytest.raises(CrmAnalyticsError) as error:
        next(iterator)
    message = str(error.value)
    assert CONFIG["access_token"] not in message
    assert all(str(value) not in message for value in first.values() if value is not None)
    assert len(query_calls(wire)) == 2
