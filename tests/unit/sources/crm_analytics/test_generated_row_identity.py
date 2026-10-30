"""Generated identities preserve raw rows without inventing cross-version entity keys."""

# pylint: disable=redefined-outer-name,unused-import

import copy
import json
from collections import Counter

import pytest

from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowBatchReader
from tests.unit.sources.crm_analytics.test_crm_analytics_lakeflow_connect import (
    CONFIG,
    DATASET,
    NEXT_VERSION,
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

ROW_ID = "_ingestion_row_id"
VERSION_COLUMN = "_crm_analytics_version_id"
RAW = {
    "supported_scope": OPTIONS["supported_scope"],
    "scope_fingerprint": OPTIONS["scope_fingerprint"],
    "selected_columns": ["Account.Id", "Amount", "Region"],
    "auto_increment_column": ROW_ID,
}


def raw_values(rows):
    return Counter((row["Account__Id"], row["Region"], str(row["Amount"])) for row in rows)


def omit_null_properties(prepared, response):
    if prepared.method != "POST" or "count()" in query_text(prepared):
        return response
    body = json.loads(response.text)
    body["results"]["records"] = [
        {key: value for key, value in row.items() if value is not None}
        for row in body["results"]["records"]
    ]
    return response_body(response, body)


def test_sparse_null_rows_are_recovered_only_with_salesforce_confirmation(source, simulator, wire):
    rows = replace_rows(simulator, 7)
    rows[0]["Account.Id"] = None
    rows[0]["Region"] = None
    rows[1]["Amount"] = None
    rows[2]["Region"] = ""  # Empty strings remain distinct from nulls.
    rows[-1] = copy.deepcopy(rows[0])
    expected = Counter((row["Account.Id"], row["Region"], str(row["Amount"])) for row in rows)
    wire[1]["transform"] = omit_null_properties
    result = exhaust(source, RAW)
    assert raw_values(result) == expected
    assert [row[ROW_ID] for row in result] == list(range(1, 8))
    assert all(
        set(row) == {"Account__Id", "Amount", "Region", ROW_ID, VERSION_COLUMN} for row in result
    )
    assert source.get_read_evidence(DATASET, RAW)["extracted_row_count"] == 7


@pytest.mark.parametrize(
    "mutation", ["missing", "bool", "string", "invalid", "null", "false_null", "false_value"]
)
def test_raw_reads_reject_absent_invalid_or_inconsistent_null_flags(
    source, simulator, wire, mutation
):
    simulator.corpus.tables[ROWS][0]["Account.Id"] = None

    def transform(prepared, response):
        if prepared.method != "POST" or "count()" in query_text(prepared):
            return response
        body = json.loads(response.text)
        records = body["results"]["records"]
        if records:
            row = records[0]
            flag = "_crm_analytics_null_0"
            assert row[flag] == 1
            if mutation == "missing":
                row.pop(flag)
            elif mutation in {"bool", "string", "invalid", "null"}:
                row[flag] = {"bool": True, "string": "1", "invalid": 2, "null": None}[mutation]
            elif mutation == "false_null":
                row[flag] = 0
            else:
                row["Account__Id"] = "source-value-must-not-appear-in-diagnostics"
        return response_body(response, body)

    wire[1]["transform"] = transform
    with pytest.raises(RuntimeError, match="Salesforce null flag") as error:
        exhaust(source, RAW)
    assert "source-value-must-not-appear-in-diagnostics" not in str(error.value)
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, RAW)


def test_null_flag_aliases_do_not_collide_with_physical_or_generated_columns(
    source, simulator, wire
):
    name = "_CRM_ANALYTICS_NULL_0"
    simulator.corpus.tables["versions"][0]["xmdMain"]["dimensions"].append({"field": name})
    for row in simulator.corpus.tables[ROWS]:
        row[name] = "physical-column-value"
    simulator.corpus.tables[ROWS][0]["Account.Id"] = None
    wire[1]["transform"] = omit_null_properties
    options = {
        **RAW,
        "selected_columns": RAW["selected_columns"] + [name],
        "auto_increment_column": "_crm_analytics_null_1",
    }
    result = exhaust(source, options)
    assert len(result) == 7
    assert all(row[name] == "physical-column-value" for row in result)
    assert [row["_crm_analytics_null_1"] for row in result] == list(range(1, 8))
    assert source.get_read_evidence(DATASET, options)["extracted_row_count"] == 7


@pytest.mark.parametrize("with_source_keys", [False, True])
def test_raw_ingestion_preserves_duplicate_and_null_business_keys(
    source, simulator, wire, with_source_keys
):
    rows = replace_rows(simulator, 7)
    for row in rows:
        row["Account.Id"] = "duplicate-account"
        row["Region"] = "same-region"
    rows[0]["Account.Id"] = None
    rows[1]["Region"] = None
    rows[-1] = copy.deepcopy(rows[-2])  # Identical projected rows must both survive.
    expected = Counter((row["Account.Id"], row["Region"], str(row["Amount"])) for row in rows)
    options = {
        **RAW,
        **({"source_primary_keys": OPTIONS["source_primary_keys"]} if with_source_keys else {}),
    }
    metadata = source.read_table_metadata(DATASET, options)
    assert metadata["primary_keys"] == [VERSION_COLUMN, ROW_ID]
    assert metadata["key_mode"] == "version_row_number"
    schema = source.get_table_schema(DATASET, options)
    assert schema[ROW_ID].dataType.typeName() == "long"
    assert schema[VERSION_COLUMN].dataType.typeName() == "string"
    assert not schema[ROW_ID].nullable and not schema[VERSION_COLUMN].nullable
    assert schema[ROW_ID].metadata == {"crm_analytics_generated": "row_number"}
    result = exhaust(source, options)
    assert raw_values(result) == expected
    assert [row[ROW_ID] for row in result] == list(range(1, 8))
    assert {row[VERSION_COLUMN] for row in result} == {VERSION}
    assert len({(row[VERSION_COLUMN], row[ROW_ID]) for row in result}) == 7
    evidence = source.get_read_evidence(DATASET, options)
    assert evidence["key_mode"] == "version_row_number"
    assert evidence["extracted_row_count"] == evidence["unique_key_count"] == 7
    assert evidence["source_keys_validated"] is False
    for prepared in wire[0]:
        query = query_text(prepared)
        if not query:
            continue
        assert ROW_ID not in query and VERSION_COLUMN not in query
        if "count()" not in query:
            assert "q = order q by ('Account__Id' asc, 'Amount' asc, 'Region' asc);" in query


def test_repeated_pinned_read_is_stable_across_input_order_and_page_size(source, simulator):
    rows = replace_rows(simulator, 9)
    for index, row in enumerate(rows):
        row["Account.Id"] = "same-account"
        row["Region"] = "same-region"
        row["Amount"] = str(index % 3)
    first = exhaust(source, {**RAW, "version_id": VERSION})
    simulator.corpus.tables[ROWS].reverse()
    other = type(source)({**CONFIG, "page_size": "4"})
    try:
        assert exhaust(other, {**RAW, "version_id": VERSION}) == first
    finally:
        other.close()


def test_row_numbers_restart_but_ingestion_keys_do_not_overlap_across_versions(source, simulator):
    first = exhaust(source, {**RAW, "version_id": VERSION})
    second_version = copy.deepcopy(simulator.corpus.tables["versions"][0])
    second_version.update(id=NEXT_VERSION, createdDate="2026-10-30T17:00:00Z")
    simulator.corpus.tables["versions"].append(second_version)
    simulator.corpus.tables[f"rows_{DATASET}_{NEXT_VERSION}"] = copy.deepcopy(
        simulator.corpus.tables[ROWS]
    )
    second = exhaust(source, {**RAW, "version_id": NEXT_VERSION})
    assert [row[ROW_ID] for row in first] == [row[ROW_ID] for row in second] == list(range(1, 8))
    old_keys = {(row[VERSION_COLUMN], row[ROW_ID]) for row in first}
    new_keys = {(row[VERSION_COLUMN], row[ROW_ID]) for row in second}
    assert old_keys.isdisjoint(new_keys)


def test_generated_mode_changes_schema_and_configuration_identity(source):
    baseline = source.pin_table(DATASET, OPTIONS)
    generated = source.pin_table(DATASET, {**OPTIONS, "auto_increment_column": ROW_ID})
    renamed = source.pin_table(DATASET, {**OPTIONS, "auto_increment_column": "other_row_id"})
    assert baseline["schema_fingerprint"] != generated["schema_fingerprint"]
    assert baseline["configuration_fingerprint"] != generated["configuration_fingerprint"]
    assert renamed["configuration_fingerprint"] != generated["configuration_fingerprint"]
    assert generated["column_mapping"] == baseline["column_mapping"]
    assert "key_mode" not in baseline


@pytest.mark.parametrize(
    "name", [None, True, "", "a.b", "x;query", "x\nname", "x" * 129, VERSION_COLUMN, "REGION"]
)
def test_invalid_or_colliding_generated_names_fail_before_query(source, wire, name):
    with pytest.raises(RuntimeError, match="auto_increment_column|generated column"):
        exhaust(source, {**RAW, "auto_increment_column": name})
    assert not any(call.method == "POST" for call in wire[0])


def test_generated_version_column_collision_fails_before_query(source, simulator, wire):
    simulator.corpus.tables["versions"][0]["xmdMain"]["dimensions"].append(
        {"field": VERSION_COLUMN}
    )
    with pytest.raises(RuntimeError, match="generated column"):
        source.pin_table(DATASET, {**OPTIONS, "auto_increment_column": ROW_ID})
    assert not any(call.method == "POST" for call in wire[0])


def test_source_key_options_remain_validated_if_provided(source, wire):
    with pytest.raises(RuntimeError, match="source_primary_keys"):
        source.pin_table(DATASET, {**RAW, "source_primary_keys": "[]"})
    with pytest.raises(RuntimeError, match="include every source_primary_keys"):
        source.pin_table(DATASET, {**RAW, "source_primary_keys": '["Name"]'})
    assert not any(call.method == "POST" for call in wire[0])


def test_primary_key_override_must_match_generated_metadata(source):
    baseline = source.pin_table(DATASET, RAW)
    assert source.pin_table(DATASET, {**RAW, "primary_keys": [VERSION_COLUMN, ROW_ID]}) == baseline
    with pytest.raises(RuntimeError, match="primary_keys"):
        source.pin_table(DATASET, {**RAW, "primary_keys": ["Account__Id", "Region"]})


def test_generated_schema_and_framework_reader_on_separate_instances(source, simulator, wire):
    rows = replace_rows(simulator, 407)
    for index, row in enumerate(rows):
        row["Account.Id"] = None if index % 7 == 0 else "duplicate-account"
        row["Region"] = "same-region"
    wire[1]["transform"] = omit_null_properties
    options = {**CONFIG, **RAW, "tableName": DATASET, "version_id": VERSION}
    schema_source = type(source)({**CONFIG, "page_size": "101"})
    reader_source = type(source)({**CONFIG, "page_size": "101"})
    try:
        schema = schema_source.get_table_schema(DATASET, options)
        reader = LakeflowBatchReader(options, schema, reader_source)
        result = list(reader.read(reader.partitions()[0]))
        assert len(result) == 407
        indexed = [row.asDict() for row in result]
        assert [row[ROW_ID] for row in indexed] == list(range(1, 408))
        assert len({(row[VERSION_COLUMN], row[ROW_ID]) for row in indexed}) == 407
        assert reader_source.get_read_evidence(DATASET, options)["query_requests"] == 8
    finally:
        schema_source.close()
        reader_source.close()


def test_generated_mode_validates_empty_snapshot(source, simulator):
    replace_rows(simulator, 0)
    assert exhaust(source, RAW) == []
    evidence = source.get_read_evidence(DATASET, RAW)
    assert evidence["extracted_row_count"] == evidence["unique_key_count"] == 0
    assert evidence["terminal_empty_page"] and evidence["metadata_revalidated"]


def test_generated_names_remain_literal_in_legacy_column_mode(source):
    options = {**RAW, "column_name_mode": "legacy_hex"}
    metadata = source.read_table_metadata(DATASET, options)
    assert metadata["primary_keys"] == [VERSION_COLUMN, ROW_ID]
    assert metadata["column_mapping"]["Account.Id"] == "c_4163636f756e742e4964"
    rows = exhaust(source, options)
    assert [row[ROW_ID] for row in rows] == list(range(1, 8))
    assert {row[VERSION_COLUMN] for row in rows} == {VERSION}


@pytest.mark.parametrize("failure", ["dimension", "measure", "metadata"])
def test_generated_mode_keeps_scalar_and_metadata_validation(source, simulator, failure):
    if failure == "dimension":
        simulator.corpus.tables[ROWS][0]["Region"] = ["multi", "value"]
    elif failure == "measure":
        simulator.corpus.tables[ROWS][0]["Amount"] = True
    else:
        simulator.corpus.tables["versions"][0]["xmdMain"]["dimensions"].append({})
    with pytest.raises(RuntimeError, match="Non-scalar|numeric|Malformed"):
        exhaust(source, RAW)


@pytest.mark.parametrize("failure", ["page", "count", "budget"])
def test_generated_mode_still_aborts_incomplete_or_over_budget_reads(source, wire, failure):
    def transform(prepared, response):
        if prepared.method != "POST":
            return response
        body = json.loads(response.text)
        records = body["results"]["records"]
        if failure == "count" and "count()" in query_text(prepared):
            records[0]["__row_count"] = 8
        elif failure == "page" and "count()" not in query_text(prepared) and records:
            records[0].pop("Amount")
        return response_body(response, body)

    wire[1]["transform"] = transform
    options = {**RAW, **({"max_query_requests": "1"} if failure == "budget" else {})}
    with pytest.raises(RuntimeError, match="projection|count|max_query_requests"):
        exhaust(source, options)
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, options)
