"""Regressions for customer-reported metadata and projection failures.

Exercise both the package and the merged source through synthetic HTTP responses.
The corpus's historical request/filter enum is independent of response casing.
"""

# pylint: disable=redefined-outer-name,unused-import

import json
from types import SimpleNamespace

import pytest

from databricks.labs.community_connector.sources.crm_analytics import (
    _generated_crm_analytics_python_source as merged,
    crm_analytics as package,
)
from tests.unit.sources.crm_analytics.test_crm_analytics_lakeflow_connect import (
    CONFIG,
    DATASET,
    OPTIONS,
    exhaust,
    response_body,
    simulator,
    wire,
)


@pytest.fixture(params=[package, merged], ids=["package", "merged"])
def source_module(request):
    return request.param


@pytest.fixture
def source(source_module, simulator):
    del simulator
    if source_module is merged:
        registered = []
        merged.register_lakeflow_source(
            SimpleNamespace(dataSource=SimpleNamespace(register=registered.append))
        )
        assert len(registered) == 1
        result = registered[0](CONFIG).lakeflow_connect
    else:
        result = source_module.CrmAnalyticsLakeflowConnect(CONFIG)
    yield result
    result.close()


def dataset_type_response(value):
    def transform(_prepared, response):
        body = json.loads(response.text)
        if "datasets" in body:
            for dataset in body["datasets"]:
                dataset["datasetType"] = value
        elif "datasetType" in body:
            body["datasetType"] = value
        return response_body(response, body)

    return transform


@pytest.mark.parametrize("option_case", ["lower", "upper"])
def test_connection_metadata_does_not_change_snapshot_identity(source, option_case):
    baseline = source.pin_table(DATASET, OPTIONS)
    metadata = {
        "access_token_expiration": "2026-10-30T17:00:00Z",
        "refresh_token_expiration": "2026-11-30T17:00:00Z",
        "externaloptionsallowlist": "source_primary_keys,auto_increment_column",
        "sourcename": "crm_analytics",
    }
    options = {**OPTIONS, **{getattr(key, option_case)(): value for key, value in metadata.items()}}
    assert source.pin_table(DATASET, options) == baseline
    assert len(exhaust(source, options)) == 7
    evidence = source.get_read_evidence(DATASET, options)
    assert evidence["extracted_row_count"] == 7
    # Token rotation changes connection metadata, not the source data contract.
    rotated = {
        **OPTIONS,
        **{getattr(key, option_case)(): value + "-changed" for key, value in metadata.items()},
    }
    assert source.pin_table(DATASET, rotated) == baseline
    assert source.get_read_evidence(DATASET, rotated) == evidence


@pytest.mark.parametrize("dataset_type", ["default", "Default"])
def test_default_response_casing_in_discovery_and_complete_read(source, wire, dataset_type):
    wire[1]["transform"] = dataset_type_response(dataset_type)
    assert source.list_tables() == [DATASET]
    assert "datasetTypes=Default" in wire[0][0].url
    assert len(exhaust(source)) == 7
    assert source.get_read_evidence(DATASET, OPTIONS)["extracted_row_count"] == 7
    # Metadata resolution and final revalidation both inspect datasetType.
    assert sum(call.url.endswith(f"/datasets/{DATASET}") for call in wire[0]) >= 2


@pytest.mark.parametrize("dataset_type", ["Live", "external", "", None, [], {}, True])
@pytest.mark.parametrize("operation", ["discovery", "read"])
def test_other_or_malformed_dataset_types_still_fail(source, wire, dataset_type, operation):
    wire[1]["transform"] = dataset_type_response(dataset_type)
    with pytest.raises(RuntimeError, match="type"):
        if operation == "discovery":
            source.list_tables()
        else:
            exhaust(source)
    assert not any(call.method == "POST" for call in wire[0])


def test_dataset_type_change_at_final_revalidation_still_aborts(source, wire):
    dataset_gets = 0

    def transform(prepared, response):
        nonlocal dataset_gets
        if prepared.url.endswith(f"/datasets/{DATASET}"):
            dataset_gets += 1
            return dataset_type_response("default" if dataset_gets == 1 else "Live")(
                prepared, response
            )
        return response

    wire[1]["transform"] = transform
    with pytest.raises(RuntimeError, match="type"):
        exhaust(source)
    assert dataset_gets == 2
    with pytest.raises(RuntimeError, match="not completed"):
        source.get_read_evidence(DATASET, OPTIONS)


@pytest.mark.parametrize(
    "requested,expected",
    [
        ("amount", ['"amount"', "Case-sensitive matches", '"Amount"']),
        ("Account__Id", ['"Account__Id"', "Destination-name matches", '"Account.Id"']),
        ("missing_column", ['"missing_column"', "Available physical fields", '"Amount"']),
    ],
)
def test_unknown_selection_explains_source_names_before_query(source, wire, requested, expected):
    options = {**OPTIONS, "selected_columns": ["Account.Id", "Region", requested]}
    with pytest.raises(RuntimeError) as error:
        exhaust(source, options)
    message = str(error.value)
    assert "selected_columns contains an unknown physical field" in message
    assert all(fragment in message for fragment in expected)
    assert "pinned version" in message
    assert "labels" in message
    assert CONFIG["access_token"] not in message
    assert not any(call.method == "POST" for call in wire[0])


def test_legacy_destination_alias_is_only_a_hint(source, wire):
    options = {
        **OPTIONS,
        "column_name_mode": "legacy_hex",
        "selected_columns": ["Account.Id", "Region", "c_416d6f756e74"],
    }
    with pytest.raises(RuntimeError) as error:
        exhaust(source, options)
    assert 'Destination-name matches (use these source names): ["Amount"]' in str(error.value)
    assert not any(call.method == "POST" for call in wire[0])


def test_selection_diagnostics_are_bounded_and_escape_metadata(source, simulator, wire):
    xmd = simulator.corpus.tables["versions"][0]["xmdMain"]
    xmd["dimensions"] += [{"field": f"a\nfield-{index}"} for index in range(20)]

    def transform(_prepared, response):
        body = json.loads(response.text)
        if "xmdMain" in body:
            body["xmdMain"]["dimensions"].append({"field": "\ud800"})
            # Deliver an escaped surrogate in JSON, without asking the simulator
            # to encode an unpaired surrogate as a raw UTF-8 code point.
            response._content = json.dumps(body, ensure_ascii=True).encode("ascii")
        return response

    wire[1]["transform"] = transform
    options = {
        **OPTIONS,
        "selected_columns": ["Account.Id", "Region"] + [f"z{index}" * 200 for index in range(20)],
    }
    with pytest.raises(RuntimeError) as error:
        exhaust(source, options)
    message = str(error.value)
    assert "unknown physical field" in message
    assert "20 total" in message
    assert len(message) < 5000
    assert "\n" not in message
    assert CONFIG["access_token"] not in message
    assert not any(call.method == "POST" for call in wire[0])
