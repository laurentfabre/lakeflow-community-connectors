"""Regressions for assets that import successfully but cannot be read by Add Data."""

import base64
import json
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlsplit

import pytest
from click.testing import CliRunner
from databricks.sdk.errors import BadRequest, NotFound, PermissionDenied
from databricks.sdk.service.workspace import ExportFormat, ExportResponse, ImportFormat

from databricks.labs.community_connector_cli import cli

DIRECTORY = "/Users/me@example.com/.community-connectors"
ASSET = f"{DIRECTORY}/Salesforce CRM Analytics.connector.json"
SETUP = f"{DIRECTORY}/Salesforce CRM Analytics.setup.json"
WHEELS = ["/Volumes/main/raw/packages/framework.whl", "/Volumes/main/raw/packages/crm.whl"]


@pytest.fixture
def workspace(monkeypatch):
    ws = MagicMock()
    ws.config.host = "https://test.databricks.com/"
    ws.current_user.me.return_value.user_name = "me@example.com"
    files = {}

    def import_manifest(*, path, content, format, overwrite):
        if path in files and not overwrite:
            raise BadRequest("RESOURCE_ALREADY_EXISTS")
        files[path] = content

    def export_manifest(*, path, format):
        assert format == ExportFormat.SOURCE
        if path not in files:
            raise NotFound("RESOURCE_DOES_NOT_EXIST")
        return ExportResponse(content=files[path])

    def get_status(path):
        if path not in files:
            raise NotFound("RESOURCE_DOES_NOT_EXIST")
        return MagicMock()

    ws.workspace.import_.side_effect = import_manifest
    ws.workspace.export.side_effect = export_manifest
    ws.workspace.get_status.side_effect = get_status
    ws.workspace.delete.side_effect = lambda path: files.pop(path)
    ws.saved_files = files
    monkeypatch.setattr(cli, "_make_workspace_client", lambda: ws)
    monkeypatch.setattr(
        cli,
        "_load_connector_spec",
        lambda *_: {
            "display_name": "Salesforce CRM Analytics",
            "connection": {"parameters": []},
        },
    )
    monkeypatch.setattr(cli, "_resolve_managed_dest_dir", lambda *_: "/Volumes/main/raw/packages")
    monkeypatch.setattr(cli, "_build_and_upload_managed_wheels", lambda *_: WHEELS)
    return ws


def unreadable_asset(workspace):
    export = workspace.workspace.export.side_effect

    def export_manifest(*, path, format):
        if path == ASSET:
            raise BadRequest(f"No access to read file at {path}")
        return export(path=path, format=format)

    workspace.workspace.export.side_effect = export_manifest


def setup_query(output):
    url = next(
        line.split("Setup:     ", 1)[1]
        for line in output.splitlines()
        if line.startswith("Setup:     ")
    )
    parsed = urlsplit(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "test.databricks.com"
    assert parsed.path == "/ingestion/add"
    return parse_qs(parsed.query)


def test_publish_reads_asset_and_outputs_encoded_setup_route(workspace):
    result = CliRunner().invoke(cli.main, ["publish", "crm_analytics"])
    assert result.exit_code == 0, result.output
    assert list(workspace.saved_files) == [ASSET]
    query = setup_query(result.output)
    assert query["ingestion-addDataPipeline-communityConnectorPath"] == [ASSET]
    assert query["ingestion-addDataPipeline-sourceType"] == ["COMMUNITY"]
    assert query["ingestion-addDataPipeline-open"] == ["true"]
    assert query["ingestion-addDataPipeline-step"] == ["connection"]
    assert "now appears" not in result.output


def test_unreadable_special_asset_gets_verified_raw_json_fallback(workspace):
    unreadable_asset(workspace)
    result = CliRunner().invoke(cli.main, ["publish", "crm_analytics"])
    assert result.exit_code == 0, result.output
    assert setup_query(result.output)["ingestion-addDataPipeline-communityConnectorPath"] == [SETUP]
    assert workspace.saved_files[ASSET] == workspace.saved_files[SETUP]
    assert json.loads(base64.b64decode(workspace.saved_files[SETUP]))["dependencies"] == WHEELS
    writes = workspace.workspace.import_.call_args_list
    assert writes[0].kwargs["format"] == ImportFormat.AUTO
    assert writes[1].kwargs["format"] == ImportFormat.RAW
    assert workspace.workspace.export.call_args_list[-1].kwargs["path"] == SETUP


@pytest.mark.parametrize(
    "error",
    [
        PermissionDenied("No access to read file"),
        BadRequest("An unrelated invalid request"),
        ConnectionError("Network unavailable"),
    ],
)
def test_other_export_failures_do_not_create_fallback_or_claim_success(workspace, error):
    workspace.workspace.export.side_effect = error
    result = CliRunner().invoke(cli.main, ["publish", "crm_analytics"])
    assert result.exit_code != 0
    assert list(workspace.saved_files) == [ASSET]
    assert "Published!" not in result.output
    assert "Setup:" not in result.output


@pytest.mark.parametrize("content", ["invalid-base64!", base64.b64encode(b"{}").decode()])
def test_corrupt_or_changed_readback_is_rejected(workspace, content):
    workspace.workspace.export.side_effect = None
    workspace.workspace.export.return_value = ExportResponse(content=content)
    result = CliRunner().invoke(cli.main, ["publish", "crm_analytics"])
    assert result.exit_code != 0
    assert list(workspace.saved_files) == [ASSET]
    assert "Published!" not in result.output


def test_unreadable_fallback_is_not_reported_as_published(workspace):
    workspace.workspace.export.side_effect = BadRequest("No access to read file")
    result = CliRunner().invoke(cli.main, ["publish", "crm_analytics"])
    assert result.exit_code != 0
    assert "Setup manifest readback failed" in result.output
    assert "Published!" not in result.output
    assert "Setup:" not in result.output


def test_overwrite_applies_to_both_manifests(workspace):
    unreadable_asset(workspace)
    workspace.saved_files[ASSET] = "old"
    workspace.saved_files[SETUP] = "old"
    result = CliRunner().invoke(cli.main, ["publish", "crm_analytics", "--overwrite"])
    assert result.exit_code == 0, result.output
    assert workspace.saved_files[ASSET] == workspace.saved_files[SETUP] != "old"
    assert all(call.kwargs["overwrite"] for call in workspace.workspace.import_.call_args_list)


@pytest.mark.parametrize("paths", [(ASSET, SETUP), (SETUP,)])
def test_unpublish_removes_both_manifests_or_a_remaining_setup_file(workspace, paths):
    workspace.saved_files.update({path: "old" for path in paths})
    result = CliRunner().invoke(cli.main, ["unpublish", "crm_analytics", "--yes"])
    assert result.exit_code == 0, result.output
    assert not workspace.saved_files
    assert [call.kwargs["path"] for call in workspace.workspace.delete.call_args_list] == list(
        paths
    )
