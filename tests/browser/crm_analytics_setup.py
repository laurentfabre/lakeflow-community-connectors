"""Exercise the offline assistant in the existing dedicated Chrome sandbox.

Run with the Playwright Python environment. No source credentials or customer
rows are used; only a new local-document tab is opened and then closed.
"""

import copy
import json
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = Path("/private/tmp/crm-analytics-setup-browser")
ARTIFACTS.mkdir(exist_ok=True)
DATASET = "0Fb000000000001"
VERSION = "0Fc000000000001"
SCOPE = "synthetic-full-visibility-fixture-v1"
REPORT = {
    "format_version": 1,
    "kind": "crm_analytics_inspection",
    "dataset_id": DATASET,
    "version_id": VERSION,
    "column_name_mode": "readable",
    "decimal_precision": 38,
    "decimal_scale": 18,
    "columns": [
        {
            "source_name": "Account.Id",
            "destination_name": "Account__Id",
            "type": "string",
            "supported": True,
        },
        {
            "source_name": "Amount",
            "destination_name": "Amount",
            "type": "decimal(38,18)",
            "supported": True,
        },
        {
            "source_name": "Calculated",
            "destination_name": None,
            "type": None,
            "supported": False,
            "reason": "Calculated field <b>excluded</b>",
        },
    ],
    "key_suggestions": ["Account.Id"],
}
BASELINE = {
    "format_version": 1,
    "dataset_id": DATASET,
    "column_name_mode": "readable",
    "scope_fingerprint": SCOPE,
    "security_fingerprint": "a" * 64,
    "auto_increment_column": None,
    "source_primary_keys": ["Account.Id"],
    "primary_keys": ["Account__Id"],
    "column_mapping": {"Account.Id": "Account__Id", "Amount": "Amount"},
    "schema_json": {
        "type": "struct",
        "fields": [
            {
                "name": "Account__Id",
                "type": "string",
                "nullable": True,
                "metadata": {"source_name": "Account.Id"},
            },
            {
                "name": "Amount",
                "type": "decimal(38,18)",
                "nullable": True,
                "metadata": {"source_name": "Amount"},
            },
        ],
    },
}


def upload(page, control, data):
    page.locator(control).set_input_files(
        {
            "name": "synthetic.json",
            "mimeType": "application/json",
            "buffer": json.dumps(data).encode(),
        }
    )


def generate(page):
    page.locator("#generate-spec").click()
    expect(page.locator("#configuration-error")).to_be_hidden()
    return json.loads(page.locator("#spec-output").inner_text())


with sync_playwright() as playwright:
    state = json.loads(Path("/private/tmp/crm-analytics-chrome-session.json").read_text())
    browser = playwright.chromium.connect_over_cdp(state["endpoint"])
    page = browser.contexts[0].new_page()
    errors, network, results = [], [], []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.on(
        "request",
        lambda request: (
            network.append(request.url) if request.url.startswith(("http:", "https:")) else None
        ),
    )
    try:
        page.set_viewport_size({"width": 1440, "height": 1000})
        page.goto((ROOT / "docs/crm-analytics-setup.html").as_uri())
        page.locator("#generate-spec").click()
        expect(page.locator("#configuration-error")).to_be_visible()
        expect(page.locator("#download-spec")).to_be_disabled()
        results.append("inspection required")

        upload(page, "#inspection-file", REPORT)
        expect(page.locator("#inspection-status")).to_contain_text("Inspection loaded")
        expect(page.locator("#field-include-2")).to_be_disabled()
        assert page.locator("#field-body b").count() == 0
        page.locator("#destination-catalog").fill("synthetic-catalog")
        page.locator("#destination-schema").fill("crm_analytics")
        page.locator("#scope-fingerprint").fill(SCOPE)
        page.locator("#generate-spec").click()
        expect(page.locator("#configuration-error")).to_contain_text("visibility")
        page.locator("#visibility-attestation").check()
        spec = generate(page)
        config = spec["objects"][0]["table"]["table_configuration"]
        assert config["auto_increment_column"] == "_ingestion_row_id"
        assert config["selected_columns"] == ["Account.Id", "Amount"]
        assert config["skip_unchanged_versions"] is True and "version_id" not in config
        assert not ({"access_token", "client_secret", "refresh_token"} & config.keys())
        results.append("raw projection and visibility attestation")

        with page.expect_download() as download_event:
            page.locator("#download-spec").click()
        download = download_event.value
        path = ARTIFACTS / "downloaded-spec.json"
        download.save_as(path)
        assert json.loads(path.read_text()) == spec
        page.locator("#destination-table").fill("changed_target")
        expect(page.locator("#download-spec")).to_be_disabled()
        results.append("download matches review; edits invalidate stale output")

        page.locator("#key-mode-strict").check()
        page.locator("#generate-spec").click()
        expect(page.locator("#configuration-error")).to_contain_text("keys")
        page.locator("#field-key-0").check()
        page.locator("#key-validation-mode").select_option("sqlite")
        page.locator("#key-validation-cache-kib").fill("1")
        page.locator("#generate-spec").click()
        expect(page.locator("#configuration-error")).to_contain_text("64")
        page.locator("#key-validation-cache-kib").fill("64")
        page.locator("#pin-version").check()
        spec = generate(page)
        config = spec["objects"][0]["table"]["table_configuration"]
        assert "auto_increment_column" not in config
        assert config["source_primary_keys"] == ["Account.Id"]
        assert (
            config["key_validation_mode"] == "sqlite" and config["key_validation_cache_kib"] == 64
        )
        assert config["version_id"] == VERSION
        results.append("strict keys, SQLite budget and version pin")

        new_report = copy.deepcopy(REPORT)
        new_report["columns"][2] = {
            "source_name": "Region",
            "destination_name": "Region",
            "type": "string",
            "supported": True,
        }
        upload(page, "#inspection-file", new_report)
        expect(page.locator("#field-include-2")).to_be_enabled()
        page.locator("#field-key-0").check()
        page.get_by_text("Optional approved schema baseline", exact=True).click()
        upload(page, "#baseline-file", BASELINE)
        expect(page.locator("#baseline-status")).to_contain_text("Baseline loaded")
        page.locator("#generate-spec").click()
        expect(page.locator("#configuration-error")).to_contain_text("projection")
        page.locator("#schema-evolution-mode").select_option("additive")
        spec = generate(page)
        config = spec["objects"][0]["table"]["table_configuration"]
        assert config["schema_baseline"] == BASELINE
        assert config["schema_evolution_mode"] == "additive" and "selected_columns" not in config
        page.locator("#scope-fingerprint").fill("different-scope")
        page.locator("#generate-spec").click()
        expect(page.locator("#configuration-error")).to_contain_text("scope")
        page.locator("#scope-fingerprint").fill(SCOPE)
        generate(page)
        results.append("baseline strict rejection, additive acceptance and scope guard")

        page.screenshot(path=str(ARTIFACTS / "desktop.png"), full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth + 2")
        page.screenshot(path=str(ARTIFACTS / "mobile.png"), full_page=True)
        results.append("mobile layout")

        malformed = copy.deepcopy(REPORT)
        malformed["columns"].append(copy.deepcopy(malformed["columns"][0]))
        upload(page, "#inspection-file", malformed)
        expect(page.locator("#inspection-error")).to_be_visible()
        expect(page.locator("#download-spec")).to_be_disabled()
        results.append("malformed import disables output")
        assert errors == [], errors
        assert network == [], network
        result = {"passed": results, "browser_errors": errors, "network_requests": network}
        (ARTIFACTS / "results.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
    finally:
        page.close()
