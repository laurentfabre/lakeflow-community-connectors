"""Read-only health, metadata inspection and migration planning.

Reports contain configuration and schema metadata, never access tokens, raw
HTTP bodies, source row samples, or OAuth secrets. A health report is not proof
that a snapshot completed or that the integration identity has full visibility.
"""

import json
import re
import time
from datetime import datetime, timezone

from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_performance import (
    extraction_plan,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_schema import (
    compare_schema,
    schema_baseline,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_support import (
    CrmAnalyticsError,
    _ReadBudget,
    _field_name,
)

DIAGNOSTICS_TABLE = "_crm_analytics_diagnostics"
_DIAGNOSTIC_OPTIONS = {
    "diagnostic_action",
    "diagnostic_dataset_id",
    "diagnostic_table_options",
    "diagnostic_check_count",
}


def _report_check(name, status, message, action=None):
    return {
        "name": name,
        "status": status,
        "message": message,
        **({"action": action} if action else {}),
    }


def _parse_expiration(value):
    try:
        if isinstance(value, (float, int)) or str(value).replace(".", "", 1).isdigit():
            timestamp = float(value)
            return datetime.fromtimestamp(
                timestamp / 1000 if timestamp > 10**11 else timestamp, timezone.utc
            )
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else None
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def connection_checks(connection_metadata: dict | None, table_options: dict | None = None) -> list:
    """Inspect only selected public UC metadata; never return the metadata map."""
    metadata = connection_metadata or {}
    options = metadata.get("options", metadata)
    if not isinstance(options, dict):
        raise CrmAnalyticsError("Connection metadata options must be an object")
    options = {str(k).lower(): v for k, v in options.items()}
    settings = table_options or {}
    required = {
        k.lower()
        for k in settings
        if k not in {"skip_unchanged_versions", "scd_type", "cluster_by", "sequence_by"}
    }
    if str(settings.get("skip_unchanged_versions", "false")).lower() == "true":
        required.add("expected_snapshot_fingerprint")
    checks = []
    allowlist = options.get("externaloptionsallowlist")
    if allowlist is None:
        checks.append(
            _report_check(
                "connection_allowlist",
                "warning",
                "UC allowlist metadata was not supplied",
                "Verify the connection allowlist against the connector spec",
            )
        )
    elif not isinstance(allowlist, str):
        checks.append(
            _report_check("connection_allowlist", "error", "UC allowlist metadata is malformed")
        )
    else:
        missing = sorted(required - {name.strip().lower() for name in allowlist.split(",")})
        checks.append(
            _report_check(
                "connection_allowlist",
                "error" if missing else "ok",
                (
                    "Missing allowed options: " + ", ".join(missing)
                    if missing
                    else "Requested source options are allowed"
                ),
                (
                    "Update the existing connection using the current spec; OAuth reauthorization may be required"
                    if missing
                    else None
                ),
            )
        )
    now = datetime.now(timezone.utc)
    for name in ("access_token_expiration", "refresh_token_expiration"):
        if name not in options:
            continue
        expiry = _parse_expiration(options[name])
        if expiry is None:
            checks.append(
                _report_check(name, "warning", "Token expiration metadata could not be interpreted")
            )
        elif expiry <= now:
            checks.append(
                _report_check(
                    name,
                    "warning",
                    "Stored expiration has passed",
                    "Check connection renewal; the source probe determines whether the injected token works",
                )
            )
        else:
            checks.append(_report_check(name, "ok", "Stored expiration is in the future"))
    return checks


def inspect_dataset(source, dataset_id: str, *, column_name_mode: str = "readable") -> dict:
    """Preview XMD fields without row queries or a full-visibility attestation."""
    if not isinstance(dataset_id, str) or not re.fullmatch(
        r"[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?", dataset_id
    ):
        raise CrmAnalyticsError("dataset_id must be a 15- or 18-character Salesforce ID")
    if column_name_mode not in ("readable", "legacy_hex"):
        raise CrmAnalyticsError("column_name_mode must be readable or legacy_hex")
    dataset = source._dataset(dataset_id)
    version_id = dataset.get("currentVersionId")
    if not isinstance(version_id, str) or not re.fullmatch(
        r"[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?", version_id
    ):
        raise CrmAnalyticsError("Dataset has no valid current version")
    version = source._version(dataset_id, version_id)
    xmd = version.get("xmdMain")
    if not isinstance(xmd, dict) or xmd.get("errorMessage"):
        raise CrmAnalyticsError("Dataset version has no usable main XMD")
    candidates = set()
    labels = {}
    for group in ("dimensions", "measures", "derivedDimensions", "derivedMeasures"):
        entries = xmd.get(group, [])
        if not isinstance(entries, list):
            raise CrmAnalyticsError("Malformed main XMD columns")
        for entry in entries:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("field"), str)
                or not entry["field"]
            ):
                raise CrmAnalyticsError("Malformed main XMD field identity")
            candidates.add(entry["field"])
            if isinstance(entry.get("label"), str):
                labels[entry["field"]] = entry["label"][:256]
    for name, _, _ in source._date_columns(xmd.get("dates", []), True):
        candidates.add(name)
    if not candidates:
        raise CrmAnalyticsError("Main XMD has no fields to inspect")
    columns = []
    for name in sorted(candidates):
        item = {
            "source_name": name,
            "label": labels.get(name, name),
            "supported": False,
            "destination_name": None,
            "type": None,
        }
        try:
            _field_name(name)
            schema, mapping, _ = source._schema(
                version,
                {
                    "selected_columns": [name],
                    "source_primary_keys": [],
                    "column_name_mode": column_name_mode,
                    "decimal_precision": 38,
                    "decimal_scale": 18,
                },
            )
            item.update(
                supported=True,
                destination_name=mapping[name],
                type=schema[0].dataType.simpleString(),
            )
        except (CrmAnalyticsError, ValueError):
            item["reason"] = (
                "Unsupported physical field, identifier, or XMD type; exclude it or correct source metadata"
            )
        columns.append(item)
    destinations = {}
    for item in columns:
        if item["supported"]:
            destinations.setdefault(item["destination_name"].casefold(), []).append(item)
    for matches in destinations.values():
        if len(matches) > 1:
            for item in matches:
                item.update(
                    supported=False,
                    reason="Case-insensitive destination collision; rename the source or inspect using legacy_hex",
                )
    return {
        "format_version": 1,
        "kind": "crm_analytics_inspection",
        "dataset_id": dataset_id,
        "version_id": version_id,
        "column_name_mode": column_name_mode,
        "decimal_precision": 38,
        "decimal_scale": 18,
        "columns": columns,
        "key_suggestions": [
            c["source_name"]
            for c in columns
            if c["supported"]
            and c["type"] == "string"
            and (
                c["source_name"].lower().endswith("id")
                or c["source_name"].lower().startswith("id_")
            )
        ],
        "key_suggestions_verified": False,
        "row_values_checked": False,
        "visibility_verified": False,
    }


def preflight(
    source,
    dataset_id: str | None = None,
    table_options: dict | None = None,
    *,
    connection_metadata: dict | None = None,
    check_count: bool = False,
) -> dict:
    """Probe permissions and plan an extraction without downloading source rows."""
    settings = dict(table_options or {})
    checks = connection_checks(connection_metadata, settings)
    skip_unchanged = settings.pop("skip_unchanged_versions", False)
    del skip_unchanged  # Framework control, not a source option.
    for control in ("scd_type", "sequence_by", "cluster_by"):
        settings.pop(control, None)
    report = {
        "format_version": 1,
        "kind": "crm_analytics_preflight",
        "checks": checks,
        "snapshot_completed": False,
    }
    try:
        if dataset_id is None:
            names = source.list_tables()
            report["datasets"] = names
            checks.append(_report_check("source_access", "ok", "Dataset discovery succeeded"))
        else:
            normalized = source._table_options(settings)
            # Report incompatible baselines instead of rejecting before a report can be built.
            probe_settings = {
                k: v
                for k, v in settings.items()
                if k not in {"schema_baseline", "schema_evolution_mode"}
            }
            descriptor = source.pin_table(dataset_id, probe_settings)
            checks.append(
                _report_check(
                    "source_access", "ok", "Dataset and pinned version metadata are accessible"
                )
            )
            plan = extraction_plan(descriptor, normalized, source._page_size)
            report.update(
                dataset_id=dataset_id,
                version_id=descriptor["version_id"],
                extraction_plan=plan,
                schema_baseline=schema_baseline(descriptor),
            )
            for key, label in (
                ("within_query_budget", "query_budget"),
                ("within_key_capacity", "key_capacity"),
            ):
                checks.append(
                    _report_check(
                        label,
                        "ok" if plan[key] else "error",
                        (
                            "Within configured capacity"
                            if plan[key]
                            else "Snapshot exceeds configured capacity; review the extraction plan"
                        ),
                    )
                )
            if "schema_baseline" in normalized:
                migration = compare_schema(
                    normalized["schema_baseline"], descriptor, normalized["schema_evolution_mode"]
                )
                report["schema_migration"] = migration
                checks.append(
                    _report_check(
                        "schema_evolution",
                        "ok" if migration["compatible"] else "error",
                        migration["action"],
                    )
                )
            else:
                checks.append(
                    _report_check(
                        "schema_evolution",
                        "warning",
                        "No approved schema baseline is configured",
                        "Review and save the candidate schema_baseline before enabling a schema policy",
                    )
                )
            if check_count:
                budget = _ReadBudget(time.monotonic(), normalized)
                source._count(
                    f'q = load "{dataset_id}/{descriptor["version_id"]}";',
                    descriptor["expected_row_count"],
                    budget,
                )
                checks.append(
                    _report_check(
                        "visible_count",
                        "ok",
                        "Visible count matches pinned metadata; this does not independently establish full visibility",
                    )
                )
    except CrmAnalyticsError as error:
        actions = {
            "AUTHENTICATION": "Reauthorize the existing UC connection as the same Salesforce integration user; do not put tokens in pipeline settings",
            "PERMISSION_DENIED": "Check CRM Analytics licensing, app access and dataset/folder permissions for the integration user",
            "RESOURCE_NOT_FOUND": "Check the dataset/version ID and its visibility to the integration user",
            "RATE_LIMITED": "Retry later and review Salesforce query quotas and extraction schedules",
        }
        code = error.code
        checks.append(
            _report_check(
                code,
                "error",
                str(error),
                actions.get(code, "Review the source configuration and connector guide"),
            )
        )
    report["ready"] = not any(c["status"] == "error" for c in checks)
    report["warnings"] = sum(c["status"] == "warning" for c in checks)
    return report


def diagnostic_report(source, options: dict) -> dict:
    """Dispatch a Spark report read using UC-injected credentials on the worker."""
    settings = {str(k).lower(): v for k, v in options.items()}
    action = settings.get("diagnostic_action", "health")
    if action not in ("health", "discover", "inspect"):
        raise CrmAnalyticsError("diagnostic_action must be health, discover, or inspect")
    raw = settings.get("diagnostic_table_options", "{}")
    try:
        table_options = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(table_options, dict):
            raise ValueError
    except (ValueError, TypeError):
        raise CrmAnalyticsError("diagnostic_table_options must be a JSON object") from None
    count = str(settings.get("diagnostic_check_count", "false")).lower()
    if count not in ("true", "false"):
        raise CrmAnalyticsError("diagnostic_check_count must be true or false")
    dataset_id = settings.get("diagnostic_dataset_id")
    if action == "inspect":
        if not dataset_id:
            raise CrmAnalyticsError("diagnostic_dataset_id is required for field inspection")
        try:
            return inspect_dataset(
                source,
                dataset_id,
                column_name_mode=table_options.get("column_name_mode", "readable"),
            )
        except CrmAnalyticsError as error:
            return {
                "format_version": 1,
                "kind": "crm_analytics_inspection_error",
                "ready": False,
                "code": error.code,
                "message": str(error),
            }
    if action == "discover":
        dataset_id, table_options = None, {}
    return preflight(
        source,
        dataset_id,
        table_options,
        connection_metadata=source.options,
        check_count=count == "true",
    )
