"""Notebook helpers using the registered Spark source and existing UC grant.

Register the deployed connector once with ``register(spark, 'crm_analytics')``.
These helpers never obtain, display or persist the connection's credentials.
"""

import json

from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_operations import (
    DIAGNOSTICS_TABLE,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_support import (
    CrmAnalyticsError,
)


def run_diagnostics(
    spark,
    *,
    connection_name: str,
    action: str = "health",
    dataset_id: str | None = None,
    table_options: dict | None = None,
    check_count: bool = False,
    retry_authentication: bool = True,
) -> dict:
    """Read a metadata report, with at most one fresh UC read after HTTP 401.

    ``discover`` lists dataset IDs; ``inspect`` previews physical fields;
    ``health`` checks options, metadata, schema policy and extraction capacity.
    ``check_count=True`` additionally executes one count query, never row samples.
    Metadata access does not prove source row visibility or a complete snapshot.
    A second Spark read requests credential injection from UC. It does not
    exchange tokens itself or guarantee that UC can renew revoked credentials.
    """
    if not isinstance(connection_name, str) or not connection_name.strip():
        raise CrmAnalyticsError("A nonempty UC connection name is required")
    if action not in ("health", "discover", "inspect"):
        raise CrmAnalyticsError("action must be health, discover, or inspect")
    if not isinstance(check_count, bool):
        raise CrmAnalyticsError("check_count must be a boolean")
    if not isinstance(retry_authentication, bool):
        raise CrmAnalyticsError("retry_authentication must be a boolean")
    if table_options is not None and not isinstance(table_options, dict):
        raise CrmAnalyticsError("table_options must be an object")
    options = {
        "diagnostic_action": action,
        "diagnostic_table_options": json.dumps(table_options or {}, allow_nan=False),
        "diagnostic_check_count": str(check_count).lower(),
    }
    if dataset_id is not None:
        options["diagnostic_dataset_id"] = dataset_id
    previous_authentication_failure = False
    for attempt in range(2 if retry_authentication else 1):
        result = _diagnostic_read(spark, connection_name, options)
        authentication_failure = result.get("code") == "AUTHENTICATION" or any(
            check.get("name") == "AUTHENTICATION" and check.get("status") == "error"
            for check in result.get("checks", [])
            if isinstance(check, dict)
        )
        lifecycle = dict(result.get("oauth_lifecycle", {}))
        verified = lifecycle.get("source_access_verified", False)
        lifecycle.update(
            managed_by="unity_catalog",
            fresh_read_attempts=attempt + 1,
            credential_reinjections=attempt,
            recovered=bool(
                previous_authentication_failure and verified and not authentication_failure
            ),
            reauthorization_required=bool(
                previous_authentication_failure and authentication_failure
            ),
        )
        result["oauth_lifecycle"] = lifecycle
        if not authentication_failure or attempt or not retry_authentication:
            return result
        previous_authentication_failure = True
    raise AssertionError("unreachable")


def _diagnostic_read(spark, connection_name, options):
    try:
        rows = (
            spark.read.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", DIAGNOSTICS_TABLE)
            .options(**options)
            .load()
            .collect()
        )
    except Exception:
        # UC injection and Python source initialization can fail before a
        # connector report exists. Do not print a platform exception containing
        # serialized options or ask users to export tokens to diagnose it.
        return {
            "format_version": 1,
            "kind": "crm_analytics_execution_error",
            "ready": False,
            "checks": [
                {
                    "name": "uc_execution",
                    "status": "error",
                    "message": "The UC-backed diagnostic read could not execute",
                    "action": (
                        "Verify the updated reader is registered, diagnostics options are "
                        "allowlisted, USE CONNECTION is granted, and Salesforce authorization is valid"
                    ),
                }
            ],
        }
    if len(rows) != 1:
        raise CrmAnalyticsError("Expected one connector diagnostic report")
    try:
        result = json.loads(rows[0]["report"])
        if not isinstance(result, dict) or result.get("format_version") != 1:
            raise ValueError
        if "checks" in result and (
            not isinstance(result["checks"], list)
            or any(not isinstance(check, dict) for check in result["checks"])
        ):
            raise ValueError
        if "oauth_lifecycle" in result and not isinstance(result["oauth_lifecycle"], dict):
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError):
        raise CrmAnalyticsError("Malformed connector diagnostic report") from None


def ensure_connection(
    spark,
    *,
    connection_name: str,
    dataset_id: str | None = None,
    table_options: dict | None = None,
    check_count: bool = False,
) -> dict:
    """Pre-ingestion gate using metadata only, or an optional count query.

    A persistent 401 requires the existing connection's owner to reauthorize
    using the same Salesforce integration user. No target/checkpoint changes or
    source rows are written or downloaded by this helper.
    """
    report = run_diagnostics(
        spark,
        connection_name=connection_name,
        action="health",
        dataset_id=dataset_id,
        table_options=table_options,
        check_count=check_count,
        retry_authentication=True,
    )
    if report.get("ready") is True:
        return report
    if report.get("oauth_lifecycle", {}).get("reauthorization_required"):
        raise CrmAnalyticsError(
            "Salesforce authentication still fails after a fresh UC-backed read. The owner must "
            "reauthorize the existing connection as the same Salesforce integration user; verify "
            "api and refresh_token scopes and the Salesforce refresh-token policy, then retry. "
            "Keep tokens out of pipeline settings and preserve existing tables/checkpoints",
            code="AUTHENTICATION",
        )
    raise CrmAnalyticsError(
        "CRM Analytics preflight failed; review run_diagnostics checks for connection permissions, "
        "configuration, schema or capacity before starting ingestion",
        code="PREFLIGHT_FAILED",
    )


def validate_pipeline(spark, pipeline_spec: dict, *, check_count: bool = False) -> dict:
    """Validate every configured dataset without deploying or changing tables."""
    if not isinstance(pipeline_spec, dict) or not isinstance(pipeline_spec.get("objects"), list):
        raise CrmAnalyticsError("A workspace pipeline spec with objects is required")
    connection = pipeline_spec.get("connection_name")
    if not isinstance(connection, str) or not connection.strip() or not pipeline_spec["objects"]:
        raise CrmAnalyticsError("A connection and at least one table are required")
    reports = []
    for entry in pipeline_spec["objects"]:
        if (
            not isinstance(entry, dict)
            or set(entry) != {"table"}
            or not isinstance(entry["table"], dict)
        ):
            raise CrmAnalyticsError("Each pipeline object must contain one table")
        table = entry["table"]
        if not table.get("source_table"):
            raise CrmAnalyticsError("Each table requires source_table")
        reports.append(
            run_diagnostics(
                spark,
                connection_name=connection,
                action="health",
                dataset_id=table["source_table"],
                table_options=table.get("table_configuration", {}),
                check_count=check_count,
            )
        )
    return {"ready": all(r.get("ready") is True for r in reports), "tables": reports}
