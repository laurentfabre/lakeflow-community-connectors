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
) -> dict:
    """Read one metadata report through runtime UC token injection.

    ``discover`` lists dataset IDs; ``inspect`` previews physical fields;
    ``health`` checks options, metadata, schema policy and extraction capacity.
    ``check_count=True`` additionally executes one count query, never row samples.
    Metadata access does not prove source row visibility or a complete snapshot.
    """
    if not isinstance(connection_name, str) or not connection_name.strip():
        raise CrmAnalyticsError("A nonempty UC connection name is required")
    if action not in ("health", "discover", "inspect"):
        raise CrmAnalyticsError("action must be health, discover, or inspect")
    if type(check_count) is not bool:
        raise CrmAnalyticsError("check_count must be a boolean")
    if table_options is not None and not isinstance(table_options, dict):
        raise CrmAnalyticsError("table_options must be an object")
    options = {
        "diagnostic_action": action,
        "diagnostic_table_options": json.dumps(table_options or {}, allow_nan=False),
        "diagnostic_check_count": str(check_count).lower(),
    }
    if dataset_id is not None:
        options["diagnostic_dataset_id"] = dataset_id
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
                    "action": "Verify the updated reader is registered, diagnostics options are allowlisted, USE CONNECTION is granted, and Salesforce authorization is valid",
                }
            ],
        }
    if len(rows) != 1:
        raise CrmAnalyticsError("Expected one connector diagnostic report")
    try:
        result = json.loads(rows[0]["report"])
        if not isinstance(result, dict) or result.get("format_version") != 1:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError):
        raise CrmAnalyticsError("Malformed connector diagnostic report") from None


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
