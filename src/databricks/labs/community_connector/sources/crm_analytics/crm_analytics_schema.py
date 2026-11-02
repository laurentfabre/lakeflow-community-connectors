"""Explicit cross-capture schema contracts; never mutate a target or checkpoint."""

import json
import re

from pyspark.sql.types import StructType

from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_support import (
    CrmAnalyticsError,
)

_CONTRACT_IDENTITY = (
    "dataset_id",
    "primary_keys",
    "source_primary_keys",
    "column_name_mode",
    "column_mapping",
    "scope_fingerprint",
    "security_fingerprint",
    "auto_increment_column",
)


def schema_baseline(descriptor: dict) -> dict:
    """Create a JSON-safe, version-independent baseline from an approved pin.

    Approval belongs to the operator. Metadata inspection alone does not prove
    source uniqueness, full visibility, or target compatibility.
    """
    result = {
        "format_version": 1,
        "schema_json": descriptor["schema_json"],
        **{name: descriptor.get(name) for name in _CONTRACT_IDENTITY},
    }
    return validate_schema_baseline(result)


def validate_schema_baseline(value) -> dict:
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 1024 * 1024:
            raise CrmAnalyticsError("schema_baseline exceeds the 1 MiB configuration limit")
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            raise CrmAnalyticsError("schema_baseline must be a baseline JSON object") from None
    required = {"format_version", "schema_json", *_CONTRACT_IDENTITY}
    try:
        if not isinstance(value, dict) or set(value) != required:
            raise ValueError
        if type(value["format_version"]) is not int or value["format_version"] != 1:
            raise ValueError
        if not re.fullmatch(r"[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?", value["dataset_id"]):
            raise ValueError
        if (
            not isinstance(value["scope_fingerprint"], str)
            or not value["scope_fingerprint"].strip()
        ):
            raise ValueError
        if not re.fullmatch(r"[0-9a-f]{64}", value["security_fingerprint"]):
            raise ValueError
        if value["column_name_mode"] not in ("readable", "legacy_hex"):
            raise ValueError
        for key in ("primary_keys", "source_primary_keys"):
            names = value[key]
            if not isinstance(names, list) or any(not isinstance(n, str) or not n for n in names):
                raise ValueError
            if len(set(names)) != len(names):
                raise ValueError
        if not value["primary_keys"]:
            raise ValueError
        mapping = value["column_mapping"]
        if not isinstance(mapping, dict) or not mapping:
            raise ValueError
        if any(
            not isinstance(k, str) or not k or not isinstance(v, str) or not v
            for k, v in mapping.items()
        ):
            raise ValueError
        raw_schema = value["schema_json"]
        if (
            not isinstance(raw_schema, dict)
            or set(raw_schema) != {"type", "fields"}
            or raw_schema["type"] != "struct"
            or not isinstance(raw_schema["fields"], list)
        ):
            raise ValueError
        for field in raw_schema["fields"]:
            if (
                not isinstance(field, dict)
                or set(field) != {"name", "type", "nullable", "metadata"}
                or not isinstance(field["name"], str)
                or not field["name"]
                or type(field["nullable"]) is not bool
                or not isinstance(field["metadata"], dict)
                or not isinstance(field["type"], str)
            ):
                raise ValueError
            scalar = field["type"]
            if scalar not in ("string", "long"):
                decimal = re.fullmatch(r"decimal\(([0-9]{1,2}),([0-9]{1,2})\)", scalar)
                if (
                    not decimal
                    or not 1 <= int(decimal[1]) <= 38
                    or not 0 <= int(decimal[2]) <= int(decimal[1])
                ):
                    raise ValueError
        schema = StructType.fromJson(value["schema_json"])
        names = schema.fieldNames()
        if not names or len({n.casefold() for n in names}) != len(names):
            raise ValueError
        if not set(value["primary_keys"]).issubset(names):
            raise ValueError
        if not set(value["source_primary_keys"]).issubset(mapping):
            raise ValueError
        if not set(mapping.values()).issubset(names):
            raise ValueError
        auto = value["auto_increment_column"]
        if auto is not None and (
            not isinstance(auto, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", auto)
        ):
            raise ValueError
        # A detached copy also rejects non-JSON configuration objects.
        return json.loads(json.dumps(value, allow_nan=False))
    except (ValueError, TypeError, KeyError, AttributeError, AssertionError):
        raise CrmAnalyticsError(
            "Malformed schema_baseline; export an approved schema baseline"
        ) from None


def compare_schema(baseline, descriptor: dict, mode: str = "strict") -> dict:
    """Allow only unchanged contracts, or nullable additions in additive mode.

    Type, nullability, source mapping, key and security changes remain breaking.
    Column order is not identity. New fields are never silently dropped.
    """
    if mode not in ("strict", "additive"):
        raise CrmAnalyticsError("schema_evolution_mode must be strict or additive")
    previous = validate_schema_baseline(baseline)
    current = schema_baseline(descriptor)
    old_fields = {f["name"]: f for f in previous["schema_json"]["fields"]}
    new_fields = {f["name"]: f for f in current["schema_json"]["fields"]}
    added = sorted(new_fields.keys() - old_fields.keys())
    removed = sorted(old_fields.keys() - new_fields.keys())
    changed = []
    for name in sorted(old_fields.keys() & new_fields.keys()):
        old, new = old_fields[name], new_fields[name]
        attributes = [k for k in ("type", "nullable", "metadata") if old.get(k) != new.get(k)]
        if attributes:
            changed.append({"column": name, "attributes": attributes})
    identity_changes = [
        key
        for key in _CONTRACT_IDENTITY
        if key != "column_mapping" and previous[key] != current[key]
    ]
    mapping_changed = sorted(
        name
        for name, target in previous["column_mapping"].items()
        if current["column_mapping"].get(name) != target
    )
    unsafe_additions = [
        name
        for name in added
        if new_fields[name].get("nullable") is not True
        or "crm_analytics_generated" in new_fields[name].get("metadata", {})
    ]
    compatible = not (removed or changed or identity_changes or mapping_changed or unsafe_additions)
    if mode == "strict" and added:
        compatible = False
    return {
        "mode": mode,
        "compatible": compatible,
        "added_columns": added,
        "removed_columns": removed,
        "changed_columns": changed,
        "mapping_changes": mapping_changed,
        "identity_changes": identity_changes,
        "requires_target_schema_review": bool(added),
        "action": (
            "Review target schema support before accepting nullable additions"
            if compatible and added
            else (
                "No schema migration required"
                if compatible
                else "Review the migration report and approve a new baseline or destination; no target was changed"
            )
        ),
    }


def enforce_schema_policy(descriptor: dict, options: dict) -> dict | None:
    if "schema_baseline" not in options:
        return None
    report = compare_schema(
        options["schema_baseline"], descriptor, options.get("schema_evolution_mode", "strict")
    )
    if not report["compatible"]:
        raise CrmAnalyticsError(
            "Schema evolution policy rejected the snapshot; run preflight for a migration report",
            code="SCHEMA_MIGRATION_REQUIRED",
        )
    return report
