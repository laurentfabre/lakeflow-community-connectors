"""Optional durable, metadata-only extraction events and a separate collector.

An extraction completion is not a target commit. Files are immutable events,
not a checkpoint or source of truth for data publication. A killed process can
leave a started-only attempt; monitoring must flag such attempts as incomplete.
"""

import json
import os
import re
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql.functions import expr
from pyspark.sql.types import StructType

from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_support import (
    CrmAnalyticsError,
)

MONITORING_STATUSES = frozenset({"started", "completed", "failed", "cancelled"})
MONITORING_SCHEMA_DDL = """
format_version BIGINT, event_id STRING, attempt_id STRING, status STRING,
event_time TIMESTAMP, started_at TIMESTAMP, dataset_id STRING, version_id STRING,
source_created_at TIMESTAMP, source_age_seconds DOUBLE, elapsed_seconds DOUBLE,
expected_row_count BIGINT, extracted_row_count BIGINT, page_queries BIGINT,
data_pages BIGINT, peak_page_rows BIGINT, http_requests BIGINT, query_requests BIGINT,
retry_count BIGINT, retry_wait_seconds DOUBLE, error_code STRING, target_committed BOOLEAN
"""
_MONITORING_CODES = frozenset(
    {
        "AUTHENTICATION",
        "PERMISSION_DENIED",
        "RESOURCE_NOT_FOUND",
        "RATE_LIMITED",
        "SOURCE_HTTP_ERROR",
        "SOURCE_UNAVAILABLE",
        "CONNECTOR_ERROR",
        "READ_DEADLINE",
        "QUERY_BUDGET",
        "MONITORING_WRITE_FAILED",
        "CANCELLED",
        "INTERNAL_ERROR",
    }
)
_VOLUME_SEGMENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,254}\Z")
_MONITORING_ID = re.compile(r"[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?\Z")


def validate_monitoring_path(value, *, option_name="monitoring_volume_path") -> str:
    """Accept only a literal UC Volume path with simple, non-traversing parts."""
    if not isinstance(value, str) or not value.startswith("/Volumes/"):
        raise CrmAnalyticsError(
            f"{option_name} must be a literal /Volumes/catalog/schema/volume path"
        )
    parts = value.removesuffix("/").split("/")
    if len(parts) < 5 or any(not _VOLUME_SEGMENT.fullmatch(p) for p in parts[2:]):
        raise CrmAnalyticsError(f"{option_name} contains an unsupported or unsafe Volume path")
    return "/".join(parts)


def monitoring_schema() -> StructType:
    """Explicit event schema, usable without an active Spark session."""
    fields = []
    for item in MONITORING_SCHEMA_DDL.strip().split(","):
        name, kind = item.strip().split()
        fields.append(
            {
                "name": name,
                "type": {
                    "BIGINT": "long",
                    "STRING": "string",
                    "TIMESTAMP": "timestamp",
                    "DOUBLE": "double",
                    "BOOLEAN": "boolean",
                }[kind],
                "nullable": True,
                "metadata": {},
            }
        )
    return StructType.fromJson({"type": "struct", "fields": fields})


def collect_monitoring_events(
    spark,
    *,
    monitoring_volume_path: str,
    monitoring_table: str,
    checkpoint_path: str,
):
    """Collect events with Auto Loader in a separate available-now job.

    Keep the same dedicated checkpoint for repeated runs. JSON failures abort
    collection rather than silently dropping events. Grant the job READ VOLUME
    on events, READ/WRITE VOLUME on its checkpoint and writes to its Delta table.
    """
    events = validate_monitoring_path(monitoring_volume_path)
    checkpoint = validate_monitoring_path(checkpoint_path, option_name="checkpoint_path")
    # The source and checkpoint must be disjoint, including ancestor paths.
    if (
        events == checkpoint
        or events.startswith(checkpoint + "/")
        or checkpoint.startswith(events + "/")
    ):
        raise CrmAnalyticsError("Monitoring events and collector checkpoint paths must be disjoint")
    if (
        not isinstance(monitoring_table, str)
        or len(monitoring_table.split(".")) != 3
        or any(
            not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,254}", part)
            for part in monitoring_table.split(".")
        )
    ):
        raise CrmAnalyticsError(
            "monitoring_table must be a three-part catalog.schema.table identifier"
        )
    quoted_table = ".".join("`" + part + "`" for part in monitoring_table.split("."))
    events_frame = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .option("rescuedDataColumn", "_rescued_data")
        .option("mode", "FAILFAST")
        .schema(monitoring_schema().add("_rescued_data", "string"))
        .load(events)
    )
    valid_event = """
        _rescued_data IS NULL AND format_version = 1
        AND event_id RLIKE '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        AND attempt_id RLIKE '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        AND status IN ('started', 'completed', 'failed', 'cancelled')
        AND event_time IS NOT NULL AND started_at IS NOT NULL
        AND event_time >= started_at AND target_committed = false
        AND elapsed_seconds >= 0 AND extracted_row_count >= 0
        AND page_queries >= 0 AND data_pages >= 0 AND peak_page_rows >= 0
        AND http_requests >= 0 AND query_requests >= 0
        AND retry_count >= 0 AND retry_wait_seconds >= 0
        AND (expected_row_count IS NULL OR expected_row_count >= 0)
        AND (source_age_seconds IS NULL OR source_age_seconds >= 0)
        AND (dataset_id IS NULL OR dataset_id RLIKE '^[A-Za-z0-9]{15}([A-Za-z0-9]{3})?$')
        AND (version_id IS NULL OR version_id RLIKE '^[A-Za-z0-9]{15}([A-Za-z0-9]{3})?$')
        AND (status != 'completed' OR
             (dataset_id IS NOT NULL AND version_id IS NOT NULL
              AND source_created_at IS NOT NULL AND expected_row_count = extracted_row_count
              AND page_queries >= 1 AND data_pages = page_queries - 1))
        AND ((status IN ('started', 'completed') AND error_code IS NULL)
             OR (status = 'cancelled' AND error_code = 'CANCELLED')
             OR (status = 'failed' AND error_code IN
                  ('AUTHENTICATION', 'PERMISSION_DENIED', 'RESOURCE_NOT_FOUND', 'RATE_LIMITED',
                   'SOURCE_HTTP_ERROR', 'SOURCE_UNAVAILABLE', 'CONNECTOR_ERROR', 'READ_DEADLINE',
                   'QUERY_BUDGET', 'MONITORING_WRITE_FAILED', 'CANCELLED', 'INTERNAL_ERROR')))
    """
    query = (
        events_frame.withColumn(
            "event_id",
            expr(
                "CASE WHEN " + valid_event + " THEN event_id "
                "ELSE raise_error('Invalid CRM Analytics monitoring event contract') END"
            ),
        )
        .drop("_rescued_data")
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(quoted_table)
    )
    query.awaitTermination()
    return query


class _MonitoringAttempt:
    """Per-iterator state; never store values or exceptions from source rows."""

    def __init__(self, path, dataset_id, started_at):
        self.path = validate_monitoring_path(path) if path is not None else None
        self.attempt_id = str(uuid.uuid4())
        self.started_at = started_at
        self.started_time = datetime.now(timezone.utc).isoformat()
        self.dataset_id = (
            dataset_id
            if isinstance(dataset_id, str) and _MONITORING_ID.fullmatch(dataset_id)
            else None
        )
        self.descriptor = {}
        self.budget = None
        self.context_key = None
        self.read_epoch = None
        self.extracted_row_count = 0
        self.page_queries = 0
        self.data_pages = 0
        self.peak_page_rows = 0
        self.emit("started")

    def emit(self, status, *, error=None):
        if self.path is None:
            return
        if status not in MONITORING_STATUSES:
            raise AssertionError("Unknown monitoring lifecycle status")
        now = datetime.now(timezone.utc)
        descriptor = self.descriptor
        source_created_at = descriptor.get("source_created_at")
        try:
            published = datetime.fromisoformat(source_created_at.replace("Z", "+00:00"))
            if published.tzinfo is None:
                raise ValueError
            source_age = max(0.0, (now - published).total_seconds())
        except (AttributeError, TypeError, ValueError):
            source_created_at, source_age = None, None
        budget = self.budget
        code = getattr(error, "code", "INTERNAL_ERROR") if error is not None else None
        if code is not None and code not in _MONITORING_CODES:
            code = "CONNECTOR_ERROR"
        event_id = str(uuid.uuid4())
        event = {
            "format_version": 1,
            "event_id": event_id,
            "attempt_id": self.attempt_id,
            "status": status,
            "event_time": now.isoformat(),
            "started_at": self.started_time,
            "dataset_id": self.dataset_id,
            "version_id": descriptor.get("version_id"),
            "source_created_at": source_created_at,
            "source_age_seconds": source_age,
            "elapsed_seconds": max(0.0, time.monotonic() - self.started_at),
            "expected_row_count": descriptor.get("expected_row_count"),
            "extracted_row_count": self.extracted_row_count,
            "page_queries": self.page_queries,
            "data_pages": self.data_pages,
            "peak_page_rows": self.peak_page_rows,
            "http_requests": budget.http_requests if budget else 0,
            "query_requests": budget.query_requests if budget else 0,
            "retry_count": budget.retry_count if budget else 0,
            "retry_wait_seconds": budget.retry_wait_seconds if budget else 0.0,
            "error_code": "CANCELLED" if status == "cancelled" else code,
            "target_committed": False,
        }
        try:
            os.makedirs(self.path, exist_ok=True)
            # Volume storage does not guarantee atomic rename. Unique immutable
            # files plus fail-fast parsing expose interrupted writes explicitly.
            with open(os.path.join(self.path, event_id + ".json"), "x", encoding="utf-8") as handle:
                json.dump(event, handle, ensure_ascii=True, allow_nan=False)
                handle.write("\n")
        except (OSError, TypeError, ValueError):
            raise CrmAnalyticsError(
                "Could not persist CRM Analytics monitoring event; "
                "check Volume permissions and storage; snapshot aborted",
                code="MONITORING_WRITE_FAILED",
            ) from None

    def record_page(self, rows):
        self.page_queries += 1
        self.data_pages += int(bool(rows))
        self.peak_page_rows = max(self.peak_page_rows, len(rows))

    def record_failure(self, error):
        try:
            self.emit("failed", error=error)
        except CrmAnalyticsError:
            # The original source failure must remain the cause of the failed
            # extraction. A monitoring write must never turn it into success.
            pass


class _ObservedSnapshotIterator:
    """Keep explicit close observable, even before the first generator next()."""

    def __init__(self, iterator, attempt, invalidate):
        self.iterator, self.attempt, self.invalidate = iterator, attempt, invalidate
        self.finished = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.finished:
            raise StopIteration
        try:
            row = next(self.iterator)
        except StopIteration:
            try:
                if self.attempt.budget is not None:
                    self.attempt.budget.remaining()
                self.attempt.emit("completed")
                if self.attempt.budget is not None:
                    self.attempt.budget.remaining()
            except BaseException as error:
                self.invalidate()
                self.attempt.record_failure(error)
                self.finished = True
                raise
            self.finished = True
            raise
        except BaseException as error:
            self.invalidate()
            self.attempt.record_failure(error)
            self.finished = True
            raise
        self.attempt.extracted_row_count += 1
        return row

    def close(self):
        if self.finished:
            return
        self.invalidate()
        try:
            self.iterator.close()
            self.attempt.emit("cancelled")
        except BaseException as error:
            self.attempt.record_failure(error)
            raise
        finally:
            self.finished = True
