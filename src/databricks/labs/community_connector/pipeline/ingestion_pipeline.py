# pylint: disable=no-member
import json
from dataclasses import dataclass, field
from typing import List, Optional
from pyspark import pipelines as sdp
from pyspark.sql.functions import col, expr
from databricks.labs.community_connector.libs.spec_parser import SpecParser
from databricks.labs.community_connector.pipeline.snapshot_versions import versioned_snapshot_source


@dataclass
class SdpTableConfig:  # pylint: disable=too-many-instance-attributes
    """SDP configuration to ingest a table."""

    source_table: str
    destination_table: str
    view_name: str
    table_config: dict[str, str]
    primary_keys: List[str]
    sequence_by: str
    scd_type: str
    with_deletes: bool = False
    cluster_by: Optional[List[str]] = field(default=None)
    skip_unchanged_versions: bool = False
    snapshot_metadata: Optional[dict] = None


def _create_streaming_table(config: SdpTableConfig) -> None:
    """Create the destination streaming table, forwarding cluster_by when set."""
    kwargs: dict = {"name": config.destination_table}
    if config.cluster_by:
        kwargs["cluster_by"] = config.cluster_by
    sdp.create_streaming_table(**kwargs)


def _build_view_name(source_table: str, flow_type: str) -> str:
    """Build a unique view name encoding source, flow type, and destination."""
    return f"source_{source_table}_{flow_type}"


def _create_cdc_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    """Create CDC table using streaming and apply_changes"""

    @sdp.view(name=config.view_name)
    def v():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )

    _create_streaming_table(config)
    sdp.apply_changes(
        target=config.destination_table,
        source=config.view_name,
        keys=config.primary_keys,
        sequence_by=col(config.sequence_by),
        stored_as_scd_type=config.scd_type,
    )

    if config.with_deletes:
        delete_view_name = _build_view_name(config.source_table, "delete")

        @sdp.view(name=delete_view_name)
        def delete_view():
            return (
                spark.readStream.format("lakeflow_connect")
                .option("databricks.connection", connection_name)
                .option("tableName", config.source_table)
                .option("isDeleteFlow", "true")
                .options(**config.table_config)
                .load()
            )

        sdp.apply_changes(
            target=config.destination_table,
            source=delete_view_name,
            keys=config.primary_keys,
            sequence_by=col(config.sequence_by),
            stored_as_scd_type=config.scd_type,
            apply_as_deletes=expr("true"),
            name=delete_view_name + "_flow",
        )


def _read_snapshot(spark, connection_name: str, config: SdpTableConfig):
    """Read one full snapshot; optional version/fingerprint pin spans Spark instances."""
    options = dict(config.table_config)
    if config.skip_unchanged_versions:
        options.update(
            version_id=config.snapshot_metadata["snapshot_version"],
            expected_snapshot_fingerprint=config.snapshot_metadata["snapshot_fingerprint"],
        )
    return (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", config.source_table)
        .options(**options)
        .load()
    )


def _create_snapshot_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    """Compare full snapshots, optionally skipping committed versions via Lakeflow."""
    source = config.view_name
    if config.skip_unchanged_versions:
        source = versioned_snapshot_source(
            config.snapshot_metadata,
            {
                "connection": connection_name,
                "source_table": config.source_table,
                "destination": config.destination_table,
                "primary_keys": config.primary_keys,
                "scd_type": config.scd_type,
            },
            lambda: _read_snapshot(spark, connection_name, config),
        )
    else:

        @sdp.temporary_view(name=config.view_name)
        def snapshot_view():
            return _read_snapshot(spark, connection_name, config)

    _create_streaming_table(config)
    sdp.create_auto_cdc_from_snapshot_flow(
        target=config.destination_table,
        source=source,
        keys=config.primary_keys,
        stored_as_scd_type=config.scd_type,
    )


def _create_append_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    """Create append table using streaming without apply_changes"""

    @sdp.view(name=config.view_name)
    def v():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )

    _create_streaming_table(config)

    @sdp.append_flow(name=config.view_name + "_flow", target=config.destination_table)
    def af():
        return spark.readStream.table(config.view_name)


def _get_table_metadata(
    spark, connection_name: str, table_list: list[str], table_configs: dict[str, str]
) -> dict:
    """Get table metadata (primary_keys, cursor_field, ingestion_type etc.)"""
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_community_table_metadata")
        .option("tableNameList", json.dumps(table_list))
        .option("tableConfigs", json.dumps(table_configs))
        .load()
    )
    metadata = {}
    for row in df.collect():
        table_metadata = {}
        if row["primary_keys"] is not None:
            table_metadata["primary_keys"] = row["primary_keys"]
        if row["cursor_field"] is not None:
            table_metadata["cursor_field"] = row["cursor_field"]
        if row["ingestion_type"] is not None:
            table_metadata["ingestion_type"] = row["ingestion_type"]
        for key in ("snapshot_version", "snapshot_fingerprint"):
            try:
                value = row[key]
            except (KeyError, ValueError):
                value = None  # Older deployed readers have no version metadata.
            if value is not None:
                table_metadata[key] = value
        metadata[row["tableName"]] = table_metadata
    return metadata


def ingest(spark, pipeline_spec: dict) -> None:
    """Ingest a list of tables"""

    # parse the pipeline spec
    spec = SpecParser(pipeline_spec)
    connection_name = spec.connection_name()
    table_list = spec.get_table_list()

    # Get table_configurations for all tables. These are merged into one dict
    # keyed by table name.
    table_configs = spec.get_table_configurations()
    metadata = _get_table_metadata(spark, connection_name, table_list, table_configs)

    def _ingest_table(table: str) -> None:
        """Helper function to ingest a single table"""
        primary_keys = metadata[table].get("primary_keys")
        cursor_field = metadata[table].get("cursor_field")
        ingestion_type = metadata[table].get("ingestion_type", "cdc")
        table_config = spec.get_table_configuration(table)
        destination_table = spec.get_full_destination_table_name(table)

        # Override parameters with spec values if available
        primary_keys = spec.get_primary_keys(table) or primary_keys
        sequence_by = spec.get_sequence_by(table) or cursor_field
        cluster_by = spec.get_cluster_by(table)
        scd_type_raw = spec.get_scd_type(table)
        if scd_type_raw == "APPEND_ONLY":
            ingestion_type = "append"
        scd_type = "2" if scd_type_raw == "SCD_TYPE_2" else "1"
        skip_unchanged = spec.get_skip_unchanged_versions(table)
        if skip_unchanged and ingestion_type != "snapshot":
            raise ValueError("skip_unchanged_versions is supported only for snapshot ingestion")

        flow_type_map = {
            "cdc": "upsert",
            "cdc_with_deletes": "upsert",
            "snapshot": "snapshot",
            "append": "append",
        }
        view_name = _build_view_name(table, flow_type_map.get(ingestion_type, "upsert"))

        config = SdpTableConfig(
            source_table=table,
            destination_table=destination_table,
            view_name=view_name,
            table_config=table_config,
            primary_keys=primary_keys,
            sequence_by=sequence_by,
            scd_type=scd_type,
            with_deletes=(ingestion_type == "cdc_with_deletes"),
            cluster_by=cluster_by,
            skip_unchanged_versions=skip_unchanged,
            snapshot_metadata=metadata[table],
        )

        if ingestion_type in ("cdc", "cdc_with_deletes"):
            _create_cdc_table(spark, connection_name, config)
        elif ingestion_type == "snapshot":
            _create_snapshot_table(spark, connection_name, config)
        elif ingestion_type == "append":
            _create_append_table(spark, connection_name, config)

    for table_name in table_list:
        _ingest_table(table_name)
