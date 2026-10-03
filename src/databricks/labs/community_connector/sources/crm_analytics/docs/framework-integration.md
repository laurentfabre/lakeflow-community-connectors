# CRM Analytics framework integration

Status: source implementation and offline contract tests only, 2026-10-02.
Framework baseline: `ddc153b9741b686759ad85f25d2d45ca8a6ac24b`.

## Connector responsibility

`CrmAnalyticsLakeflowConnect` discovers datasets, maps pinned main XMD to a
schema, declares `snapshot` ingestion and configured destination business keys,
and yields every row of the selected version and column projection.
`CrmAnalyticsDataSource` exposes it through the framework's `lakeflow_connect`
data source.

The framework's Python batch reader invokes `read_table(..., None, ...)` once
and ignores the returned offset. All pages must be consumed inside the iterator.
A prefix is not a complete snapshot; exceptions during extraction must fail the
read. A no-change result is not an empty source: an unchanged version is read in
full. Independent schema/read instances must receive the same explicit `version_id`.

The shared workspace pipeline already uses `apply_changes_from_snapshot` to
maintain the destination from full snapshots. No extra archive is required merely
to use that path. Managed ingestion and real Spark/Delta failure atomicity still
require validation on the chosen runtime; Python unit tests do not prove them.

## Keys, naming and scope

Configure `source_primary_keys` with SOURCE field names. Connector metadata
returns the corresponding DESTINATION names. The shared pipeline strips its
`primary_keys` override before invoking the source, then uses it downstream;
omit that override unless it exactly matches the names returned by the connector.
The source cannot validate an override it never receives.

Readability must not change identity: persist the original field names in Spark
field metadata and the descriptor's `column_mapping`. Keep naming mode, keys,
visibility, projection and schema consistent with existing targets/checkpoints.
Changing `column_name_mode` or `selected_columns` requires an explicit migration
or new baseline, not an in-place reinterpretation of an existing table. The
configuration fingerprint distinguishes all-column mode from explicit selection;
input selection order is immaterial. Structurally valid unselected field changes
do not change the selected schema. Selected schema drift remains fail-closed.

The connector requires scalar physical values and a stable full-visibility scope.
Counts, exact pages, unique/non-null keys, terminal-page checks and metadata
revalidation are safeguards under the source contract, not independent proof
that visibility or values cannot change. Live tests must compare exact keys/values.

## Read budgets and completion

`max_query_requests` counts actual SAQL HTTP attempts per `read_table` invocation,
including retries, count probes and the terminal empty page. Metadata GETs do
not consume the query allowance. `max_read_seconds` covers monotonic elapsed
time from the read call through iterator exhaustion, including cold metadata,
request processing, retry sleeps and pauses by the consumer. Separate earlier
schema discovery is outside that read's time budget.

Both limits fail the entire read, never return a successful prefix or an empty
fallback. Consumers must propagate iterator errors and publish only after full
exhaustion. A completed read exposes query/time metrics through
`get_read_evidence`: `query_requests`, `elapsed_seconds`, `max_query_requests`
and `max_read_seconds`. A failed or unfinished read has no completion evidence.
Synchronous HTTP cannot guarantee cancellation at an exact wall-clock deadline:
timeouts are capped and late responses are rejected when control returns.
Changing budget controls does not change the data configuration fingerprint.
Neither local tests nor these controls establish source scalability or runtime
atomicity of downstream snapshot application.

## Optional project-level archival systems

The community source package contains no Bronze capture, manifest store,
retention policy or replay deployment. These are separate application concerns,
not side effects of connector reads or declarative pipeline planning. No archive
modules are included in this connector wheel or generated standalone reader.

An application needing historical replay must independently implement immutable
storage, durable completion evidence, writer fencing, publication ordering,
idempotent application and retention that protects pending consumers. It must
distinguish a caught-up replay callback from a validated empty snapshot and prove
recovery across data/manifest/target commits. The source's descriptor/read-evidence
helpers do not establish those guarantees by themselves.

## Runtime validation

Before deployment obtain an explicit Databricks profile, approved UC connection,
catalog/schema/Volume, runtime and test resources. Never auto-select a profile.
Verify injected-token renewal, matching source identity, full multi-page reads,
delete safety, naming/key configuration, and source-to-target value equality.
See [validation.md](validation.md) for the source test evidence and remaining gates.
