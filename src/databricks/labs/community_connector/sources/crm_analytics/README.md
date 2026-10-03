# Lakeflow Salesforce CRM Analytics Community Connector

This connector reads complete, version-pinned snapshots of Salesforce CRM
Analytics datasets into Databricks. It reads Analytics dataset rows, not
Salesforce sObjects such as `Account` or `Opportunity`.

Credentialed Salesforce reads, the OAuth lifecycle, and actual Databricks
Spark/Delta execution have not yet been validated for this connector. Complete
the repository's `/validate-connector crm_analytics` workflow before production
use. Offline checks do not establish production readiness.

## Prerequisites

- A Salesforce organization with CRM Analytics enabled and a materialized
  `Default` dataset available through the Analytics REST API.
- An administrator-approved OAuth application and a stable integration identity
  with API access and full visibility of every selected dataset. Dataset
  permissions, sharing rules, and security predicates must not narrow that
  identity's view during or between captures without an approved new baseline.
- Scalar physical columns described by the dataset's main extended metadata
  (XMD), and configured business keys that are stable, unique, and non-null
  across the entire dataset. Selected multivalue or derived/calculated columns
  are not supported; an explicit `selected_columns` projection can exclude them.
- An explicit REST API version supported by the target organization, at least
  `53.0`. The edition of a Salesforce guide is not evidence that a version is
  enabled for your organization.
- A Unity Catalog-enabled Databricks environment where you can create or use a
  community connector connection, write the selected destination, and run a
  compatible pipeline. Network access to the Salesforce API origin and the
  applicable OAuth endpoints is required.

## Setup

### Required Connection Parameters

The connection uses OAuth. Supply your application's identity, then sign in and
authorize the connection in a browser as the intended shared integration
identity. The connection obtains the access token automatically and owns its
renewal; you do not paste an access token into the normal connection setup or
pipeline specification. Never put credentials in source code or fixtures.

| Name | Type | Required | Description | Example |
| --- | --- | --- | --- | --- |
| `client_id` | string | Yes | Salesforce OAuth application's consumer key. | `<APP_CONSUMER_KEY>` |
| `client_secret` | secret string | Yes | Salesforce OAuth application's consumer secret; store as a connection secret. | `<APP_CONSUMER_SECRET>` |
| `instance_url` | string | Yes | Trusted HTTPS API origin for the authorized organization. Host must end in `.salesforce.com`; no resource path, query, fragment, embedded credentials, or port other than 443. | `https://example.my.salesforce.com` |
| `api_version` | string | Yes | Explicit organization-supported REST version in `NN.0` format, at least `53.0`. There is no latest-version default. | `64.0`, only if verified for the target organization |
| `page_size` | integer | No | SAQL rows per extraction page: 1–10,000; default 1,000. Not a total row limit. | `1000` |
| `timeout_seconds` | integer | No | Per-request connect and read timeout: 1–300 seconds; default 30. Not an overall capture deadline. | `30` |
| `max_retries` | integer | No | Additional attempts for connection/timeouts and HTTP 429, 500, 502, 503, or 504: 0–8; default 3. | `3` |
| `max_retry_delay_seconds` | integer | No | Retry-delay cap: 1–120 seconds; default 30. A valid `Retry-After` beyond this cap aborts the snapshot so it can be retried later. | `30` |
| `externalOptionsAllowList` | string | Yes | Required, definitive set of permitted table options. Use the exact value below. | See exact value below |

Set `externalOptionsAllowList` to:

```text
source_primary_keys,primary_keys,version_id,supported_scope,scope_fingerprint,column_name_mode,selected_columns,decimal_precision,decimal_scale,max_keys,max_query_requests,max_read_seconds
```

These table options belong under each table's `table_configuration`, not in the
connection credentials. Common pipeline controls such as `scd_type` and
`cluster_by` are handled by the pipeline, not the source reader.

### Obtaining the Required Parameters

1. Have a Salesforce administrator open **Setup → App Manager** and create an
   **External Client App**, or use an existing approved connected app. Salesforce
   restricts new connected-app creation from Spring '26; see the official app
   guidance in [References](#references).
2. Give the app an appropriate integration name and contact, and enable OAuth.
   Configure it for browser-based authorization with PKCE. The connector spec
   requests `api` and `refresh_token`; Salesforce also describes the refresh
   permission as `offline_access`. Grant those permissions to this integration
   rather than copying broader mobile/tutorial scopes or permissive security
   policies.
3. Register the exact callback URI for each connection-creation path you intend
   to use:
   - **Databricks UI:** register
     `https://<your-workspace-host>/login/oauth/http.html`, using the actual
     workspace host and confirming the URI presented by that environment.
   - **Local community-connector CLI:** inspect the exact loopback callback used
     by the selected CLI version, including scheme, hostname, port, and path,
     and register that URI in Salesforce. Do not assume that `http://localhost`
     authorizes `http://127.0.0.1:<port>/callback`, that ports are interchangeable,
     or that wildcard callbacks are accepted. Dynamic-port compatibility must
     be established before relying on this path.
4. Apply your organization's app-access and refresh-token policies. Authorize
   only the intended integration identity and approved administrators. Keep
   app secrets required by the connection's authentication method; do not copy
   mobile-app instructions that disable secrets without verifying compatibility.
5. Copy the app's consumer key and secret into `client_id` and `client_secret`.
   Use the authorized organization's API origin for `instance_url`, not a
   generic login URL.
6. Confirm the organization's supported API version and enter it explicitly.
   The default OAuth endpoints in the spec use the production Salesforce login
   service. For a sandbox or an organization requiring My Domain, configure the
   connection layer's `authorization_endpoint` and `token_endpoint` overrides
   with that organization's approved OAuth endpoints. Do not change
   `instance_url` to a login endpoint to compensate for an OAuth mismatch.

Exact callback acceptance, PKCE, browser consent, token refresh/rotation, current
token injection, and the match between the authorized identity and
`instance_url` remain Phase 2 validation gates. The source reader does not
refresh tokens itself: HTTP 401 fails the read and requires renewal through the
connection or the separately configured local test credential provider.

### Create a Unity Catalog Connection

1. Open **Add Data** and follow the Lakeflow Community Connector setup flow for
   Salesforce CRM Analytics, where available in your environment.
2. Create a connection with the parameters above, or select an existing approved
   connection for the same organization and integration identity.
3. Set the exact `externalOptionsAllowList` shown above and complete browser
   authorization. Confirm that the callback and token lifecycle work before
   starting scheduled ingestion.

The repository's community-connector CLI and Unity Catalog connection API are
alternative configuration surfaces, subject to their OAuth support and the
same callback validation gates. A connection specification alone does not prove
that either path has successfully authenticated to Salesforce.

## Supported Objects

Tables are discovered dynamically as CRM Analytics **dataset IDs**, not dataset
labels or API names. Use the exact 15- or 18-character dataset ID. Discovery
includes only `Default` datasets with a current materialized version.

| Object category | Ingestion mode | Keys | Source cursor |
| --- | --- | --- | --- |
| Materialized `Default` dataset with supported scalar physical columns and full visibility | Complete `snapshot` | Operator-supplied source dimension/date-string keys, mapped to destination names | None |

Each capture selects one version and uses it for the schema, count queries, and
every page. Pass an explicit `version_id` whenever schema discovery and reading
occur in separate Spark contexts; a version resolved in one context is not
automatically pinned in another. If a pinned version expires, the capture fails
instead of switching to the current version.

Every read returns the full snapshot, even if the version is unchanged. The
connector validates counts before and after extraction against `totalRowCount`,
requires exact page lengths and a terminal empty page, checks all key tuples,
and rechecks schema/count/security metadata. It fails on incomplete data,
warnings, unsupported values, or metadata drift. A failed or partially consumed
read is not a complete snapshot. An affirmatively empty snapshot is different
from no change: an empty aggregate result is not accepted as proof of zero rows.

This connector exposes no native dataset-row CDC stream, tombstone feed, or row-level cursor.
A snapshot pipeline can infer changes and removals between accepted complete
snapshots with the same keys and visibility scope. It cannot recover source
changes between captures or make a visibility change equivalent to a business
deletion. The source does not guarantee historical-version retention or
immutability of all associated XMD/security properties.

Unsupported scope includes sObjects, live/external dataset types, selected
derived/calculated or multivalue fields, arbitrary row filters, and sample/total-row
caps. Supported scalar values for the chosen projection and stable full row
visibility must be established by the operator; runtime checks cannot detect
every multivalue or equal-count permission change. Column selection changes only
the projection, never row visibility or the completeness checks.

### Selecting columns

Omit `selected_columns` to retain the strict all-physical-column behavior. To
exclude unsupported or unwanted columns, supply a nonempty JSON array of exact
source physical names, for example `["Account.Id","FiscalYear","Amount"]`.
Every field in `source_primary_keys` must be included. Empty, duplicate, unknown,
derived or unsafe requested names fail; selected fields still require supported
types and values. Malformed metadata is rejected even for unselected fields.

Selection order is normalized: reversing that array does not change the schema
or scope. The schema and source query project only the selected fields. Valid
changes confined to unselected fields do not change the selected schema;
selected schema, count and security drift still fail. Collision checks apply
to the selected destination columns.

The descriptor and configuration fingerprint distinguish all-column mode from
explicit selection, even if today's selected fields happen to be identical.
Changing the projection requires an approved target/checkpoint baseline or
migration; never silently reinterpret an existing snapshot history. The new
projection-aware fingerprint also requires baseline review for external state
keyed by an earlier connector fingerprint.

### Column names and keys

The default `column_name_mode="readable"` preserves ASCII letters, digits and
underscores and maps dots to double underscores: `Account.Id` becomes
`Account__Id`, while `FiscalYear` remains `FiscalYear`. Other code points become
`_uXXXX_` (lowercase hexadecimal, at least four digits); digit-leading names and
the reserved name `none` (any case) receive a `c_` prefix. Source quoting/control
restrictions still apply. Recover the exact original name through each Spark
field's `source_name` metadata or the snapshot descriptor's `column_mapping`.

Mapping collisions are checked case-insensitively across the projected schema.
For example, `Account.Id` and `Account__Id`, or `Id` and `id`, cause a clear
failure. The connector never silently renames an existing field when a new
colliding field appears. Exclude a non-key colliding field with an explicit
projection, rename the source fields, or explicitly select
`column_name_mode="legacy_hex"`, which reproduces the prototype's reversible
`c_` + UTF-8 hex names (for example `c_4163636f756e742e4964`).

**Existing prototype targets:** set `column_name_mode="legacy_hex"` before
upgrading to preserve names and Spark field metadata. Do not switch an existing
target/checkpoint to readable mode in place. Use an approved migration or new
baseline, including destination keys, clustering, visibility/scope configuration
and any separately archived schemas. Readable names are not reversibly decodable
without metadata; the legacy decode helper requires an explicit legacy mode.
The descriptor's configuration fingerprint now includes the naming mode, even
for `legacy_hex`. Any external state keyed by the old configuration fingerprint
needs an explicit baseline review; schema compatibility is not a claim that all
external state can be reused unchanged.

Use original physical names in `source_primary_keys`. The common pipeline
`primary_keys` override, if used, must contain the corresponding mapped
destination names in the same order. Prefer omitting that override so the
pipeline uses the connector-provided keys. A dataset ID is not a row key.

## Table Configurations

### Source & Destination

Set these directly under each `table` object in the pipeline specification:

| Option | Required | Description |
| --- | --- | --- |
| `source_table` | Yes | Exact 15- or 18-character CRM Analytics dataset ID. |
| `destination_catalog` | No | Target catalog; otherwise the pipeline default. |
| `destination_schema` | No | Target schema; otherwise the pipeline default. |
| `destination_table` | No | Target table name; otherwise `source_table`. A descriptive explicit name is recommended. |

### Common `table_configuration` options

| Option | Required | Description |
| --- | --- | --- |
| `scd_type` | No | `SCD_TYPE_1` by default, or the framework's `SCD_TYPE_2` snapshot history mode. This is snapshot comparison, not source-native CDC. `APPEND_ONLY` is not supported for this snapshot source. |
| `primary_keys` | No | Destination-key override. Normally omit. If supplied, use the exact mapped counterpart of `source_primary_keys` for the chosen naming mode, in the same order. |
| `sequence_by` | No | Leave unset. The snapshot source has no row cursor, and the snapshot pipeline does not use this option to order dataset versions. |
| `cluster_by` | No | Mapped destination columns for Liquid Clustering; consumed by the pipeline, not forwarded to the source. |

### Source-specific `table_configuration` options

| Option | Required | Description |
| --- | --- | --- |
| `source_primary_keys` | Yes | Nonempty JSON array of distinct source physical column names, such as `["Account.Id","FiscalYear"]`. Each must be a scalar dimension or physical date string, never a numeric measure/epoch. Composite keys must be unique and non-null across the full snapshot. |
| `supported_scope` | Yes | Exactly `scalar_full_visibility`. This is your attestation that the chosen projection has supported scalar values and stable full row visibility; it is not proof derived automatically from Salesforce. |
| `scope_fingerprint` | Yes | Nonblank, stable, operator-managed identifier for the approved principal/visibility scope; at most 1,024 characters. Do not use a token, run ID, or capture timestamp. Changes require an approved new baseline, not inferred deletions against an old target. |
| `version_id` | No | Exact 15- or 18-character dataset-version ID. Otherwise current version is resolved once per table context. Explicitly pin it across independent schema/read contexts. |
| `column_name_mode` | No | `readable` (default) or `legacy_hex`. Case-insensitive collisions fail. Preserve `legacy_hex` for existing hex-named targets; changing modes requires migration. |
| `selected_columns` | No | Nonempty JSON array of distinct source physical names, including all business keys. Omit for all physical columns. Order is immaterial; changing selection requires migration. |
| `decimal_precision` | No | Measure precision from 1–38; default 38. All measures use this contract. |
| `decimal_scale` | No | Measure scale from 0 through `decimal_precision`; default 18. Lowering precision below 18 also requires lowering scale. Values that require rounding or overflow fail the capture. |
| `max_keys` | No | Maximum full-snapshot uniqueness-validation set: 1–10,000,000; default 1,000,000. Exceeding it fails; it never truncates a snapshot. Size it together with available memory. |
| `max_query_requests` | No | Per-read SAQL HTTP-attempt allowance: 1–100,000; default 2,000. Includes retries, both counts and the terminal empty page. Does not count metadata GETs. |
| `max_read_seconds` | No | Per-read monotonic elapsed-time budget: 1–86,400 seconds; default 1,800. Includes metadata fetched during the read, retry waits and caller delays. Exceeding it aborts the snapshot. |

The pipeline specification accepts arrays and serializes them as JSON. When
passing options through a string-only interface, use JSON array strings, not a
Python list representation. Unknown source options are rejected; do not add
filters or row caps to make a large snapshot fit.

### Extraction budgets

A retry-free snapshot needs at least `ceil(totalRowCount / page_size) + 3` SAQL
attempts: one query per data page, two count queries and a terminal empty page.
That includes three attempts for a positively validated empty dataset. The
connector rejects an impossible allowance before the first SAQL request. Actual
attempts, including failed attempts and retries, consume the allowance; a new
snapshot read gets a new allowance. This is not an organization-wide quota.

Elapsed time starts when the snapshot read is requested and runs through iterator
exhaustion, including any cold metadata lookup, retry waits and downstream/caller
pauses. Schema discovery done separately before that read is outside the budget.
The connector checks elapsed time around requests, parsing and row delivery, and
caps request timeouts to the remaining budget. Synchronous HTTP calls cannot be
forcibly cancelled at an exact deadline; an over-budget response is rejected when
control returns. Retry waits that would exhaust the remaining time are refused.

Neither budget truncates a snapshot or converts a failure into empty data. Rows
already yielded must be discarded if validation later fails; no successful
completion evidence is issued. Completion evidence includes attempt/time metrics
only after full exhaustion. Changing budgets alone does not change the data
configuration fingerprint. Defaults are defensive settings, not validated
Salesforce quotas, scalability claims or runtime guarantees. They do not fix the
underlying offset-pagination cost; measure representative datasets live.

## Data Type Mapping

The schema is derived from the selected version's main XMD, not sampled rows.

| Source physical field | Databricks type | Behavior |
| --- | --- | --- |
| Scalar dimension | `STRING` | Strings are preserved; arrays, objects, and non-string values fail. |
| Scalar measure | `DECIMAL(p,s)` | Defaults to `DECIMAL(38,18)`; exact numeric parsing, without floating-point rounding. Overflow or excess fractional precision fails. |
| XMD date `epochDay` / `epochSecond` | `BIGINT` | Must be an exact signed 64-bit integer; no implicit timestamp conversion. |
| Other supported physical XMD date fields, including `fullField` and date parts | `STRING` | Retains source strings. Parse into dates/timestamps downstream with an explicit format and timezone policy. |
| Null non-key value | Null of the declared type | Preserved as null; not replaced by zero or an empty string. |

Null keys fail regardless of schema nullability. Selected fields whose names contain
unsupported quoting/control characters fail rather than being rewritten in a
way that could change SAQL meaning.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the repository's community connector setup to make the `crm_analytics`
source available to your pipeline. This package contains the source reader,
not an immutable archive or a replay deployment.
Do not select the managed Salesforce sObject connector as a substitute for this
CRM Analytics dataset connector.

### Step 2: Configure Your Pipeline

For the repository's workspace-pipeline path, update `pipeline_spec` in your
pipeline entrypoint. Replace all placeholders with approved values. This
example reads one explicitly pinned dataset version with composite source keys
and an explicit projection (use only fields present in your dataset):

```json
{
  "pipeline_spec": {
    "connection_name": "crm_analytics_connection",
    "objects": [
      {
        "table": {
          "source_table": "<DATASET_ID>",
          "destination_catalog": "<CATALOG>",
          "destination_schema": "<SCHEMA>",
          "destination_table": "account_analytics",
          "table_configuration": {
            "source_primary_keys": ["Account.Id", "FiscalYear"],
            "selected_columns": ["Account.Id", "FiscalYear", "Amount"],
            "supported_scope": "scalar_full_visibility",
            "scope_fingerprint": "<APPROVED_STABLE_VISIBILITY_SCOPE>",
            "version_id": "<PINNED_VERSION_ID>",
            "decimal_precision": "38",
            "decimal_scale": "18",
            "max_keys": "1000000",
            "max_query_requests": "2000",
            "max_read_seconds": "1800",
            "scd_type": "SCD_TYPE_1"
          }
        }
      }
    ]
  }
}
```

Use only keys actually present in that dataset's XMD. Add another table entry
for each additional dataset, with its own approved keys and visibility scope.
Keep credentials in the named connection. A fixed `version_id` intentionally
rereads the same version; orchestration must resolve and propagate a new pin
for each new capture if the goal is to follow dataset refreshes. Validate the
selected environment's managed-ingestion configuration separately; this JSON
is not a direct managed ingestion API request.

### Step 3: Run and Schedule the Pipeline

First complete live validation for one small supported dataset, including a
multi-page read and a refreshed version. Verify full row/key/value equality,
not just the first page or a sample. Then schedule triggered captures at a
cadence that fits source refresh times, query cost, and org-specific quotas.

Each scheduled capture still reads a full snapshot. An unchanged version does
not skip extraction in the stock reader. A true empty snapshot can remove all
previous rows through snapshot comparison; permission failures and partial
results must never be substituted for empty data. Preserve the integration
identity and approved visibility scope between runs.

### Optional immutable archive and ordered replay

The standard snapshot pipeline can maintain current-state data without an
additional archive. This connector wheel intentionally contains no capture job,
manifest database or historical replay deployment. Applications that need an
immutable archive must implement and validate that separately, including fencing,
schema evolution, retention and recovery. Do not interpret source iterator
checks as proof of cross-system publication atomicity. See
[the framework integration boundary](docs/framework-integration.md).

#### Best Practices

- Start with a single scalar dataset and explicitly verified source keys.
- Keep the same integration identity and visibility contract. Treat changes as
  a new-baseline decision, even when row counts are unchanged.
- Pin a version once for each capture, pass it through schema and extraction,
  and allow complete exhaustion and validation before publishing results.
- Adjust `page_size`, timeouts, and schedules to the target org. Discovery uses
  up to 200 datasets per page; this is separate from the SAQL extraction page
  size. Representative volumes and offsets above 10,000 need live validation;
  no universal maximum safe dataset size is asserted.
- Provision memory for the full key-validation set. Raising `max_keys` permits
  a larger validation set; it does not change Salesforce's limits.
- Size query/time budgets using measured source cost and the complete snapshot,
  including retries and final validation. A budget failure requires a full retry.
- Validate empty datasets, refreshes, expired versions, precision, and recovery
  before enabling deletion-sensitive production snapshot comparison.
- Connector instances are sequential, not thread-safe. They lazily reuse an
  HTTP session without cookies or ambient authentication; standalone callers
  should call `close()` when finished. Serialization excludes the live session,
  and a later request creates a fresh one without changing the pinned version.

#### Troubleshooting

| Symptom | Action |
| --- | --- |
| OAuth callback rejection | Compare the complete requested callback with the app registration. Validate UI and local CLI paths independently; do not assume wildcard or dynamic-port support. |
| HTTP 401 | Renew or reauthorize through the connection/test token provider, then retry the complete snapshot. The source reader does not refresh credentials. |
| HTTP 403 or a missing dataset/version | Verify the integration identity, permissions, dataset type, and explicit IDs. Do not fall back to another dataset or current version. |
| Count mismatch, short page, warnings, or metadata drift | Abort and investigate visibility, source changes, unsupported fields, and version availability. Do not publish the rows already read. |
| Duplicate/null key | Correct the business-key definition at the dataset's actual grain. Do not deduplicate, drop nulls, or invent a row hash to pass validation. |
| Unsupported XMD/value or decimal overflow | Confirm scalar physical-column coverage and configure a lossless decimal contract. Treat schema changes as a baseline/migration decision. |
| `max_keys` exceeded | Assess memory and approved full-snapshot volume, then explicitly adjust the bound. Do not cap or sample the extraction. |
| Query/time budget exceeded | Discard the incomplete snapshot. Investigate query cost, retries and downstream delays; adjust limits only with an approved cost/runtime envelope, then retry the full read. |
| Invalid `selected_columns` | Use distinct safe physical source names and include every source key. Exclude unsupported non-key fields; approve a new baseline when changing projection. |
| HTTP 429/5xx, repeated timeout, or delay exceeds retry policy | Reduce load or retry later after investigating source quotas and request duration. Retries are bounded; do not publish a partial snapshot. |
| Unknown table option or wrong destination keys | Use the definitive allowlist; put original names in `source_primary_keys`, and normally omit the destination `primary_keys` override. |
| Destination column collision | Inspect the source schema for case-only, dotted/underscore or escaped-name collisions. Rename source fields or deliberately use `legacy_hex`; never silently drop a column. |

## References

- [Connector implementation](crm_analytics.py), [connection specification](connector_spec.yaml), and [API research and live-validation gaps](crm_analytics_api_doc.md).
- [Framework integration and optional archive boundary](docs/framework-integration.md).
- [Offline validation evidence and remaining live gates](docs/validation.md).
- [Salesforce CRM Analytics REST API overview](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_rest.meta/bi_dev_guide_rest/bi_rest_overview.htm).
- [Salesforce CRM Analytics REST API guide](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/bi_dev_guide_rest.pdf).
- [Salesforce CRM Analytics SAQL guide](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/bi_dev_guide_saql.pdf).
- [Salesforce External Client Apps and Connected Apps](https://developer.salesforce.com/docs/platform/mobile-sdk/guide/connected-apps.html) and [app creation guidance](https://developer.salesforce.com/docs/platform/mobile-sdk/guide/eca-create.html). The mobile-specific scope and secret-policy examples are not this connector's security policy.
