# CRM Analytics API Documentation

Research date: 2026-10-02. This is the Phase 1, public-documentation contract for
`crm_analytics`, not evidence of a successful Salesforce organization read. The
official REST PDF downloaded for this research identifies itself as Winter '27,
last updated September 11, 2026. No credentials or live organization endpoints
were used. API-version availability must be checked on the target organization
in Phase 2; the edition of a public guide is not that check.

The research record below remains historical. A subsequent targeted Databricks
ingestion smoke test is described in [validation](docs/validation.md); it does
not close the remaining production gates.

## Implementation decision and scope

Implement **full snapshots of CRM Analytics dataset rows**, discovered dynamically
and addressed by dataset ID. These are not Salesforce `Account`, `Opportunity`,
or other sObjects. The documented extraction mechanism is a read-only SAQL query
submitted through `POST /services/data/v{api_version}/wave/query`. SAQL explicitly
supports an immutable-looking dataset/version reference, ungrouped projections,
ordering, offsets, and limits. Version retention and immutability of every
associated property are **not** guaranteed by this research: version predicates
and XMD can change, and old versions can become unavailable. [R1, R2, R3]

Initial supported scope is materialized `Default` datasets, with scalar
dimensions, scalar measures, and the physical date fields identified by XMD.
Dataset names and column sets are organization-specific; do not create a static
list of business tables. Do not silently fall back to another dataset, current
version, SOQL, the Salesforce Bulk API, or a capped sample.

Airbyte's Salesforce connector documents dynamically discovered standard/custom
sObjects; Fivetran's Salesforce connector documents REST/Bulk API and SOQL reads.
Those vendor pages did not establish CRM Analytics row extraction support and
were used only to distinguish scope, not as proof of a Wave API contract.
Consequently all selected datasets share one researched API pattern. [R8, R9]

## Authorization

Use **Salesforce OAuth 2.0**, with an integration identity authorized for the
selected Analytics app/datasets. Every data request carries:

```http
Authorization: Bearer <access_token>
Accept: application/json
```

Query requests additionally use `Content-Type: application/json`. Prefer a
managed OAuth connection that injects a current access token; do not run a
browser authorization flow from connector methods. The Salesforce CRM Analytics
quick start documents the `api` and `refresh_token` / `offline_access` scopes.
Its permissive tutorial policies are not a production security recommendation.
[R1, pp. 2, 9–12; R7]

For local Phase 2 testing, obtain an access token securely outside the connector
and configure `instance_url` plus `access_token`. If the implementation also
supports runtime token refresh, store `client_id`, `client_secret`, and
`refresh_token` only in the local secret configuration or connection secret
store. Salesforce documents refresh as obtaining a replacement access token
after expiry [R7], but this research has **not** independently verified the
complete Salesforce token-endpoint request/rotation contract. Do not invent or
claim a tested refresh implementation. A bearer-token-only implementation must
fail clearly on 401 and document that renewal belongs to the token provider.

Recommended connection contract:

| Option | Purpose |
| --- | --- |
| `instance_url` | Trusted HTTPS Salesforce My Domain/API origin, without a path, query, or embedded credentials. |
| `access_token` | Secret OAuth bearer token, supplied locally or injected by the connection. Never log it. |
| `api_version` | Explicit supported REST version, at least 53.0 for `totalRowCount`; do not assert that a default is the latest version. |
| `page_size` | Local extraction page-size policy, preferably 1,000 and no more than 10,000 initially. This is not a documented global dataset-size limit. |
| bounded timeout/retry options | Finite request timeout, retry count, backoff, and delay cap. |

Illustrative read, with placeholders rather than credentials:

```http
GET https://<my-domain>.my.salesforce.com/services/data/v<api_version>/wave/datasets?pageSize=200&hasCurrentOnly=true&datasetTypes=Default
Authorization: Bearer <access_token>
```

Do not follow redirects or foreign-origin pagination URLs with an Authorization
header. Validate every returned link against the configured origin and expected
resource path. A simulator uses synthetic credentials; it must never need a
Salesforce login.

## Object List

`GET /wave/datasets` returns the datasets available to the current user. The
`datasets` array is paginated with `nextPageUrl`; `totalSize` is the collection
count, not a dataset row count. `pageSize` accepts 1–200, defaults to 25, and
`page` is an opaque generated token. Use each returned `nextPageUrl`, detect
cycles/duplicate IDs, and fail on malformed pages. Do not construct numeric page
tokens. Relevant filters are `datasetTypes`, `hasCurrentOnly`, `ids`, and optional
`folderId`; the connector should not introduce a hidden folder/search filter.
[R1, pp. 77–81, 282–283; R4]

| Read endpoint, relative to `/services/data/v{api_version}` | Use |
| --- | --- |
| `GET /wave/datasets` | Discover datasets. |
| `GET /wave/datasets/{dataset_id}` | Resolve `currentVersionId`, dataset type, and descriptive metadata. |
| `GET /wave/datasets/{dataset_id}/versions` | List available versions; useful for diagnosis, not a row-change stream. |
| `GET /wave/datasets/{dataset_id}/versions/{version_id}` | Pin schema/count metadata for the exact version. Request `filterGroup=Medium` to obtain `xmdMain`. |
| `POST /wave/query` | Execute read-only SAQL for a pinned dataset/version. |
| `GET /wave/limits?types=DatasetQueries` | Inspect organization Analytics query usage/limits in Phase 2. |

Dataset and version GETs are independently corroborated by the Salesforce LWC
adapter reference [R4, R5]; query syntax is corroborated by the SAQL guide [R2].
The precise REST response-field definitions come from [R1]. Do not treat that
single schema reference as two independent schema observations.

Expose the dataset's opaque `id` as the table name; retain `name` and `label`
only as display information. An API/developer name can be renamed; a label need
not be unique. IDs are not sortable cursors. Treat a disappeared or inaccessible
dataset as an error, not an empty table. Restrict the first implementation to
materialized Default datasets. The request filter is `datasetTypes=Default`;
resource responses may return `datasetType: "default"` or `"Default"`, which the
connector compares case-insensitively. `Live`, `StagedData`, and `Trended` are deferred.

The versions collection has documented `url` and `versions` fields, not a
documented row-pagination token. Optional version-list parameters
`createdAfter`, `createdBefore`, and `includeArchived` appear from API 62.0.
Do not infer guaranteed history, unlimited retention, or a monotonic sequence
from these filters. [R1, pp. 82–84, 284–287]

## Object Schema

Read the exact version resource with `filterGroup=Medium`, then use `xmdMain`,
not user dashboard formatting, row sampling, or the current version's schema.
`xmdMain` is a **Medium** response field. A small/default response can omit it;
missing XMD must not mean a zero-column table. [R1, pp. 285–286, 387–389]

The following is an **illustrative minimal fixture**, not a captured response:

```json
{
  "id": "0Fc000000000001AAA",
  "dataset": {"id": "0Fb000000000001AAA"},
  "totalRowCount": 2,
  "xmdMain": {
    "dimensions": [{"field": "Account.Id", "label": "Account"}],
    "measures": [{"field": "Revenue", "label": "Revenue"}],
    "dates": [],
    "derivedDimensions": [],
    "derivedMeasures": []
  }
}
```

### Complete metadata fields relevant to discovery and version extraction

The base asset fields inherited by Dataset and DatasetVersion are
`assetSharingUrl`, `createdBy`, `createdDate`, `description`, `id`, `label`,
`lastAccessedDate`, `lastModifiedBy`, `lastModifiedDate`, `name`, `namespace`,
`permissions`, `type`, and `url`. They describe assets, not dataset row columns.

Dataset's additional fields are `clientShardsUrl`, `currentVersionCreatedBy`,
`currentVersionCreatedDate`, `currentVersionId`, `currentVersionLastModifiedBy`,
`currentVersionLastModifiedDate`, `currentVersionPredicate`,
`currentVersionSharingInheritance`, `currentVersionSupportsNewDates`, legacy
`currentVersionTotalRows` (through API 52.0), `currentVersionTotalRowCount`
(53.0+), `currentVersionUrl`, `dataRefreshDate`, `datasetType`, `folder`,
`lastMetadataChangedDate`, `lastQueriedDate`, `licenseAttributes`,
`liveConnection`, `userXmd`, `versionsUrl`, and `visibility`.

DatasetVersion's additional fields are `dataset`, `files`, `filesUrl`,
`predicate`, `predicateVersion`, `securityCoverageUrl`, `sharingSource`,
`source`, legacy `totalRows` (through 52.0), `totalRowCount` (53.0+), `xmdMain`,
and `xmdsUrl`. The docs say `predicate` and `predicateVersion` are returned only
under a condition involving the input predicate; their absence does **not**
prove unrestricted row visibility. Use `totalRowCount`, never the deprecated
name, for the supported API range. [R1, pp. 244–246, 285–288]

### XMD field inventory and physical row schema

Top-level XMD fields are `createdBy`, `createdDate`, `dataset`, `dates`,
`derivedDimensions`, `derivedMeasures`, `dimensions`, `errorMessage`,
`language`, `lastModifiedBy`, `lastModifiedDate`, `measures`, `organizations`,
`showDetailsDefaultFields`, `type`, and `url`.

For physical row projection, enumerate **all** `dimensions[].field` and
`measures[].field`, including fields with `showInExplorer=false`. For every
entry in `dates`, enumerate its `fields` map: `day`, `epochDay`, `epochSecond`,
`fiscalMonth`, `fiscalQuarter`, `fiscalWeek`, `fiscalYear`, `fullField`, `hour`,
`minute`, `month`, `quarter`, `second`, `week`, and `year`. The values of that
map are the actual field names; do not manufacture names by concatenating
suffixes. Deduplicate names only when the inferred types agree; fail on
contradictory metadata. [R1, pp. 378–389; R2, pp. 89–90]

**Connector projection policy (local implementation, not an API guarantee):**
the default inventories and reads all physical columns. Optional
`selected_columns` chooses an explicit nonempty set of exact physical names,
including all business keys. Selection is sorted canonically and pushed into
the ungrouped SAQL projection. Unsupported unselected fields can be excluded;
selected derived/unsafe/unknown fields and conflicting selected types fail.
Malformed metadata remains an error. Valid changes confined to unselected fields
do not change the projected schema. Projection mode/list is part of the data
configuration fingerprint and requires migration when changed. Selection never
filters rows or weakens full-visibility/completeness checks.

Selection values are case-sensitive **source** names, not XMD labels or encoded
destination names: select `Account.Id`, not `Account__Id`; select `Amount`, not
`amount`. Read names from the pinned version's `xmdMain.dimensions[].field`,
`measures[].field`, and the values of `dates[].fields`. A logical date name or a
field from another version may be absent from this physical inventory. Unknown
selection errors list unmatched names and bounded case/destination-name hints,
or a sample of available physical fields. Hints never rewrite the selection or
silently drop fields. Correct the configuration using the pinned metadata.

The remaining XMD dimension properties are `conditionalFormatting`,
`customActions`, `customActionsEnabled`, `defaultAction`, `label`,
`linkTemplate`, `linkTemplateEnabled`, `linkTooltip`, `members`,
`recordDisplayFields`, `recordIdField`, `recordOrganizationIdField`,
`salesforceActions`, `salesforceActionsEnabled`, and `showInExplorer`.
They control presentation/actions, not row identity or physical types.

The remaining XMD measure properties are `conditionalFormatting`,
`currencySettings`, `format`, `label`, and `showInExplorer`. Display formatting
is not a trustworthy storage precision/scale declaration.

The remaining XMD date properties are `alias`, `compact`, `description`,
`firstDayOfWeek`, `fiscalMonthOffset`, `format`, `fullyQualifiedName`,
`isConvertedDateTime`, `isYearEndFiscalYear`, `label`, `showInExplorer`, and
`type` (`Date`, `DateOnly`, or `DateTime`). These must not cause a lossy or
implicit timezone conversion.

Derived dimensions/measures are not automatically treated as physical columns.
Reject unsupported derived-only or ambiguous metadata until the implementation
has an explicit, tested policy; never silently substitute a calculated or
grouped result for source rows. Multivalue dimensions are also unsupported in
the initial connector: the SAQL guide explicitly warns about their behavior in
`foreach` and `group`. If metadata or runtime values reveal lists/objects,
fail the snapshot rather than joining or stringifying them. [R2, pp. 26–27]

**TBD, Phase 2:** confirm that main XMD enumerates every physical field for each
supported dataset builder/date representation, including hidden columns and
multivalue indicators. The public REST schema does not document a universal
multivalue flag. A simulator must not invent one as guaranteed Salesforce API
behavior. Metadata-only rejection cannot be claimed to detect all such fields.

## Get Object Primary Keys

The researched APIs do not provide a guaranteed unique primary key for an
arbitrary dataset. Dataset IDs identify datasets, not their rows;
`recordIdField` is an action-link setting, not a uniqueness constraint.

By default, require configured **stable, unique, non-null composite keys** for each dataset,
for example `source_primary_keys=["Account.Id", "FiscalYear"]`. Use source field names
and validate membership in the pinned schema. Initially prefer scalar dimension
keys. Aggregate datasets need keys at their aggregate grain, not an assumed
Salesforce object ID. Never infer keys from labels, a sample, or a mutable-row
hash. Reject duplicate/null keys across the **entire** snapshot, including page
boundaries. A uniqueness check only inside a page is insufficient.

Keys also give a total order for offset pagination. Validate uniqueness while
extracting, retain a global key set or use bounded/disk-backed validation, and
fail visibly when local validation limits are exceeded. Do not stop reading at
a memory/row cap and report success.

**Optional raw-row identity (connector policy, not a Salesforce feature):**
`auto_increment_column="_ingestion_row_id"` adds a local BIGINT number 1..N and
the STRING `_crm_analytics_version_id`. Their pair is returned as the destination
primary key. The source is ordered by all projected physical values before
numbering, retaining identical rows and null/duplicate business keys.
`source_primary_keys` may be omitted; if supplied its fields must still exist
in the physical projection. Generated fields are not SAQL source columns and
must not appear in `selected_columns`.

In generated mode the projection adds `(case when 'Field' is null then 1 else
0 end)` under internal, collision-safe aliases. A missing physical JSON property
is restored as null only when its numeric flag is 1. A flag of 0 requires a
present non-null value. Missing/invalid flags and contradictions fail; extra
physical properties still fail. Internal aliases never enter target rows or
schemas. This uses the documented searched `case` and `is null` operators
[R2, pp. 35–37]. A targeted live smoke corroborated sparse null recovery for a
two-column projection; wider field/type coverage still needs validation.

Numbers restart per version. The version component prevents incorrect entity
matches when numbers repeat across versions; this identity does not follow a
business entity through changes. SCD Type 1 replaces the previous version's raw
rows and downstream steps can clean business keys. SCD Type 2 records histories
of snapshot rows, not stable entities. This is not a global persistent sequence
or an append-only capture archive. A schema/key/checkpoint migration or new
target is required to enable/disable the mode or change the output name.

Count, exact-page, scalar-value, metadata, visibility and budget checks remain.
Generated uniqueness cannot prove source uniqueness or detect every equal-count
repeated/missing row. Completion evidence explicitly marks
`source_keys_validated=false`; validate source-to-target row multiplicities and
ordering/collation behavior on real data before relying on this mode.

## Object's ingestion type

All supported tables declare `snapshot`, with configured primary keys and no
source cursor field. No native dataset-row change feed, deleted-row endpoint,
or row update timestamp was established. `dataRefreshDate` and version creation
time describe dataset assets; neither is a row cursor. Dataset versions are
opaque identities, not CDC events. [R1]

`read_table(..., start_offset, ...)` must yield the **complete** pinned snapshot
in its single returned iterator. The imported framework batch reader calls it
once with `start_offset=None` and ignores its returned offset. An unchanged
version still requires complete rows in the direct-source path; an empty
iterator is not a no-change signal. Source failures cannot become a valid
empty snapshot.

Snapshot-derived CDC detects differences between completed observed snapshots,
not every source transaction. Immutable Bronze storage, a COMPLETE manifest,
monotonic publication sequence, and ordered replay are separate integration
responsibilities outside this source package; `docs/framework-integration.md`
describes that boundary, not an included archive implementation.

## Read API for Data Retrieval

### Version pinning and metadata consistency

Implemented table options use `source_primary_keys` (optional with generated
identity), optional `auto_increment_column`, and optional explicit `version_id`.
The separate common pipeline `primary_keys` override uses mapped destination
names; source identifiers must not be supplied through that reserved override
unless they equal the mapped names. `column_name_mode=readable` preserves simple
ASCII names, maps dots to `__`, escapes other code points as `_uXXXX_`, and prefixes
digit-leading names or case-insensitive `none` with `c_`. Case-insensitive schema
collisions fail. `legacy_hex` reproduces the original `c_` + UTF-8 hex schema.
Both retain source names in field metadata; existing targets need an explicit
naming-mode migration or must opt into `legacy_hex` before upgrading.
`version_id` allows an archival capture to select one version before Spark
schema resolution. Without it, resolve `currentVersionId` once and cache/persist
that selection for schema, metadata, count query, every page, and all retries
within the read. Validate returned dataset/version IDs. Never re-resolve current
version between pages or retry an expired version against a newer one.

Re-read the pinned metadata at the end and compare row count, structural XMD,
and all available security predicate/sharing information. Asset presentation
metadata and last-access timestamps are not business-change columns. Security
and schema changes invalidate the read; an HTTP 200 alone is not completion.
Metadata checks cannot detect every external permission/principal change.

### Read-only SAQL request and response

Illustrative payload for page 2 of a pinned version (two columns shown only to
make the example readable; implementation projects **all supported fields**):

```json
{
  "queryLanguage": "SAQL",
  "query": "q = load \"0Fb000000000001AAA/0Fc000000000001AAA\"; q = foreach q generate 'Account.Id' as 'Account.Id', 'Revenue' as 'Revenue'; q = order q by 'Account.Id' asc; q = offset q 1000; q = limit q 1000;"
}
```

The REST guide requires both dataset ID and version ID in a SAQL `load`; a
dataset name is not accepted there. The SAQL guide documents ungrouped
`foreach` as one projection per input row. Do not use `group`, `unique`,
aggregation, `flatten`, or `filter` for row extraction. `order` must precede
`offset`, and `offset` must precede `limit`. Composite ordering is supported:
`q = order q by ('Key1' asc, 'Key2' asc);`. [R1, pp. 13–18; R2, pp. 52–53, 64–66]

IDs must be validated as Salesforce identifiers. Field names must be safely
quoted/escaped according to supported SAQL syntax, or rejected when the
implementation cannot safely represent them. Never interpolate user SQL/SAQL.
Single-quoted identifiers are distinct from double-quoted string literals.
Avoid the reserved projection name `none`. Dotted names are single identifiers,
not nested JSON paths. Map them reversibly for Delta consumers; do not silently
replace punctuation and create name collisions.

Documented response structure:

| Field | Meaning / handling |
| --- | --- |
| `action` | Query action, usually `query`. |
| `responseId` | Diagnostic request identifier. |
| `results.metadata` | Query lineage/projection metadata, not a replacement for pinned dataset XMD. |
| `results.records` | Array of row objects; validate its presence, type, projected keys, and value types. Generated mode validates internal null flags before reconstructing confirmed omitted null properties and enforcing exact physical keys. |
| `query` | Original query text. |
| `responseTime` | Query timing. |
| `warnings` | Optional warning/error information. Fail closed on warnings until a specific harmless code is justified. |

The REST guide's example includes `warnings` even in a structured query
response, including a limit warning. A warning must not be discarded merely
because the HTTP status is 200. No response continuation token is documented
for query records; pagination is by SAQL offset. [R1, pp. 13–15]

### Completeness protocol

1. Pin version and structural schema; require nonnegative integral
   `totalRowCount` from that version.
2. Independently query the same version with
   `q = load "<dataset_id>/<version_id>"; q = group q by all; q = foreach q generate count() as '__row_count';`.
   This aggregation is for **validation only**, never the rows being ingested.
3. Initially require visible count equal to `totalRowCount`. This intentionally
   rejects restricted/filtered visibility when the metadata count represents
   more rows than the integration identity can query. Do not support an
   unverified subset by quietly weakening this gate.
4. Read offsets `0, page_size, 2 * page_size, ...`, or advance by actual returned
   count with a strict progress check. Require every non-final page to match
   the requested size. Reject overlarge pages, malformed rows, duplicates,
   missing/null keys, schema mismatches, or count overflow.
5. Require exact row count equality with the independently queried count and
   version metadata. Issue a terminal page request at the expected total and
   require it to contain no rows. A short/empty page before the expected total
   is an extraction failure, not success.
6. Repeat count/security/schema checks at completion. Only then let the
   integration publish a complete snapshot. The iterator may have yielded
   partial rows before its final check; its consumer must propagate failure
   and must never publish a manifest on partial consumption.

For zero rows, require affirmative version count 0, count-query evidence of 0,
and an empty offset-0 record page. **TBD:** exact `group by all` response for a
zero-row dataset must be captured in Phase 2. Do not assume an absent/malformed
`results.records` is the zero-row case. If an empty aggregation array is
observed, supporting it requires the other independent zero-row evidence.

This is an implementation design based on documented primitives, not a
Salesforce-provided transaction/snapshot isolation guarantee. Matching counts
and pinned IDs reduce truncation risks but cannot prove absence of an
equal-count visibility change. Maintain a stable principal and visibility-scope
fingerprint; treat scope changes as a new baseline requiring explicit approval.

### Limits, retries, and alternative APIs

**Connector controls, not researched Salesforce limits:** each read has a
`max_query_requests` allowance (default 2,000; range 1–100,000) and
`max_read_seconds` elapsed budget (default 1,800; range 1–86,400). The allowance
counts actual SAQL HTTP attempts, including retries, both count queries and the
terminal page; metadata GETs are excluded. The retry-free minimum is
`ceil(totalRowCount / page_size) + 3`. Elapsed time includes cold read-time
metadata, request processing, retry delays and consumer pauses from read call
through exhaustion. Separate schema discovery is outside that interval. Any
budget breach aborts the full snapshot without completion evidence. Request
timeouts are capped to remaining time, but synchronous HTTP does not guarantee
hard deadline cancellation. These controls limit work; they do not establish
SAQL performance, organization-wide quotas or a scalable extraction alternative.

Without an explicit SAQL limit, queries return at most 10,000 rows. Always send
an explicit page limit. The researched SAQL offset section does **not** state a
maximum total offset; do not invent a universal 100,000-row cap or promise
unlimited extraction. Phase 2 must test offsets beyond 10,000 and representative
dataset sizes, query duration, response sizes, and organization limits. [R2,
pp. 64–66]

`GET /wave/limits` documents `DatasetQueries`, but the public REST resource does
not establish one numeric quota appropriate to every organization. Inspect
effective limits and measure consumption in Phase 2. Initially read pages
serially; do not introduce unbounded partition concurrency. Retry connection
failures, timeouts, and transient status responses with bounded exponential
backoff/jitter; honor a valid bounded `Retry-After`. Permission failures and
invalid/expired versions fail immediately; exhausted transient failures fail
the snapshot. Never log bearer tokens or source row payloads. [R1, pp. 96–99]

| Alternative | Decision |
| --- | --- |
| SQL via the same `/wave/query` resource, `queryLanguage="SQL"` | Documented, but the examples use dataset names. Not selected because SAQL has an explicit documented ID/version pin and paging contract. Do not infer SQL version-pin semantics. |
| `GET /wave/datasets/{id}/versions/{version}/files/{file}` | Documented binary resource restricted to the Integration User. File format, complete extraction, and availability are not established here; not an initial fallback. |
| SOQL / Bulk API / Salesforce CDC | Operate on a different source model; not a dataset-row extraction substitute. |
| Dashboard/lens exports | Can be aggregated, filtered, or presentation-limited; not a raw dataset snapshot contract. |

## Field Type Mapping

Use fixed schema metadata, not inference from the first row. Preserve actual
JSON null versus an empty string, leading zeros, and Unicode. Parse JSON
numbers as `decimal.Decimal` before any precision-sensitive conversion.

| Source physical field | Suggested Spark representation | Validation |
| --- | --- | --- |
| Scalar dimension (`dimensions[].field`) | `StringType` | Require string or null; no coercion of arrays, booleans, or objects. |
| Scalar measure (`measures[].field`) | Explicit bounded `DecimalType(precision, scale)` | XMD display format does not establish storage precision. Configure the contract or choose a documented conservative default, and reject overflow or fractional rounding instead of silently using binary float. |
| Date `fullField` and calendar/fiscal components | `StringType` | Preserve the source physical representation. The SAQL guide identifies date components as dimensions. Do not discard parts, normalize timezone, or infer integers from digit strings. |
| Date `epochDay` / `epochSecond` | `LongType` (or explicit lossless decimal contract) | Require an integral in-range value; seconds/days since UTC epoch, not local wall-clock timestamps. Reject fractional or out-of-range values. |
| Multivalue/list/object | Unsupported initially | Fail visibly; SAQL projection behavior is not a lossless array-export guarantee. |
| Derived-only/unknown type | Unsupported initially | Reject rather than omitting a required physical field or guessing. |

Measures need an explicit documented precision policy. `DoubleType` with
unconditional `float()` would violate this project's precision requirement.
Decimal values unsupported by the chosen Spark schema must raise. Salesforce
may itself have transformed uploaded values; preserve the queried dataset
value, not claim reconstruction of pre-ingestion source values. [R2, pp. 25–27,
89–90]

Schema evolution policy: an additive nullable field may be accepted by an
explicit integration policy; removals, key changes, incompatible type changes,
ambiguous names, and decimal precision loss require a migration and must stop
normal snapshot application. The selected version's projected schema must be
identical throughout extraction; explicit selection can ignore structurally valid
changes confined to unselected fields without relaxing count/security checks.

## Known Quirks and Phase 2 Validation

The following are release gates, not reasons to collect credentials during
Phase 1:

- Record discovery and exact-version metadata for a real `Default` dataset;
  establish API-version availability, principal permissions, `xmdMain` shape,
  and complete physical field inventory.
- Compare source-to-target values, including null and empty values, non-ASCII
  text, leading-zero identifiers, decimal extremes, hidden/dotted columns,
  dates/timezones, and unsupported multivalue fields.
- Read a dataset with more than one page and more than 10,000 rows; verify raw
  total, independent count query, ordered pages, last/terminal pages, duplicate
  detection, and no server truncation/warnings. Measure runtime and quotas.
- Refresh the dataset while extracting: all pages must remain on the original
  selected version. Retry the same page and compare values. Establish version
  retention and fail behavior when a pinned version expires or disappears.
- Validate the legitimate empty-dataset count shape; prove empty publication
  cannot arise from missing access, malformed JSON, a timeout, or no new version.
- Change schema or security metadata during extraction and verify abort.
  Establish what `totalRowCount` means under row-level security, and verify
  restricted scopes fail until an explicit safe policy is implemented.
- Validate token expiry/renewal ownership and sanitized errors; do not label
  an access-token-only connector as refresh-token capable.
- Use a non-null unique composite key; prove deterministic pagination and
  visible failure for null/duplicate/changed keys, including duplicates across
  page boundaries.
- Demonstrate inserts, updates, null transitions, and deletes across completed
  snapshots using the separate archival/replay integration. Do not infer
  transaction-level CDC or exactly-once source extraction from simulator tests.

### Deferred Tables / Capabilities

`Live`, `StagedData`, and `Trended` dataset types; dashboards; lenses; recipes;
dataflows/jobs; folders/apps as destination tables; multi-valued dimensions;
derived/calculated-only datasets; arbitrary SAQL/SQL and row filters; binary
dataset files; write-back testing; source-native row CDC; automatic replay of
every historical version. They have different semantics, fidelity requirements,
or unverified access/retention contracts and are not silently included.

## Sources and References / Research Log

Full URLs are retained for reproducibility. Page numbers below are the printed
PDF page numbers, not the viewer's zero-based index. HTML pages corroborate the
published API design; none is a captured organization response. The public web
search tool returned no usable text during this research, so official PDFs and
provider pages were retrieved directly. No unofficial blog was used for a
technical capability claim.

| ID | Source type / URL | Accessed (UTC) | Confidence / confirmed |
| --- | --- | --- | --- |
| R0 | User-provided official overview: https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_rest.meta/bi_dev_guide_rest/bi_rest_overview.htm | 2026-10-02 | Starting reference; browser-shell retrieval alone was insufficient. |
| R1 | Official REST guide: https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/bi_dev_guide_rest.pdf | 2026-10-02 | High for published contract: OAuth pp. 2, 9–12; queries pp. 13–18, 106; datasets pp. 77–84; limits pp. 96–99; response schemas pp. 244–246, 282–288; XMD pp. 378–389. Winter '27, updated 2026-09-11. |
| R2 | Official SAQL guide: https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/bi_dev_guide_saql.pdf | 2026-10-02 | High: null handling pp. 4–5; literals and multivalue pp. 25–27; ungrouped projections pp. 52–53; load/order/offset/limit pp. 64–66; count p. 78; physical date fields pp. 89–90. |
| R3 | Official query guide within R1, pp. 13–18; independently corroborated by SAQL `load` in R2 p. 65 | 2026-10-02 | High: REST SAQL requires dataset ID/version ID; query response envelopes and warnings. These are two guides, not two live observations. |
| R4 | Official REST dataset resources/schema within R1, pp. 77–84, 282–288 | 2026-10-02 | High: discovery pagination, type values, version metadata and counts. Exact response schemas have only this authoritative published source, so independent live corroboration remains TBD. |
| R5 | Official LWC getDatasetVersion: https://developer.salesforce.com/docs/platform/lwc/guide/reference-wire-adapters-get-dataset-version.html | 2026-10-02 | High: independently confirms GET `/wave/datasets/${datasetIdOrApiName}/versions/${versionId}` and required identifiers. |
| R6 | Official Salesforce core REST guide: https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/api_rest.pdf | 2026-10-02 | High: bearer Authorization header request examples. Not evidence that sObject APIs read Analytics datasets. |
| R7 | Official OAuth refresh overview: https://developer.salesforce.com/docs/platform/mobile-sdk/guide/oauth-refresh-token-flow.html | 2026-10-02 | High for replacement-token concept only; exact runtime token request/rotation implementation not established by retrieved page. |
| R8 | Airbyte-maintained Salesforce documentation: https://raw.githubusercontent.com/airbytehq/airbyte/master/docs/integrations/sources/salesforce.md | 2026-10-02 | High for Airbyte's own scope: standard/custom sObjects; not evidence of CRM Analytics support. |
| R9 | Fivetran-maintained Salesforce documentation: https://fivetran.com/docs/connectors/applications/salesforce | 2026-10-02 | High for Fivetran's own REST/Bulk/SOQL scope; not evidence of CRM Analytics support. |

## Research acceptance status

- All source API template sections are present.
- Read endpoints, paging parameters, response metadata inventory, dynamic row
  schema derivation, key policy, and snapshot strategy are documented.
- No credentialed source requests or Databricks deployment were made during the
  original research stage; subsequent targeted runtime evidence is recorded
  separately in [validation](docs/validation.md).
- Cross-source API semantics are corroborated where possible; exact XMD/schema
  and zero-row response behavior still require live evidence, not fabricated
  second sources.
- Runtime refresh, multivalue/derived fields, version retention, quotas, security
  visibility semantics, and large-data extraction remain explicit limitations
  or Phase 2 gates. Offline simulation cannot close them.
