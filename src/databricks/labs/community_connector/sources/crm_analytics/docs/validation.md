# CRM Analytics validation status

Date: 2026-10-02. **Phase 1, offline only; not production certification.**

Framework baseline: `ddc153b9741b686759ad85f25d2d45ca8a6ac24b`.
Local environment: Python 3.12.13, PySpark 4.2.0, pytest 8.4.2.
No Java runtime, Salesforce organization, or Databricks workspace was used.

## Verified locally

### Customer metadata follow-up — 2026-10-30

The current synthetic source suite passes **389 tests with 2 expected skips**;
the separate registry suite passes **9 tests**. The combined command is:

```bash
CONNECTOR_TEST_MODE=simulate .venv/bin/python -m pytest \
  tests/unit/sources/crm_analytics/ tests/unit/sparkpds_registry_test.py -q
```

The 48 added regressions run against both the package and the registered merged
deployment source. They verify lowercase `datasetType: "default"` responses in
discovery, metadata resolution, and complete reads; historical `"Default"` also
works. The query enum remains `datasetTypes=Default`. Other/malformed dataset
types fail before query execution, and a type change during final revalidation
still invalidates the snapshot.

The connection-supplied `access_token_expiration` key is treated as transport
metadata. Lowercase and uppercase option keys support complete reads without
changing the pinned descriptor, configuration fingerprint, or read evidence as
the expiry changes. The 407-row framework batch-reader test includes this
metadata in both column naming modes. Token refresh remains the connection
layer's responsibility; the reader does not interpret this expiry metadata.

Unknown `selected_columns` errors now identify unmatched fields and provide
bounded case-sensitive or destination-name hints, or available source names.
Names remain exact and case-sensitive; hints never remap or omit columns. Tests
cover readable/legacy aliases and bounded diagnostics with unsafe unselected
metadata. Source Pylint using the repository configuration scores 10.00/10.

These are synthetic reproductions of the reported behavior. The customer's
specific unmatched field has not been identified from their metadata, and this
follow-up does not establish live Salesforce ingestion or customer deployment.

### Generated row identity — 2026-10-30

An additional **52 synthetic regression cases** exercise both packaged and merged
deployment readers with `auto_increment_column`. They preserve duplicate/null
business keys and identical rows across pages, verify 1..N numbering and the
version/number key pair, and confirm stable repeat reads despite page-size and
input-order changes. Versions can reuse numbers without overlapping keys.

The suite checks optional/retained source keys, exact destination-key overrides,
empty snapshots, generated-name validation/collisions, readable and legacy
physical names, configuration/schema fingerprints, and generated field metadata.
Separate schema/read instances run 407 rows through the actual Python framework
batch reader in eight SAQL requests. Malformed pages/counts/metadata, unsupported
values, and impossible budgets still fail. Existing strict-mode regressions
continue to reject duplicate/null business keys.

These tests establish synthetic behavior, not Salesforce ordering/collation,
multiset equality, or actual Spark/Delta snapshot application. Generated keys
are technical row addresses; completion evidence explicitly reports
`source_keys_validated=false`. Validate full source/target row multiplicities,
wide projection ordering, schema/key migration, version transitions and runtime
restart behavior before enabling this mode on a customer destination.

### Historical source baseline — 2026-10-02

Run from the repository root:

```bash
CONNECTOR_TEST_MODE=simulate .venv/bin/pytest tests/unit/sources/crm_analytics/ -v --tb=short
```

Final main-agent connector verification after the second local batch:
**289 passed, 2 expected skips in 1.11s**. This adds 129 independent synthetic
cases to the first review batch's 160 passing source tests.
The skips cover unsupported partitioning and a separate delete stream.
The previous revision passed 150 tests with 2 expected skips, including 33 archive
tests. The archive and its tests have moved out of this source contribution;
their independent relocated suite passes 33 tests and is not counted as source
connector coverage. It remains experimental and has not been behaviorally repaired.

The suite exercises the real Python `LakeflowBatchReader` with 407 synthetic
rows, full transport options, separate schema/read instances, all pages and a
terminal empty page. It verifies exact numeric JSON parsing and framework
Decimal conversion, pinned/refreshed/expired versions, completeness failures,
duplicate/null composite keys, empty snapshots, URL safety, bounded retries,
authentication failures, sanitized errors, and schema/security metadata drift.
This is not JVM Spark execution or proof of Salesforce's SAQL behavior.

Both readable and legacy naming modes run through the 407-row batch reader.
Additional tests assert literal naming/collision rules, metadata reversal, legacy
schema compatibility, naming-mode fingerprints, a stable synthetic order unlike
Python's, cross-page duplicate rejection, and the two provisional empty-count
shapes. Date role overlap is explicitly tested as a conservative rejection.
Session tests cover reuse across snapshots/pages/retries, cookie isolation,
per-instance and pickle isolation, close/reopen and response cleanup on errors.
The original Name/name corpus remains unchanged and uses legacy mode in the
inherited harness; readable-mode fixtures rename that synthetic collision only
in memory. This is fixture scope, not evidence that real collisions are supported.

The second batch independently verifies explicit column selection, canonical
selection order and mode/list fingerprints. It checks that unsupported unselected
fields can be excluded without hiding malformed metadata, that valid unselected
schema changes leave the projection unchanged, and that selected schema, counts,
security and key failures remain fail-closed. A projected 407-row read uses
separate schema/read instances through the actual Python batch reader and an
exact eight-query allowance.

Deterministic clock and HTTP-attempt tests cover budget bounds and defaults,
retry-free minimum-query preflight, exact allowances, POST retries/timeouts and
GET retries. They verify cold metadata, lazy startup, parsing, response cleanup,
retry delays, per-request timeout caps, the consumer pause after the final row,
and final metadata revalidation. Limits reset across reads and serialization;
stale readers cannot erase newer evidence. Metrics appear only after complete
validation. A reproduced lone-surrogate input failure was fixed to return a
sanitized connector error instead of a raw Unicode encoding exception.

Review-fix regression checks passed:

- Existing CLI connector-spec suite: 46 tests.
- Existing framework spec-parser and pipeline unit suites: 74 tests.
- Repository-configured Pylint 4.0.5 checks on source and
  simulator-handler modules: 10.00/10. Generated code is excluded as in CI.

Run CLI tests separately from framework tests: their two top-level `tests`
packages conflict when collected in one process. The CLI source path must be
included explicitly in this development environment:

```bash
CONNECTOR_TEST_MODE=simulate PYTHONPATH=tools/community_connector/src:src \
  .venv/bin/pytest tools/community_connector/tests/test_connector_spec.py -q
CONNECTOR_TEST_MODE=simulate .venv/bin/pytest \
  tests/unit/libs/test_spec_parser.py tests/unit/pipeline/test_ingestion_pipeline.py -q
```

All committed corpus data is synthetic. Regenerate it with:

```bash
.venv/bin/python -m tests.unit.sources.crm_analytics.bootstrap_corpus
```

The bootstrap adapts fixture XMD through the repository's
`write_corpus_from_schemas` helper, then restores source field names. Do not
mistake those fixtures for captured Salesforce responses.
The bootstrap regression regenerates into temporary storage and checks every
corpus file byte-for-byte against the existing source-named fixtures.

## Required live and runtime gates

Continue with `/validate-connector crm_analytics`. Establish a controlled
Salesforce dataset, approved integration identity, stable business keys and full
visibility before record-mode tests. Resolve drift from real responses before
claiming simulator fidelity. Verify OAuth callback registration, refresh and
token injection, including the chosen environment's endpoint/identity mapping.

Prove full source-to-target key/value equality, representative volumes and
SAQL offsets above 10,000, empty count response shape, supported XMD physical
types, version retention, and refresh/security behavior. Equal-count visibility
changes cannot be ruled out by row counts alone.

Choose a Databricks profile and destination explicitly before deployment.
Actual Spark/Delta snapshot application, recovery and deployment remain
unproven. Manifest publication, historical replay, retention and archive schema
evolution are separate application work, not features of this wheel. See
[the integration boundary](framework-integration.md).

The shared pipeline strips its `primary_keys` override before invoking source
validation, then uses that override downstream. The source's guard cannot
enforce that path: omit the override, or supply the exact mapped counterpart of
`source_primary_keys`. A regression test records this framework boundary.

## Interpretation of the review fixes

Readable naming, collision checks and HTTP pooling change local behavior, not
the source capability evidence. Preserve `legacy_hex` for old schemas; switching
modes requires migration. The configuration fingerprint now includes the mode,
so external state keyed by the prototype fingerprint needs baseline review.
The second batch also includes projection mode/list in that fingerprint, even
when selection is omitted. External state keyed by a previous fingerprint needs
another explicit baseline review. Reordering an explicit selection is immaterial;
changing its fields or switching between all/explicit mode requires migration.
Budget tuning alone does not change the pin or data configuration fingerprint.
The default 2,000 query attempts and 1,800 seconds are defensive local policies,
not Salesforce capacity guarantees. Synchronous HTTP deadline checks are
cooperative, not strict cancellation. This work does not prove a scalable
replacement for ordered offset queries.

Selected date epoch fields occurring in both measures and date roles remain
rejected as conflicting types; explicit selection can exclude them. Main-XMD
overlap is a live-validation question; a user-XMD PUT
example is not a main-XMD GET fixture. Zero-row aggregate behavior is provisional:
an explicit zero count is accepted; an empty aggregate array is rejected.
Case/Unicode fixtures test the chosen synthetic order, not Salesforce collation.
Benchmark actual SAQL query cost and offsets before claiming volume support; no
quadratic physical cost or generally usable binary bulk-export path is established.
