# Synthetic CRM Analytics fixtures

All JSON here is generated synthetic data, not Salesforce-recorded evidence.
The dataset and version IDs, dates, fields, visibility metadata and row values
exist only to exercise the connector contract offline. Source-named fields are
intentionally dotted, Unicode, comma-containing and case-distinct.

Regenerate from repository root with:

```sh
.venv/bin/python -m tests.unit.sources.crm_analytics.bootstrap_corpus
```

The helper uses `write_corpus_from_schemas` against an offline XMD schema adapter,
then maps encoded destination names back to physical source names. The production
connector still discovers all dataset schemas dynamically. Live record-mode
validation of SAQL ordering, offset limits, zero-count shape, XMD and security
semantics remains required; passing these tests does not establish those facts.

The suite also documents a shared-pipeline boundary: `SpecParser` strips the
`primary_keys` override before source validation. The shared ingestion pipeline
then uses that separate override. Source-only override tests do not prove that
path is guarded; omit the override or explicitly use the exact encoded names.
