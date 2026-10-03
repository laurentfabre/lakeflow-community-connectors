"""Regenerate synthetic source-named corpus, never responses recorded from Salesforce.

Run from repository root: .venv/bin/python -m tests.unit.sources.crm_analytics.bootstrap_corpus
The production connector has no static TABLE_SCHEMAS; XMD is its schema authority.
"""

import json

from databricks.labs.community_connector.source_simulator.tools.corpus_from_schema import (
    write_corpus_from_schemas,
)
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics import (
    CrmAnalyticsLakeflowConnect,
)
from tests.unit.sources.crm_analytics.test_crm_analytics_lakeflow_connect import (
    DATASET,
    VERSION,
    OPTIONS,
    ROWS,
    SPEC_DIR,
)


def main(output=None):
    """Generate the legacy collision fixture, reversing aliases via field metadata.

    Legacy mode preserves the established synthetic values and Name/name source
    columns. Default readable mode rejects that collision explicitly; its tests
    rename one synthetic field in memory instead of rewriting this golden corpus.
    """
    dataset = {
        "id": DATASET,
        "datasetType": "Default",
        "currentVersionId": VERSION,
        "name": "SYNTHETIC_Fixture_Only",
        "folder": {"id": "synthetic-folder"},
        "permissions": {"read": True},
    }
    version = {
        "id": VERSION,
        "dataset": {"id": DATASET},
        "totalRowCount": 7,
        "createdDate": "2026-09-01T00:00:00Z",
        "predicate": None,
        "xmdMain": {
            "dimensions": [
                {"field": field} for field in ("Account.Id", "Region", "Name", "name", "État, rôle")
            ],
            "measures": [{"field": "Amount"}],
            "dates": [
                {
                    "type": "Date",
                    "fields": {
                        "fullField": "CloseDate",
                        "epochSecond": "CloseDate_epochSecond",
                        "year": "CloseDate_Year",
                    },
                }
            ],
        },
    }
    schema, mapping, _ = CrmAnalyticsLakeflowConnect._schema(
        version,
        CrmAnalyticsLakeflowConnect._table_options({**OPTIONS, "column_name_mode": "legacy_hex"}),
    )

    class SyntheticXmdAdapter:
        """Expose the dynamic fixture schema to the standard bootstrap tool offline."""

        @staticmethod
        def list_tables():
            return [ROWS]

        @staticmethod
        def get_table_schema(_table, _options):
            return schema

        @staticmethod
        def read_table_metadata(_table, _options):
            return {"primary_keys": [mapping["Account.Id"], mapping["Region"]]}

    output = output or SPEC_DIR / "corpus"
    source_names = {field.name: field.metadata["source_name"] for field in schema}
    generated = write_corpus_from_schemas(SyntheticXmdAdapter(), output, records_per_table=7)
    for path in generated:
        rows = json.loads(path.read_text(encoding="utf-8"))
        source_rows = [{source_names[key]: value for key, value in row.items()} for row in rows]
        for index, row in enumerate(source_rows):
            row["Account.Id"] = f"synthetic-account-{index:04}"
            row["Region"] = "synthetic-region"
            row["CloseDate"] = "2026-09-01"
            row["CloseDate_Year"] = "2026"
            row["CloseDate_epochSecond"] = 1788220800 + index
            row["Amount"] = str(row["Amount"])
        path.write_text(
            json.dumps(source_rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    for name, data in (("datasets", [dataset]), ("versions", [version])):
        (output / f"{name}.json").write_text(
            json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )


if __name__ == "__main__":
    main()
