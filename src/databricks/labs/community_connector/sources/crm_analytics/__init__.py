"""Salesforce CRM Analytics community connector."""

from databricks.labs.community_connector.sources.crm_analytics.crm_analytics import (
    CrmAnalyticsLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class CrmAnalyticsDataSource(LakeflowSource):
    _lakeflow_connect_cls = CrmAnalyticsLakeflowConnect


__all__ = ["CrmAnalyticsLakeflowConnect", "CrmAnalyticsDataSource"]
