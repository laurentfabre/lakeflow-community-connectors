"""Fail-closed snapshots of version-pinned Salesforce CRM Analytics datasets.

The independent archival layer must consume the iterator to exhaustion before
publishing. A yielded prefix is never evidence of a complete snapshot. This
module does not implement source-native CDC or OAuth token renewal.
"""

from __future__ import annotations

import json
import math
import random
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Iterator
from urllib.parse import parse_qs, urlsplit, urlunsplit

import requests
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_support import (
    CrmAnalyticsError,
    _ReadBudget,
    _field_list,
    _field_name,
    _fingerprint,
    _integer,
    _response_body,
    _unknown_columns_message,
    decode_column_name,
    encode_column_name,
)

_ID = re.compile(r"[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?\Z")
_RETRY_STATUSES = {429, 500, 502, 503, 504}
_GENERATED_VERSION_COLUMN = "_crm_analytics_version_id"
_DATE_PARTS = {
    "day",
    "epochDay",
    "epochSecond",
    "fiscalMonth",
    "fiscalQuarter",
    "fiscalWeek",
    "fiscalYear",
    "fullField",
    "hour",
    "minute",
    "month",
    "quarter",
    "second",
    "week",
    "year",
}


def _identifier(value, name: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise CrmAnalyticsError(f"{name} must be a 15- or 18-character Salesforce ID")
    return value


def _is_default_dataset_type(value) -> bool:
    # The datasetTypes request enum is "Default"; resource responses can use
    # lowercase "default". Type casing does not change the supported scope.
    return isinstance(value, str) and value.casefold() == "default"


class CrmAnalyticsLakeflowConnect(LakeflowConnect):
    """Read complete snapshots of scalar, full-visibility Default datasets.

    Connection: instance_url, access_token, api_version; optional page_size
    (1..10000, default 1000), timeout_seconds (1..300, default 30), max_retries
    (0..8, default 3), max_retry_delay_seconds (1..120, default 30).

    Table: source_primary_keys (JSON array of source names), supported_scope exactly
    scalar_full_visibility, scope_fingerprint (operator-managed stable scope
    identity), optional version_id, decimal_precision (default 38), decimal_scale
    (default 18), max_keys (default 1000000), column_name_mode (readable by default,
    or legacy_hex for prototype schemas), selected_columns (exact source names),
    max_query_requests (default 2000), max_read_seconds (default 1800), and
    auto_increment_column (optional output name for raw per-version row IDs).
    Generated mode preserves duplicate/null business keys and uses the version
    ID plus row number as destination keys; source_primary_keys becomes optional.
    Exceeding limits fails, never truncates.
    Instances are for sequential use, not concurrent threads. Call close() to
    release pooled HTTP connections; a later request lazily opens a new session.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        raw = options.get("instance_url", "")
        try:
            parsed = urlsplit(raw)
            good_origin = (
                parsed.scheme == "https"
                and parsed.hostname
                and parsed.hostname.endswith(".salesforce.com")
                and parsed.port in (None, 443)
                and not parsed.username
                and not parsed.password
                and parsed.path in ("", "/")
                and not parsed.query
                and not parsed.fragment
                and parsed.netloc.isascii()
                and "%" not in parsed.netloc
                and "\\" not in raw
            )
        except (TypeError, ValueError):
            good_origin = False
        if not good_origin:
            raise CrmAnalyticsError("instance_url must be a trusted HTTPS *.salesforce.com origin")
        self._origin = f"https://{parsed.hostname}"
        token = options.get("access_token")
        if not isinstance(token, str) or not token or any(ord(c) < 32 for c in token):
            raise CrmAnalyticsError("A nonempty injected OAuth access_token is required")
        self._token = token
        version = options.get("api_version", "")
        if (
            not isinstance(version, str)
            or not re.fullmatch(r"[0-9]{2,3}\.0", version)
            or int(version[:-2]) < 53
        ):
            raise CrmAnalyticsError("An explicit supported api_version >= 53.0 is required")
        self._base = f"/services/data/v{version}/wave"
        self._page_size = _integer(options.get("page_size", "1000"), "page_size", 1, 10000)
        self._timeout = _integer(options.get("timeout_seconds", "30"), "timeout_seconds", 1, 300)
        self._retries = _integer(options.get("max_retries", "3"), "max_retries", 0, 8)
        self._delay_cap = _integer(
            options.get("max_retry_delay_seconds", "30"), "max_retry_delay_seconds", 1, 120
        )
        self._contexts: dict[str, dict] = {}
        self._evidence: dict[str, dict] = {}
        self._read_epochs: dict[str, int] = {}
        self._session = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.trust_env = False
        return self._session

    def close(self) -> None:
        """Release HTTP resources without discarding pinned snapshot contexts."""
        session = getattr(self, "_session", None)
        self._session = None
        if session is not None:
            session.cookies.clear()
            session.close()

    def __getstate__(self) -> dict:
        # Spark ships the connector to executors. Live sockets/cookies must stay
        # local; serialization must not close the original instance's session.
        return {**self.__dict__, "_session": None}

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._session = None

    def _delay(self, response, attempt: int) -> float:
        delay = min(self._delay_cap, (2**attempt) + random.random())
        raw = response.headers.get("Retry-After") if response is not None else None
        if raw:
            try:
                wait = float(raw)
            except (ValueError, TypeError):
                try:
                    date = parsedate_to_datetime(raw)
                    if date.tzinfo is None:
                        date = date.replace(tzinfo=timezone.utc)
                    wait = (date - datetime.now(timezone.utc)).total_seconds()
                except (ValueError, TypeError, OverflowError):
                    wait = 0
            if not math.isnan(wait) and wait >= 0:
                if wait > self._delay_cap:
                    raise CrmAnalyticsError(
                        "Retry-After exceeds bounded retry policy; retry the snapshot later"
                    )
                delay = min(self._delay_cap, max(delay, wait))
        return delay

    def _request(self, method: str, path: str, *, params=None, payload=None, budget=None) -> dict:
        if not path.startswith(self._base + "/") or "\\" in path:
            raise CrmAnalyticsError("Refusing an unexpected API resource path")
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        session = self._get_session()
        for attempt in range(self._retries + 1):
            response = None
            try:
                # Reuse TCP/TLS connections, never source cookie authentication.
                # OAuth remains an explicit per-request header, without netrc,
                # ambient proxies, or implicit redirect forwarding.
                session.cookies.clear()
                timeout = (
                    budget.request_timeout(
                        self._timeout, query=method == "POST" and path == self._base + "/query"
                    )
                    if budget is not None
                    else (self._timeout, self._timeout)
                )
                response = session.request(
                    method,
                    self._origin + path,
                    params=params,
                    json=payload,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=False,
                )
            except (requests.Timeout, requests.ConnectionError):
                if budget is not None:
                    budget.remaining()
                if attempt == self._retries:
                    raise CrmAnalyticsError(
                        "CRM Analytics request failed after bounded retries"
                    ) from None
            except requests.RequestException:
                raise CrmAnalyticsError("CRM Analytics request failed") from None
            else:
                if budget is not None:
                    budget.remaining()
                status = response.status_code
                if status == 401:
                    raise CrmAnalyticsError(
                        "CRM Analytics authentication expired or invalid; renew the injected token externally"
                    )
                if status not in _RETRY_STATUSES:
                    if status != 200:
                        raise CrmAnalyticsError(f"CRM Analytics HTTP {status}; snapshot aborted")
                    body = _response_body(response)
                    if budget is not None:
                        budget.remaining()
                    return body
                if attempt == self._retries:
                    raise CrmAnalyticsError(f"CRM Analytics HTTP {status} after bounded retries")
            finally:
                session.cookies.clear()
                if response is not None:
                    response.close()
            delay = self._delay(response, attempt)
            if budget is None:
                time.sleep(delay)
            else:
                budget.sleep(delay, query=method == "POST" and path == self._base + "/query")
        raise AssertionError("unreachable")

    def _next_link(self, link: str) -> str:
        if not isinstance(link, str) or "\\" in link or link.startswith("//"):
            raise CrmAnalyticsError("Unsafe dataset pagination link")
        try:
            parsed = urlsplit(link)
            if parsed.scheme or parsed.netloc:
                if (
                    parsed.scheme != "https"
                    or parsed.hostname != urlsplit(self._origin).hostname
                    or parsed.port not in (None, 443)
                    or parsed.username
                    or parsed.password
                ):
                    raise ValueError("foreign origin")
            if parsed.path != self._base + "/datasets" or parsed.fragment:
                raise ValueError("unexpected path")
            query = parse_qs(parsed.query, strict_parsing=True, keep_blank_values=True)
            if set(query) - {"page", "pageSize", "hasCurrentOnly", "datasetTypes"}:
                raise ValueError("unexpected filter")
            if any(len(v) != 1 or not v[0] for v in query.values()) or not query.get("page"):
                raise ValueError("invalid page token")
            if query.get("datasetTypes", ["Default"]) != ["Default"]:
                raise ValueError("changed dataset scope")
            if query.get("hasCurrentOnly", ["true"]) != ["true"]:
                raise ValueError("changed version scope")
            if "pageSize" in query:
                _integer(query["pageSize"][0], "discovery pageSize", 1, 200)
            return urlunsplit(("", "", parsed.path, parsed.query, ""))
        except (ValueError, TypeError):
            raise CrmAnalyticsError("Unsafe dataset pagination link") from None

    def list_tables(self) -> list[str]:
        path = self._base + "/datasets"
        params = {"pageSize": "200", "hasCurrentOnly": "true", "datasetTypes": "Default"}
        names, seen_links, expected = [], set(), None
        while True:
            if path in seen_links:
                raise CrmAnalyticsError("Cycle in dataset discovery pagination")
            seen_links.add(path)
            body = self._request("GET", path, params=params)
            rows = body.get("datasets")
            if not isinstance(rows, list) or len(rows) > 200:
                raise CrmAnalyticsError("Malformed dataset discovery page")
            total = _integer(body.get("totalSize"), "dataset totalSize", 0, 2**63 - 1)
            if expected is not None and total != expected:
                raise CrmAnalyticsError("Dataset discovery changed during pagination")
            expected = total
            for dataset in rows:
                if not isinstance(dataset, dict) or not _is_default_dataset_type(
                    dataset.get("datasetType")
                ):
                    raise CrmAnalyticsError("Unexpected dataset type in discovery")
                name = _identifier(dataset.get("id"), "dataset id")
                _identifier(dataset.get("currentVersionId"), "current version id")
                if name in names:
                    raise CrmAnalyticsError("Duplicate dataset in discovery")
                names.append(name)
            if len(names) > expected:
                raise CrmAnalyticsError("Dataset discovery exceeds declared count")
            next_url = body.get("nextPageUrl")
            if next_url is None:
                if len(names) != expected:
                    raise CrmAnalyticsError("Incomplete dataset discovery")
                return names
            if not rows or len(names) >= expected:
                raise CrmAnalyticsError("Non-progressing dataset discovery pagination")
            path, params = self._next_link(next_url), None

    @staticmethod
    def _table_options(table_options: dict[str, str]) -> dict:
        # Spark passes connection and framework options to schema()/read_table()
        # together with table options. Normalize casing (CaseInsensitiveDict)
        # and discard only the explicit transport names, never arbitrary filters.
        transport = {
            "tablename",
            "tablenamelist",
            "tableconfigs",
            "source_name",
            "databricks.connection",
            "instance_url",
            "access_token",
            "access_token_expiration",
            "api_version",
            "page_size",
            "timeout_seconds",
            "max_retries",
            "max_retry_delay_seconds",
            # COMMUNITY OAuth connection-layer keys (see CLI OAUTH_OPTION_KEYS).
            # They do not alter source extraction and must never enter fingerprints.
            "community_oauth_flow",
            "client_id",
            "client_secret",
            "oauth_scope",
            "token_endpoint",
            "authorization_endpoint",
            "authorization_code",
            "pkce_verifier",
            "oauth_redirect_uri",
            "oauth_provider",
            "oauth_credential_exchange_method",
            "expires_in_secs",
            "refresh_token",
        }
        options = {
            str(k).lower(): v
            for k, v in (table_options or {}).items()
            if str(k).lower() not in transport
        }
        allowed = {
            "source_primary_keys",
            "primary_keys",
            "version_id",
            "supported_scope",
            "scope_fingerprint",
            "decimal_precision",
            "decimal_scale",
            "max_keys",
            "column_name_mode",
            "selected_columns",
            "auto_increment_column",
            "max_query_requests",
            "max_read_seconds",
        }
        if set(options) - allowed:
            raise CrmAnalyticsError("Unknown table option; filters and row caps are not supported")
        if options.get("supported_scope") != "scalar_full_visibility":
            raise CrmAnalyticsError(
                "Attest supported_scope=scalar_full_visibility after checking physical fields and visibility"
            )
        scope = options.get("scope_fingerprint")
        if not isinstance(scope, str) or not scope.strip() or len(scope) > 1024:
            raise CrmAnalyticsError("A stable operator-managed scope_fingerprint is required")
        mode = options.get("column_name_mode", "readable")
        if mode not in ("readable", "legacy_hex"):
            raise CrmAnalyticsError("column_name_mode must be readable or legacy_hex")
        options["column_name_mode"] = mode
        auto_column = options.get("auto_increment_column")
        if "auto_increment_column" in options and (
            not isinstance(auto_column, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", auto_column)
            or auto_column.lower() == _GENERATED_VERSION_COLUMN.lower()
        ):
            raise CrmAnalyticsError(
                "auto_increment_column must be an ASCII output identifier of 1..128 characters "
                "and must differ from the reserved generated version column"
            )
        keys = (
            []
            if auto_column and "source_primary_keys" not in options
            else _field_list(options.get("source_primary_keys"), "source_primary_keys")
        )
        options["source_primary_keys"] = keys
        if "selected_columns" in options:
            selected = _field_list(options["selected_columns"], "selected_columns")
            options["selected_columns"] = sorted(selected)
            if not set(keys).issubset(selected):
                raise CrmAnalyticsError(
                    "selected_columns must include every source_primary_keys field"
                )
        # primary_keys is a shared pipeline destination override. If present,
        # accept only this naming mode's counterpart, preventing a wrong merge key.
        if "primary_keys" in options:
            destination_keys = options.pop("primary_keys")
            if isinstance(destination_keys, str):
                try:
                    destination_keys = json.loads(destination_keys)
                except ValueError:
                    raise CrmAnalyticsError(
                        "primary_keys override must contain mapped destination names"
                    ) from None
            expected_keys = (
                [_GENERATED_VERSION_COLUMN, auto_column]
                if auto_column
                else [encode_column_name(_field_name(k), mode) for k in keys]
            )
            if destination_keys != expected_keys:
                raise CrmAnalyticsError(
                    "primary_keys override does not match generated version/row keys"
                    if auto_column
                    else "primary_keys override does not match mapped source_primary_keys"
                )
        precision = _integer(options.get("decimal_precision", "38"), "decimal_precision", 1, 38)
        options["decimal_precision"] = precision
        options["decimal_scale"] = _integer(
            options.get("decimal_scale", "18"), "decimal_scale", 0, precision
        )
        options["max_keys"] = _integer(options.get("max_keys", "1000000"), "max_keys", 1, 10000000)
        options["max_query_requests"] = _integer(
            options.get("max_query_requests", "2000"), "max_query_requests", 1, 100000
        )
        options["max_read_seconds"] = _integer(
            options.get("max_read_seconds", "1800"), "max_read_seconds", 1, 86400
        )
        if "version_id" in options:
            options["version_id"] = _identifier(options["version_id"], "version_id")
        return options

    def _dataset(self, name: str, budget=None) -> dict:
        body = self._request("GET", self._base + f"/datasets/{name}", budget=budget)
        if body.get("id") != name or not _is_default_dataset_type(body.get("datasetType")):
            raise CrmAnalyticsError("Dataset identity/type changed or is unsupported")
        return body

    def _version(self, name: str, version: str, budget=None) -> dict:
        body = self._request(
            "GET",
            self._base + f"/datasets/{name}/versions/{version}",
            params={"filterGroup": "Medium"},
            budget=budget,
        )
        if (
            body.get("id") != version
            or not isinstance(body.get("dataset"), dict)
            or body["dataset"].get("id") != name
        ):
            raise CrmAnalyticsError("Dataset version response identity mismatch")
        _integer(body.get("totalRowCount"), "version totalRowCount", 0, 2**63 - 1)
        return body

    @staticmethod
    def _created_at(version: dict) -> str:
        value = version.get("createdDate")
        try:
            date = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if date.tzinfo is None:
                raise ValueError("missing timezone")
        except (AttributeError, TypeError, ValueError):
            raise CrmAnalyticsError(
                "Pinned version requires a timezone-qualified createdDate"
            ) from None
        return date.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _date_columns(dates, explicit: bool):
        if not isinstance(dates, list):
            raise CrmAnalyticsError("Malformed XMD dates")
        for date in dates:
            if not isinstance(date, dict):
                raise CrmAnalyticsError("Malformed XMD date")
            date_type = date.get("type")
            if date_type is not None and not isinstance(date_type, str):
                raise CrmAnalyticsError("Malformed XMD date type")
            supported_date = date_type in {None, "Date", "DateOnly", "DateTime"}
            fields = date.get("fields")
            if (
                not isinstance(fields, dict)
                or not fields
                or any(not isinstance(part, str) or not part for part in fields)
            ):
                raise CrmAnalyticsError("Malformed XMD physical date fields")
            if not explicit and (not supported_date or set(fields) - _DATE_PARTS):
                raise CrmAnalyticsError("Unsupported XMD physical date fields or date type")
            for part, name in fields.items():
                if name is not None:
                    yield (
                        name,
                        "long" if part in {"epochDay", "epochSecond"} else "string",
                        supported_date and part in _DATE_PARTS,
                    )

    @staticmethod
    def _schema(version: dict, options: dict) -> tuple[StructType, dict, dict]:
        xmd = version.get("xmdMain")
        if not isinstance(xmd, dict) or xmd.get("errorMessage"):
            raise CrmAnalyticsError(
                "Pinned version lacks usable main XMD; filterGroup=Medium is required"
            )
        selected = set(options["selected_columns"]) if "selected_columns" in options else None
        kinds = {}
        physical = set()

        def identity(name):
            # Projection can exclude an unsafe SAQL identifier, but never hide
            # malformed metadata (including a missing or non-string identity).
            if not isinstance(name, str) or not name:
                raise CrmAnalyticsError("Malformed main XMD field identity")
            return name

        def add(name, kind, *, supported=True):
            name = identity(name)
            if supported:
                physical.add(name)
            if selected is not None and name not in selected:
                return
            name = _field_name(name)
            if not supported:
                raise CrmAnalyticsError("Unsupported XMD physical date fields or date type")
            if name in kinds and kinds[name] != kind:
                raise CrmAnalyticsError("Conflicting physical column types in XMD")
            kinds[name] = kind

        for group, kind in (
            ("dimensions", "string"),
            ("measures", "decimal"),
            ("derivedDimensions", "derived"),
            ("derivedMeasures", "derived"),
        ):
            values = xmd.get(group, [])
            if not isinstance(values, list):
                raise CrmAnalyticsError("Malformed main XMD columns")
            for entry in values:
                if not isinstance(entry, dict):
                    raise CrmAnalyticsError("Malformed main XMD column")
                name = identity(entry.get("field"))
                if kind == "derived":
                    if selected is None or name in selected:
                        raise CrmAnalyticsError("Derived/calculated XMD columns are not supported")
                else:
                    add(name, kind)
        for name, kind, supported in CrmAnalyticsLakeflowConnect._date_columns(
            xmd.get("dates", []), selected is not None
        ):
            add(name, kind, supported=supported)
        if selected is not None and set(kinds) != selected:
            raise CrmAnalyticsError(
                _unknown_columns_message(
                    selected - set(kinds), physical, options["column_name_mode"]
                )
            )
        if not kinds:
            raise CrmAnalyticsError("Pinned XMD contains no supported physical columns")
        auto_column = options.get("auto_increment_column")
        for key in options["source_primary_keys"]:
            if key not in kinds or (not auto_column and kinds[key] != "string"):
                raise CrmAnalyticsError(
                    "Each primary key must be a scalar dimension/physical date string in XMD"
                )
        mode = options["column_name_mode"]
        mapping = {name: encode_column_name(name, mode) for name in sorted(kinds)}
        # Delta resolves columns case-insensitively. Never silently suffix or
        # rename a collision: that would make names change as a schema evolves.
        if len({target.lower() for target in mapping.values()}) != len(mapping):
            raise CrmAnalyticsError(
                "Case-insensitive destination column collision; rename source fields "
                "or exclude a non-key field with selected_columns, "
                "or explicitly select column_name_mode=legacy_hex"
            )
        types = {
            "string": StringType(),
            "long": LongType(),
            "decimal": DecimalType(options["decimal_precision"], options["decimal_scale"]),
        }
        schema = StructType(
            [
                StructField(mapping[name], types[kind], True, {"source_name": name})
                for name, kind in sorted(kinds.items())
            ]
        )
        if auto_column:
            generated_names = {_GENERATED_VERSION_COLUMN.lower(), auto_column.lower()}
            if generated_names.intersection(name.lower() for name in mapping.values()):
                raise CrmAnalyticsError(
                    "A generated column collides with a physical destination column"
                )
            schema.add(
                StructField(
                    _GENERATED_VERSION_COLUMN,
                    StringType(),
                    False,
                    {"crm_analytics_generated": "dataset_version"},
                )
            )
            schema.add(
                StructField(
                    auto_column, LongType(), False, {"crm_analytics_generated": "row_number"}
                )
            )
        return schema, mapping, kinds

    @staticmethod
    def _security(dataset: dict, version: dict) -> dict:
        folder = dataset.get("folder")
        if folder is not None and not isinstance(folder, dict):
            raise CrmAnalyticsError("Malformed dataset folder/security metadata")
        return {
            "dataset": {
                k: dataset.get(k) for k in ("permissions", "visibility", "licenseAttributes")
            },
            "folder_id": (folder or {}).get("id"),
            "version": {
                k: version.get(k)
                for k in (
                    "predicate",
                    "predicateVersion",
                    "sharingSource",
                    "permissions",
                    "securityCoverageUrl",
                )
            },
        }

    def _context(
        self, table_name: str, table_options: dict[str, str], budget=None
    ) -> tuple[str, dict]:
        name = _identifier(table_name, "table_name")
        options = self._table_options(table_options)
        # Operational limits do not alter the dataset's identity or cached pin.
        # Each read gets fresh limits rather than inheriting a cached allowance.
        options.pop("max_query_requests")
        options.pop("max_read_seconds")
        key = _fingerprint([name, options])
        if key not in self._contexts:
            dataset = self._dataset(name, budget=budget)
            version_id = options.get("version_id") or _identifier(
                dataset.get("currentVersionId"), "currentVersionId"
            )
            version = self._version(name, version_id, budget=budget)
            schema, mapping, kinds = self._schema(version, options)
            security = self._security(dataset, version)
            auto_column = options.get("auto_increment_column")
            descriptor = {
                "dataset_id": name,
                "version_id": version_id,
                "source_created_at": self._created_at(version),
                "expected_row_count": int(version["totalRowCount"]),
                "source_primary_keys": options["source_primary_keys"],
                "primary_keys": (
                    [_GENERATED_VERSION_COLUMN, auto_column]
                    if auto_column
                    else [mapping[k] for k in options["source_primary_keys"]]
                ),
                "column_name_mode": options["column_name_mode"],
                "projection_mode": "explicit" if "selected_columns" in options else "all_physical",
                "selected_columns": options.get("selected_columns"),
                "column_mapping": mapping,
                "schema_json": schema.jsonValue(),
                "schema_fingerprint": _fingerprint(schema.jsonValue()),
                "security_fingerprint": _fingerprint(security),
                "scope_fingerprint": options["scope_fingerprint"],
                "configuration_fingerprint": _fingerprint(
                    {
                        "origin": self._origin,
                        "api": self._base,
                        "dataset_id": name,
                        "keys": options["source_primary_keys"],
                        "column_name_mode": options["column_name_mode"],
                        "projection_mode": (
                            "explicit" if "selected_columns" in options else "all_physical"
                        ),
                        "selected_columns": options.get("selected_columns"),
                        "schema": schema.jsonValue(),
                        "scope": options["scope_fingerprint"],
                        "supported_scope": options["supported_scope"],
                        **({"auto_increment_column": auto_column} if auto_column else {}),
                    }
                ),
            }
            if auto_column:
                descriptor.update(
                    auto_increment_column=auto_column,
                    key_mode="version_row_number",
                )
            self._contexts[key] = {"descriptor": descriptor, "options": options, "kinds": kinds}
        return key, self._contexts[key]

    def pin_table(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Resolve once, returning a JSON-safe snapshot descriptor (no credentials).

        For driver/executor consistency, put descriptor['version_id'] in table
        options *before* independent Spark schema and read invocations. A pin is
        not completion evidence; call get_read_evidence only after exhaustion.
        """
        _, context = self._context(table_name, table_options)
        return json.loads(json.dumps(context["descriptor"]))

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        return StructType.fromJson(self.pin_table(table_name, table_options)["schema_json"])

    def describe_snapshot(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Archival adapter descriptor; does not mark a snapshot complete.

        Copy source_version into the explicit version_id table option before
        creating another connector instance or independent Spark read/schema.
        """
        pin = self.pin_table(table_name, table_options)
        return {
            **pin,
            "source_version": pin["version_id"],
            "row_count": pin["expected_row_count"],
            "metadata_fingerprint": _fingerprint(
                {
                    k: pin[k]
                    for k in (
                        "schema_fingerprint",
                        "security_fingerprint",
                        "configuration_fingerprint",
                    )
                }
            ),
        }

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        return {"ingestion_type": "snapshot", **self.pin_table(table_name, table_options)}

    def get_read_evidence(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return evidence only after this instance's latest iterator completed."""
        key, _ = self._context(table_name, table_options)
        if key not in self._evidence:
            raise CrmAnalyticsError("Snapshot iterator has not completed validation")
        return dict(self._evidence[key])

    def _query(self, query: str, budget=None) -> list[dict]:
        body = self._request(
            "POST",
            self._base + "/query",
            payload={"queryLanguage": "SAQL", "query": query},
            budget=budget,
        )
        results = body.get("results")
        rows = results.get("records") if isinstance(results, dict) else None
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise CrmAnalyticsError("Malformed SAQL results.records")
        if budget is not None:
            budget.remaining()
        return rows

    def _count(self, load: str, expected: int, budget=None) -> int:
        rows = self._query(
            load + " q = group q by all; q = foreach q generate count() as '__row_count';",
            budget=budget,
        )
        # An empty aggregate array has not yet been corroborated live. It is
        # deliberately not accepted as zero: Phase 2 must establish its shape.
        if len(rows) != 1 or set(rows[0]) != {"__row_count"}:
            raise CrmAnalyticsError("Unsupported or malformed SAQL count response")
        count = _integer(rows[0]["__row_count"], "visible row count", 0, 2**63 - 1)
        if count != expected:
            raise CrmAnalyticsError("Visible query count differs from pinned version totalRowCount")
        return count

    @staticmethod
    def _value(value, kind: str, options: dict):
        if value is None:
            return None
        if kind == "string":
            if not isinstance(value, str):
                raise CrmAnalyticsError("Non-scalar/non-string dimension or physical date value")
            return value
        if isinstance(value, (bool, dict, list, float)):
            raise CrmAnalyticsError("Unsupported numeric physical value")
        if not isinstance(value, (str, int, Decimal)):
            raise CrmAnalyticsError("Unsupported numeric physical value")
        try:
            number = Decimal(value)
        except (InvalidOperation, ValueError):
            raise CrmAnalyticsError("Malformed numeric physical value") from None
        if not number.is_finite():
            raise CrmAnalyticsError("Non-finite numeric physical value")
        if kind == "long":
            if number != number.to_integral_value() or not -(2**63) <= number < 2**63:
                raise CrmAnalyticsError("Physical epoch value is fractional or overflows LongType")
            return int(number)
        # Work from the tuple, not context-sensitive quantize/normalize: Python's
        # default Decimal precision 28 must never round a DecimalType(38, s).
        _, digits, exponent = number.as_tuple()
        trimmed = list(digits)
        while trimmed and trimmed[-1] == 0:
            trimmed.pop()
            exponent += 1
        scale = max(0, -exponent) if trimmed else 0
        integer_digits = max(0, len(trimmed) + exponent) if trimmed else 0
        if (
            scale > options["decimal_scale"]
            or integer_digits > options["decimal_precision"] - options["decimal_scale"]
        ):
            raise CrmAnalyticsError("Measure would overflow or round the configured DecimalType")
        return str(number) if number else "0"  # JSON-compatible; framework performs Spark coercion

    def _revalidate(self, descriptor: dict, options: dict, budget: _ReadBudget) -> None:
        name, version_id = descriptor["dataset_id"], descriptor["version_id"]
        dataset = self._dataset(name, budget=budget)
        version = self._version(name, version_id, budget=budget)
        schema, _, _ = self._schema(version, options)
        if (
            int(version["totalRowCount"]) != descriptor["expected_row_count"]
            or self._created_at(version) != descriptor["source_created_at"]
            or _fingerprint(schema.jsonValue()) != descriptor["schema_fingerprint"]
            or _fingerprint(self._security(dataset, version)) != descriptor["security_fingerprint"]
        ):
            raise CrmAnalyticsError(
                "Pinned schema/count/security metadata changed during extraction"
            )
        budget.remaining()

    def _convert_row(
        self, row: dict, context: dict, row_number: int | None = None
    ) -> tuple[dict, tuple]:
        descriptor, options = context["descriptor"], context["options"]
        mapping = descriptor["column_mapping"]
        if set(row) != set(mapping.values()):
            raise CrmAnalyticsError("Snapshot row differs from the exact pinned projection")
        converted = {
            target: self._value(row[target], context["kinds"][source], options)
            for source, target in mapping.items()
        }
        auto_column = options.get("auto_increment_column")
        if auto_column:
            if row_number is None:
                raise CrmAnalyticsError("Missing generated row number")
            converted[_GENERATED_VERSION_COLUMN] = descriptor["version_id"]
            converted[auto_column] = row_number
        row_key = tuple(converted[k] for k in descriptor["primary_keys"])
        if any(value is None for value in row_key):
            raise CrmAnalyticsError("Null or duplicate primary key in snapshot")
        return converted, row_key

    def read_table(
        self, table_name: str, start_offset: dict | None, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], None]:
        started_at = time.monotonic()
        if start_offset is not None and start_offset != {}:
            raise CrmAnalyticsError("Snapshot reads do not accept incremental offsets")
        budget = _ReadBudget(started_at, self._table_options(table_options))
        key, context = self._context(table_name, table_options, budget=budget)
        self._evidence.pop(key, None)
        epoch = self._read_epochs.get(key, 0) + 1
        self._read_epochs[key] = epoch
        budget.remaining()
        descriptor, options = context["descriptor"], context["options"]
        name, version_id = descriptor["dataset_id"], descriptor["version_id"]
        expected = descriptor["expected_row_count"]
        if expected > options["max_keys"]:
            raise CrmAnalyticsError(
                "Snapshot exceeds configured max_keys validation capacity; increase explicitly"
            )
        # Two count probes and the terminal empty page are mandatory, even for
        # an empty snapshot. This is a lower bound: retries consume more calls.
        minimum_queries = (expected + self._page_size - 1) // self._page_size + 3
        if minimum_queries > budget.max_query_requests:
            raise CrmAnalyticsError(
                "Snapshot requires more than max_query_requests; increase explicitly"
            )
        load = f'q = load "{name}/{version_id}";'
        mapping = descriptor["column_mapping"]
        projection = ", ".join(f"'{source}' as '{target}'" for source, target in mapping.items())
        # A generated number is assigned after retrieval. Order by every physical
        # projected value so rows with duplicate business keys remain repeatable.
        # Equal full projections are indistinguishable; retain every occurrence.
        order_columns = (
            list(mapping.values())
            if options.get("auto_increment_column")
            else descriptor["primary_keys"]
        )
        order = ", ".join(f"'{column}' asc" for column in order_columns)
        query = load + f" q = foreach q generate {projection}; q = order q by ({order});"

        def check_read():
            if self._read_epochs[key] != epoch:
                raise CrmAnalyticsError(
                    "Concurrent reads of the same pinned snapshot are unsupported"
                )
            budget.remaining()

        def iterate():
            check_read()
            before = self._count(load, expected, budget=budget)
            seen = set()
            offset = 0
            while True:
                check_read()
                rows = self._query(
                    query + f" q = offset q {offset}; q = limit q {self._page_size};",
                    budget=budget,
                )
                if len(rows) != min(self._page_size, expected - offset):
                    raise CrmAnalyticsError("Incomplete or overlarge snapshot page")
                if offset == expected:
                    # The exact-length check above requires the terminal page
                    # to be empty; a nonempty terminal page already failed.
                    break
                for index, row in enumerate(rows, start=offset + 1):
                    check_read()
                    converted, row_key = self._convert_row(row, context, row_number=index)
                    if row_key in seen:
                        raise CrmAnalyticsError("Null or duplicate primary key in snapshot")
                    seen.add(row_key)
                    check_read()
                    yield converted
                    check_read()
                offset += len(rows)
            after = self._count(load, expected, budget=budget)
            self._revalidate(descriptor, options, budget)
            check_read()
            self._evidence[key] = {
                "dataset_id": name,
                "version_id": version_id,
                "expected_row_count": expected,
                "visible_count_before": before,
                "visible_count_after": after,
                "extracted_row_count": offset,
                "unique_key_count": len(seen),
                "terminal_empty_page": True,
                "metadata_revalidated": True,
                "scope_fingerprint": options["scope_fingerprint"],
                **(
                    {"key_mode": "version_row_number", "source_keys_validated": False}
                    if options.get("auto_increment_column")
                    else {}
                ),
                **budget.metrics(),
            }

        return iterate(), None
