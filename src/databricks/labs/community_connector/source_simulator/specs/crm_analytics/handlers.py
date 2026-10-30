"""Narrow, independently parsed SAQL simulation for source connector tests.

Corpora use source names, not connector-encoded aliases. The handler implements
raw dictionary field lookup (including dots), projection aliases, total ordering
on scalar string keys, offset/limit and count. It is not a general SAQL engine.
"""

import base64
import json
import re
from urllib.parse import parse_qs, urlencode, urlsplit

from requests import Response


def _response(prepared, body, status=200):
    response = Response()
    response.status_code = status
    response.headers["Content-Type"] = "application/json"
    response.encoding = "utf-8"
    response._content = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
    response._content_consumed = True  # Buffered synthetic body has no raw socket to close.
    response.request = prepared
    response.url = prepared.url
    return response


def _error(prepared, message, status=400):
    return _response(prepared, [{"errorCode": "SIMULATOR_CONTRACT", "message": message}], status)


def _params(prepared):
    values = parse_qs(urlsplit(prepared.url).query, keep_blank_values=True)
    if any(len(v) != 1 for v in values.values()):
        raise ValueError("duplicate query parameter")
    return {k: v[0] for k, v in values.items()}


def _entries(corpus, table):
    result = corpus.get(table)
    if not isinstance(result, list) or any(not isinstance(x, dict) for x in result):
        raise ValueError(f"Missing or malformed simulator corpus: {table}")
    return result


def datasets(prepared, _spec, corpus):
    try:
        params = _params(prepared)
        if set(params) - {"pageSize", "page", "datasetTypes", "hasCurrentOnly"}:
            raise ValueError("unsupported discovery parameter")
        size = int(params.get("pageSize", "25"))
        if not 1 <= size <= 200:
            raise ValueError("invalid page size")
        selected = _entries(corpus, "datasets")
        if "datasetTypes" in params:
            selected = [x for x in selected if x.get("datasetType") == params["datasetTypes"]]
        if params.get("hasCurrentOnly") == "true":
            selected = [x for x in selected if x.get("currentVersionId")]
        offset = 0
        if params.get("page"):
            text = base64.urlsafe_b64decode(params["page"].encode()).decode()
            if not re.fullmatch(r"dataset-page:[0-9]+", text):
                raise ValueError("invalid cursor")
            offset = int(text.split(":")[1])
        rows = selected[offset : offset + size]
        next_url = None
        if offset + len(rows) < len(selected):
            params["page"] = base64.urlsafe_b64encode(
                f"dataset-page:{offset + len(rows)}".encode()
            ).decode()
            next_url = urlsplit(prepared.url).path + "?" + urlencode(params)
        return _response(
            prepared, {"datasets": rows, "totalSize": len(selected), "nextPageUrl": next_url}
        )
    except (ValueError, TypeError, UnicodeError):
        return _error(prepared, "invalid discovery request or corpus")


def dataset(prepared, _spec, corpus):
    name = urlsplit(prepared.url).path.rsplit("/", 1)[-1]
    matches = [x for x in _entries(corpus, "datasets") if x.get("id") == name]
    if len(matches) != 1:
        return _error(prepared, "dataset unavailable", 404)
    return _response(prepared, matches[0])


def version(prepared, _spec, corpus):
    path = urlsplit(prepared.url).path.split("/")
    name, version_id = path[-3], path[-1]
    matches = [
        x
        for x in _entries(corpus, "versions")
        if x.get("id") == version_id and x.get("dataset", {}).get("id") == name
    ]
    if len(matches) != 1:
        return _error(prepared, "version unavailable", 404)
    result = dict(matches[0])
    if _params(prepared).get("filterGroup") != "Medium":
        result.pop("xmdMain", None)
    return _response(prepared, result)


def query(prepared, _spec, corpus):
    try:
        payload = json.loads(prepared.body)
        if set(payload) != {"queryLanguage", "query"} or payload["queryLanguage"] != "SAQL":
            raise ValueError("unsupported query language/options")
        statements = [s.strip() for s in payload["query"].split(";") if s.strip()]
        load = re.fullmatch(
            r'q = load "([A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?)/([A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?)"',
            statements[0],
        )
        if not load:
            raise ValueError("query must load a pinned ID/version")
        dataset_id, version_id = load.groups()
        versions = _entries(corpus, "versions")
        if not any(
            v.get("id") == version_id and v.get("dataset", {}).get("id") == dataset_id
            for v in versions
        ):
            return _error(prepared, "version unavailable", 404)
        rows = _entries(corpus, f"rows_{dataset_id}_{version_id}")
        if statements[1:] == [
            "q = group q by all",
            "q = foreach q generate count() as '__row_count'",
        ]:
            records = [{"__row_count": len(rows)}]
        else:
            if len(statements) != 5:
                raise ValueError("unsupported query operation")
            project = re.fullmatch(r"q = foreach q generate (.+)", statements[1])
            order = re.fullmatch(r"q = order q by \((.+)\)", statements[2])
            offset = re.fullmatch(r"q = offset q ([0-9]+)", statements[3])
            limit = re.fullmatch(r"q = limit q ([0-9]+)", statements[4])
            if not all((project, order, offset, limit)):
                raise ValueError("incorrect projection/order/offset/limit sequence")
            # Quoted names may contain commas. Parse full quoted grammar, not
            # comma splitting, and demand the entire projection is consumed.
            # Aliases come from the request; do not derive them from source
            # names or import the production connector's naming policy.
            projection_text = project.group(1)
            projection_matches = list(
                re.finditer(
                    r"(?:'(?P<source>[^'\\\x00-\x1f]+)'|"
                    r"\(case when '(?P<null_source>[^'\\\x00-\x1f]+)' is null then 1 else 0 end\))"
                    r" as '(?P<alias>[A-Za-z_][A-Za-z0-9_]*)'",
                    projection_text,
                )
            )
            if ", ".join(match.group(0) for match in projection_matches) != projection_text:
                raise ValueError("unsupported projection")
            projection = [
                (
                    match.group("source") or match.group("null_source"),
                    match.group("alias"),
                    match.group("null_source") is not None,
                )
                for match in projection_matches
            ]
            aliases = [alias for _, alias, _ in projection]
            if not projection or len({alias.lower() for alias in aliases}) != len(aliases):
                raise ValueError("duplicate/empty projection")
            sort_text = order.group(1)
            sort_keys = re.findall(r"'([A-Za-z_][A-Za-z0-9_]*)' asc", sort_text)
            if ", ".join(f"'{k}' asc" for k in sort_keys) != sort_text or not sort_keys:
                raise ValueError("unsupported ordering")
            if set(sort_keys) - set(aliases):
                raise ValueError("unknown order field")
            size, start = int(limit.group(1)), int(offset.group(1))
            if not 1 <= size <= 10000:
                raise ValueError("unsupported page size")
            projected = [
                {
                    alias: int(row[source] is None) if is_null else row[source]
                    for source, alias, is_null in projection
                }
                for row in rows
            ]
            # Null/malformed key values remain visible to connector validation.
            projected.sort(key=lambda r: tuple((r[k] is not None, str(r[k])) for k in sort_keys))
            records = projected[start : start + size]
        return _response(
            prepared, {"action": "query", "results": {"records": records}, "warnings": []}
        )
    except (ValueError, TypeError, IndexError, KeyError):
        return _error(prepared, "query outside supported simulator contract")
