"""Client credentials (M2M) token renewal; offline, synthetic, no live proof implied."""

import json
import pickle

import pytest
import requests

from databricks.labs.community_connector.sources.crm_analytics.crm_analytics import (
    CrmAnalyticsError,
    CrmAnalyticsLakeflowConnect,
)
from tests.unit.sources.crm_analytics.test_crm_analytics_lakeflow_connect import (
    CONFIG,
    OPTIONS,
    exhaust,
    simulator,  # noqa: F401  (pytest fixture)
)

TOKEN_URL = "https://synthetic-fixture.salesforce.com/services/oauth2/token"
SECRET = "synthetic-client-secret-never-valid"
STALE = "synthetic-stale-injected-token"
M2M_CONFIG = {
    **CONFIG,
    "access_token": STALE,
    "community_oauth_flow": "m2m",
    "client_id": "synthetic-client-id",
    "client_secret": SECRET,
    "token_endpoint": TOKEN_URL,
}


def _response(status, body):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    response._content_consumed = True
    response.headers["Content-Type"] = "application/json"
    return response


@pytest.fixture
def salesforce(simulator, monkeypatch):  # noqa: F811
    """Token endpoint fake in front of the simulator; API rejects revoked tokens."""
    del simulator
    original = requests.Session.request
    state = {"issued": 0, "valid": set(), "token_status": 200, "token_calls": []}

    def request(session, method, url, **kwargs):
        if url == TOKEN_URL:
            state["token_calls"].append(kwargs)
            if state["token_status"] != 200:
                return _response(
                    state["token_status"],
                    {"error": "invalid_grant", "error_description": "no client credentials user"},
                )
            state["issued"] += 1
            token = f"synthetic-fresh-token-{state['issued']}"
            state["valid"].add(token)
            return _response(200, {"access_token": token, "token_type": "Bearer"})
        bearer = kwargs["headers"]["Authorization"].removeprefix("Bearer ")
        if bearer not in state["valid"]:
            return _response(401, [{"errorCode": "INVALID_SESSION_ID"}])
        return original(session, method, url, **kwargs)

    monkeypatch.setattr(requests.Session, "request", request)
    return state


def test_m2m_requests_fresh_token_and_ignores_stale_injected_one(salesforce):
    source = CrmAnalyticsLakeflowConnect(M2M_CONFIG)
    try:
        assert exhaust(source)
    finally:
        source.close()
    assert salesforce["issued"] == 1
    call = salesforce["token_calls"][0]
    assert call["data"]["grant_type"] == "client_credentials"
    assert call["allow_redirects"] is False


def test_m2m_renews_once_on_401_and_replays_request(salesforce):
    source = CrmAnalyticsLakeflowConnect(M2M_CONFIG)
    try:
        assert exhaust(source)
        salesforce["valid"].clear()  # Salesforce session timeout
        assert exhaust(source)
    finally:
        source.close()
    assert salesforce["issued"] == 2


def test_m2m_token_survives_pickling_to_executors(salesforce):
    source = CrmAnalyticsLakeflowConnect(M2M_CONFIG)
    try:
        assert exhaust(source)
        clone = pickle.loads(pickle.dumps(source))
        assert exhaust(clone)
        clone.close()
    finally:
        source.close()
    assert salesforce["issued"] == 1


def test_m2m_persistent_401_fails_after_single_renewal(salesforce, monkeypatch):
    original = requests.Session.request

    def always_unauthorized(session, method, url, **kwargs):
        if url == TOKEN_URL:
            return original(session, method, url, **kwargs)
        return _response(401, [{"errorCode": "INVALID_SESSION_ID"}])

    monkeypatch.setattr(requests.Session, "request", always_unauthorized)
    source = CrmAnalyticsLakeflowConnect(M2M_CONFIG)
    try:
        with pytest.raises(CrmAnalyticsError) as error:
            source.pin_table("0Fb000000000001", OPTIONS)
    finally:
        source.close()
    assert error.value.code == "AUTHENTICATION"
    assert salesforce["issued"] == 2  # initial + one renewal, no loop


def test_m2m_token_failure_is_actionable_and_never_leaks_secret(salesforce):
    salesforce["token_status"] = 400
    source = CrmAnalyticsLakeflowConnect(M2M_CONFIG)
    try:
        with pytest.raises(CrmAnalyticsError) as error:
            source.pin_table("0Fb000000000001", OPTIONS)
    finally:
        source.close()
    message = str(error.value)
    assert error.value.code == "AUTHENTICATION"
    assert "invalid_grant" in message and "Run As" in message
    assert SECRET not in message and "synthetic-client-id" not in message


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://synthetic-fixture.salesforce.com/services/oauth2/token",
        "https://attacker.example/services/oauth2/token",
        "https://synthetic-fixture.salesforce.com.attacker.example/services/oauth2/token",
        "https://synthetic-fixture.salesforce.com/services/oauth2/authorize",
        "https://user:pw@synthetic-fixture.salesforce.com/services/oauth2/token",
    ],
)
def test_m2m_secret_only_sent_to_salesforce_token_endpoint(endpoint):
    with pytest.raises(CrmAnalyticsError, match="token_endpoint"):
        CrmAnalyticsLakeflowConnect({**M2M_CONFIG, "token_endpoint": endpoint})


def test_u2m_and_static_connections_keep_injected_token(salesforce):
    salesforce["valid"].add(STALE)
    config = {**M2M_CONFIG, "community_oauth_flow": "u2m"}
    source = CrmAnalyticsLakeflowConnect(config)
    try:
        assert exhaust(source)
    finally:
        source.close()
    assert salesforce["issued"] == 0


def test_m2m_without_injected_access_token_is_accepted(salesforce):
    config = {k: v for k, v in M2M_CONFIG.items() if k != "access_token"}
    source = CrmAnalyticsLakeflowConnect(config)
    try:
        assert exhaust(source)
    finally:
        source.close()
    assert salesforce["issued"] == 1
