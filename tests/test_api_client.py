"""Tests du client API : authentification et nouvelles tentatives (requests est simulé)."""
import pytest
import requests

from marketplace import api_client
from marketplace.api_client import MarketplaceClient


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"HTTP {self.status}")

    def json(self):
        return self.payload


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(api_client.time, "sleep", lambda _: None)


@pytest.fixture
def client():
    return MarketplaceClient("http://api:5000/", "secret-token")


def test_sends_bearer_token_and_date(monkeypatch, client):
    seen = {}

    def fake_get(url, headers, params, timeout):
        seen.update(url=url, headers=headers, params=params)
        return FakeResponse([{"order_id": "o1"}])

    monkeypatch.setattr(api_client.requests, "get", fake_get)
    assert client.get_orders("2026-04-07") == [{"order_id": "o1"}]
    assert seen["url"] == "http://api:5000/orders"
    assert seen["headers"]["Authorization"] == "Bearer secret-token"
    assert seen["params"] == {"date": "2026-04-07"}


def test_retries_then_succeeds(monkeypatch, client):
    responses = [FakeResponse([], status=500), FakeResponse([{"id": 1}])]
    monkeypatch.setattr(api_client.requests, "get", lambda *a, **k: responses.pop(0))
    assert client.get_sellers() == [{"id": 1}]


def test_gives_up_after_all_retries(monkeypatch, client):
    calls = []

    def always_fail(*args, **kwargs):
        calls.append(1)
        return FakeResponse([], status=503)

    monkeypatch.setattr(api_client.requests, "get", always_fail)
    with pytest.raises(requests.HTTPError):
        client.get_products()
    assert len(calls) == client.retries


def test_rejects_non_list_payload(monkeypatch, client):
    monkeypatch.setattr(api_client.requests, "get", lambda *a, **k: FakeResponse({"error": "x"}))
    with pytest.raises(ValueError):
        client.get_orders("2026-04-07")


def test_fetch_dataset_routes_to_the_right_endpoint(monkeypatch, client):
    urls = []

    def fake_get(url, **kwargs):
        urls.append(url)
        return FakeResponse([{"id": 1}])

    monkeypatch.setattr(api_client.requests, "get", fake_get)
    client.fetch_dataset("customers", "2026-04-07")
    client.fetch_dataset("orders", "2026-04-07")
    assert urls == ["http://api:5000/customers", "http://api:5000/orders"]
