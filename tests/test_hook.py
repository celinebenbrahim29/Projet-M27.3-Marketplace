"""Tests du MarketplaceAPIHook (nécessitent Airflow : ignorés dans le conteneur `pipeline`).

Lancer dans le conteneur Airflow :
    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/test_hook.py -v
"""
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow")

from marketplace import api_client  # noqa: E402
from marketplace_api_hook import MarketplaceAPIHook  # noqa: E402


@pytest.fixture
def hook(monkeypatch):
    """Hook dont la Connection Airflow est simulée (aucune base Airflow requise)."""
    connection = SimpleNamespace(host="http://api:5000", password="conn-token")
    monkeypatch.setattr(MarketplaceAPIHook, "get_connection", lambda self, conn_id: connection)
    return MarketplaceAPIHook()


def test_hook_reads_url_and_token_from_the_connection(monkeypatch, hook):
    seen = {}

    def fake_get(url, headers, params, timeout):
        seen.update(url=url, headers=headers)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: [{"order_id": "o1"}])

    monkeypatch.setattr(api_client.requests, "get", fake_get)
    assert hook.get_orders("2026-04-07") == [{"order_id": "o1"}]
    assert seen["url"] == "http://api:5000/orders"
    assert seen["headers"]["Authorization"] == "Bearer conn-token"


def test_hook_default_connection_is_marketplace_api():
    assert MarketplaceAPIHook.default_conn_name == "marketplace_api"
    assert MarketplaceAPIHook().marketplace_conn_id == "marketplace_api"


def test_hook_exposes_the_expected_methods(hook):
    for method in ("get_orders", "get_sellers", "get_products", "fetch_dataset", "check_health"):
        assert callable(getattr(hook, method))
