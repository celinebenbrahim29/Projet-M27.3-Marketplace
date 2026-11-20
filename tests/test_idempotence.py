"""Test d'idempotence sur la vraie base PostgreSQL (ignoré si la base est injoignable).

Principe : charger deux fois la même journée doit donner le même nombre de faits.
Le test utilise une date et des identifiants fictifs, puis nettoie tout ce qu'il a créé.
"""
import inspect

import pytest

from marketplace import warehouse

DT = "2000-01-03"
FAKE_ID = 9_000_001


def fake_data(n_orders=5):
    return {
        "sellers": [{"seller_id": FAKE_ID, "seller_name": "Test", "region": "Paris"}],
        "products": [{"product_id": FAKE_ID, "seller_id": FAKE_ID, "product_name": "Test",
                      "category": "Mode", "price": 10.0}],
        "customers": [{"customer_id": FAKE_ID, "customer_name": "Test"}],
        "orders": [
            {"order_id": f"{DT}-{i:05d}", "order_date": DT, "seller_id": FAKE_ID,
             "product_id": FAKE_ID, "customer_id": FAKE_ID, "quantity": 1,
             "unit_price": 10.0, "amount": 10.0, "status": "completed"}
            for i in range(1, n_orders + 1)
        ],
    }


def cleanup(cur):
    """Supprime les lignes de test, faits d'abord (clés étrangères)."""
    cur.execute("DELETE FROM dwh.fact_orders WHERE dt = %s", (DT,))
    cur.execute("DELETE FROM staging.orders WHERE dt = %s", (DT,))
    cur.execute("DELETE FROM analytics.sales_anomalies WHERE dt = %s", (DT,))
    cur.execute("DELETE FROM analytics.daily_summary WHERE dt = %s", (DT,))
    cur.execute("DELETE FROM dwh.dim_date WHERE full_date = %s", (DT,))
    cur.execute("DELETE FROM dwh.dim_seller WHERE seller_id = %s", (FAKE_ID,))
    cur.execute("DELETE FROM dwh.dim_product WHERE product_id = %s", (FAKE_ID,))
    cur.execute("DELETE FROM dwh.dim_customer WHERE customer_id = %s", (FAKE_ID,))
    cur.execute("DELETE FROM staging.sellers WHERE seller_id = %s", (FAKE_ID,))
    cur.execute("DELETE FROM staging.products WHERE product_id = %s", (FAKE_ID,))
    cur.execute("DELETE FROM staging.customers WHERE customer_id = %s", (FAKE_ID,))


@pytest.fixture
def clean_db():
    try:
        db = warehouse.connect()
    except Exception:
        pytest.skip("PostgreSQL injoignable : test d'intégration ignoré")
    with db, db.cursor() as cur:
        cleanup(cur)
    yield
    with db, db.cursor() as cur:
        cleanup(cur)
    db.close()


def test_loading_same_day_twice_gives_same_count(clean_db):
    first = warehouse.load(DT, fake_data())
    second = warehouse.load(DT, fake_data())
    assert first == second == 5


def test_reloading_replaces_the_day_instead_of_adding(clean_db):
    warehouse.load(DT, fake_data(n_orders=5))
    assert warehouse.load(DT, fake_data(n_orders=3), dims=False) == 3


def test_fact_rebuild_deletes_before_inserting():
    """Garde-fou : le DELETE doit précéder l'INSERT dans la reconstruction des faits."""
    source = inspect.getsource(warehouse._rebuild_fact_orders)
    assert source.index("DELETE FROM dwh.fact_orders") < source.index("INSERT_FACT_ORDERS")
