"""Tests des contrôles qualité (aucune base ni réseau nécessaire)."""
from datetime import date, timedelta

import pytest

from marketplace.quality import validate

DT = "2026-04-07"


def order(order_id="o1", **overrides):
    base = {"order_id": order_id, "order_date": DT, "amount": 10.0, "quantity": 1}
    return {**base, **overrides}


def test_valid_orders_pass():
    validate([order("o1"), order("o2")], "orders", DT)


def test_empty_dataset_is_rejected():
    with pytest.raises(ValueError, match="Empty dataset"):
        validate([], "orders", DT)


def test_duplicate_ids_are_rejected():
    with pytest.raises(ValueError, match="duplicate IDs"):
        validate([order("o1"), order("o1")], "orders", DT)


def test_missing_id_is_rejected():
    with pytest.raises(ValueError, match="duplicate IDs"):
        validate([{"seller_id": None}], "sellers", DT)


def test_future_date_is_rejected():
    future = (date.today() + timedelta(days=1)).isoformat()
    with pytest.raises(ValueError, match="Future execution date"):
        validate([order(order_date=future)], "orders", future)


@pytest.mark.parametrize(
    "bad_order",
    [
        order(amount=-1),                 # montant négatif
        order(quantity=0),                # quantité nulle
        order(order_date="2026-01-01"),   # date différente de la partition
    ],
)
def test_invalid_orders_are_rejected(bad_order):
    with pytest.raises(ValueError, match="Invalid orders"):
        validate([bad_order], "orders", DT)
