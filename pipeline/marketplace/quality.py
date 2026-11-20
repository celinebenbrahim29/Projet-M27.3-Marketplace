"""Contrôles qualité appliqués aux données avant tout chargement."""
from datetime import date

ID_FIELDS = {
    "orders": "order_id",
    "sellers": "seller_id",
    "products": "product_id",
    "customers": "customer_id",
}


def validate(rows, kind, dt):
    """Lève ValueError si le jeu de données est inexploitable.

    Règles : jeu non vide, identifiants présents et uniques et, pour les commandes,
    date non future, date cohérente avec la partition, montant positif, quantité > 0.
    """
    if not rows:
        raise ValueError(f"Empty dataset: {kind}")

    ids = [row.get(ID_FIELDS[kind]) for row in rows]
    if any(value is None for value in ids) or len(ids) != len(set(ids)):
        raise ValueError(f"Invalid or duplicate IDs: {kind}")

    if kind == "orders":
        if date.fromisoformat(dt) > date.today():
            raise ValueError("Future execution date")
        if any(_invalid_order(row, dt) for row in rows):
            raise ValueError("Invalid orders")


def _invalid_order(order, dt):
    return (
        order.get("order_date") != dt
        or float(order.get("amount", -1)) < 0
        or int(order.get("quantity", 0)) <= 0
    )
