"""API Marketplace simulée (remplace le backoffice de Maelys Marketplace).

Les données sont générées à la volée et sont DÉTERMINISTES : une même date renvoie
toujours les mêmes commandes (graine = md5 de la date). Cela permet de tester
l'idempotence du pipeline en le rejouant.

Authentification : header `Authorization: Bearer <API_TOKEN>` (sauf /health).
"""
import hashlib
import os
import random
from datetime import date
from functools import lru_cache

from flask import Flask, jsonify, request

app = Flask(__name__)

TOKEN = os.environ.get("API_TOKEN")  # fourni par docker-compose.yml depuis le fichier .env
if not TOKEN:
    raise RuntimeError("Variable d'environnement manquante : API_TOKEN (voir .env.example)")

# Volumes de l'énoncé : 2 400 vendeurs, 180 000 produits. 700 commandes/jour
# donnent ~4,2 M€ de chiffre d'affaires par mois.
N_SELLERS = 2400
N_PRODUCTS = 180000
N_CUSTOMERS = 500
N_ORDERS_PER_DAY = 700

CATEGORIES = ["Mode", "Maison", "Beauté", "Sport", "Electronique"]
REGIONS = ["Paris", "Lyon", "Lille", "Marseille"]


def seed(value):
    """Convertit une chaîne (la date) en graine entière stable."""
    return int(hashlib.md5(value.encode()).hexdigest()[:12], 16)


def sellers():
    return [
        {"seller_id": i, "seller_name": f"Vendeur {i:03d}", "region": REGIONS[i % 4]}
        for i in range(1, N_SELLERS + 1)
    ]


@lru_cache(maxsize=1)
def products():
    """Catalogue complet ; mis en cache car il est identique à chaque appel."""
    return [
        {
            "product_id": i,
            "seller_id": 1 + (i - 1) % N_SELLERS,
            "product_name": f"Produit {i:06d}",
            "category": CATEGORIES[i % 5],
            "price": round(8 + (i * 13) % 190 + 0.99, 2),
        }
        for i in range(1, N_PRODUCTS + 1)
    ]


def customers():
    return [
        {"customer_id": i, "customer_name": f"Client {i:04d}"}
        for i in range(1, N_CUSTOMERS + 1)
    ]


@app.before_request
def check_token():
    if request.path != "/health" and request.headers.get("Authorization") != f"Bearer {TOKEN}":
        return jsonify({"error": "Unauthorized"}), 401


@app.get("/health")
def health():
    return jsonify({"status": "ok", "version": "demo-1.0"})


@app.get("/sellers")
def sellers_route():
    return jsonify(sellers()[: int(request.args.get("limit", N_SELLERS))])


@app.get("/products")
def products_route():
    return jsonify(products())


@app.get("/customers")
def customers_route():
    return jsonify(customers()[: int(request.args.get("limit", N_CUSTOMERS))])


@app.get("/orders")
def orders_route():
    """Commandes d'une journée : /orders?date=YYYY-MM-DD."""
    dt = request.args.get("date", "")
    try:
        date.fromisoformat(dt)
    except ValueError:
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400

    rng = random.Random(seed(dt))
    catalog = products()
    orders = []
    for i in range(N_ORDERS_PER_DAY):
        product = rng.choice(catalog)
        quantity = rng.randint(1, 3)
        orders.append({
            "order_id": f"{dt}-{i + 1:05d}",
            "order_date": dt,
            "seller_id": product["seller_id"],
            "product_id": product["product_id"],
            "customer_id": rng.randint(1, N_CUSTOMERS),
            "quantity": quantity,
            "unit_price": product["price"],
            "amount": round(quantity * product["price"], 2),
            "status": "completed",
        })
    return jsonify(orders)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
