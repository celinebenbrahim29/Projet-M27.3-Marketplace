"""Chargement du Data Warehouse PostgreSQL : staging -> dwh -> analytics.

Tout le chargement d'une date se fait dans UNE transaction (voir `load`).
Règle d'idempotence : pour une date donnée, on SUPPRIME puis on RÉINSÈRE ses lignes
(staging.orders et dwh.fact_orders). Rejouer le même jour redonne donc le même résultat,
sans doublon. Les dimensions, elles, sont mises à jour par upsert (ON CONFLICT).
"""
import psycopg2
from psycopg2.extras import execute_values

from . import config

# Colonnes de chaque dimension dans staging ; la première est la clé métier.
STAGING_DIMENSIONS = {
    "sellers": ["seller_id", "seller_name", "region"],
    "products": ["product_id", "seller_id", "product_name", "category", "price"],
    "customers": ["customer_id", "customer_name"],
}
STAGING_ORDER_COLUMNS = [
    "order_id", "dt", "seller_id", "product_id", "customer_id",
    "quantity", "unit_price", "amount", "status",
]

# --- staging -> dwh : dimensions (upsert) ---
UPSERT_DIM_SELLER = """
    INSERT INTO dwh.dim_seller (seller_id, seller_name, region)
    SELECT seller_id, seller_name, region FROM staging.sellers
    ON CONFLICT (seller_id) DO UPDATE
    SET seller_name = EXCLUDED.seller_name, region = EXCLUDED.region
"""
UPSERT_DIM_PRODUCT = """
    INSERT INTO dwh.dim_product (product_id, product_name, category, price)
    SELECT product_id, product_name, category, price FROM staging.products
    ON CONFLICT (product_id) DO UPDATE
    SET product_name = EXCLUDED.product_name,
        category = EXCLUDED.category,
        price = EXCLUDED.price
"""
UPSERT_DIM_CUSTOMER = """
    INSERT INTO dwh.dim_customer (customer_id, customer_name)
    SELECT customer_id, customer_name FROM staging.customers
    ON CONFLICT (customer_id) DO UPDATE SET customer_name = EXCLUDED.customer_name
"""
INSERT_DIM_DATE = """
    INSERT INTO dwh.dim_date (date_key, full_date, year, month, day)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

# --- staging -> dwh : table de faits (seules les commandes `completed` sont gardées) ---
INSERT_FACT_ORDERS = f"""
    INSERT INTO dwh.fact_orders
        (order_id, dt, seller_key, product_key, customer_key, date_key,
         quantity, amount, commission)
    SELECT o.order_id, o.dt, s.seller_key, p.product_key, cu.customer_key, dd.date_key,
           o.quantity, o.amount, ROUND(o.amount * {config.COMMISSION_RATE}, 2)
    FROM staging.orders o
    JOIN dwh.dim_seller  s  ON s.seller_id  = o.seller_id
    JOIN dwh.dim_product p  ON p.product_id = o.product_id
    LEFT JOIN dwh.dim_customer cu ON cu.customer_id = o.customer_id
    JOIN dwh.dim_date dd ON dd.full_date = o.dt
    WHERE o.dt = %s AND o.status = 'completed'
"""

# --- dwh -> analytics ---
UPSERT_DAILY_SUMMARY = """
    INSERT INTO analytics.daily_summary
        (dt, total_orders, revenue, average_basket, commission)
    SELECT dt,
           COUNT(DISTINCT order_id),
           SUM(amount),
           ROUND(SUM(amount) / NULLIF(COUNT(DISTINCT order_id), 0), 2),
           SUM(commission)
    FROM dwh.fact_orders
    WHERE dt = %s
    GROUP BY dt
    ON CONFLICT (dt) DO UPDATE
    SET total_orders = EXCLUDED.total_orders,
        revenue = EXCLUDED.revenue,
        average_basket = EXCLUDED.average_basket,
        commission = EXCLUDED.commission
"""

# Anomalie = CA du jour inférieur à 70 % de la moyenne des 7 jours précédents.
# `is_anomaly` reste NULL tant que les 7 jours d'historique ne sont pas disponibles.
UPSERT_SALES_ANOMALIES = f"""
    INSERT INTO analytics.sales_anomalies
        (dt, revenue, average_previous_7_days, ratio, is_anomaly)
    SELECT d.dt, d.revenue, prev.avg_rev,
           ROUND(d.revenue / NULLIF(prev.avg_rev, 0), 4),
           CASE WHEN prev.n = {config.ANOMALY_WINDOW_DAYS}
                THEN d.revenue < {config.ANOMALY_THRESHOLD} * prev.avg_rev
                ELSE NULL END
    FROM analytics.daily_summary d
    CROSS JOIN LATERAL (
        SELECT AVG(revenue) AS avg_rev, COUNT(*) AS n
        FROM analytics.daily_summary p
        WHERE p.dt >= d.dt - INTERVAL '{config.ANOMALY_WINDOW_DAYS} days' AND p.dt < d.dt
    ) prev
    WHERE d.dt = %s
    ON CONFLICT (dt) DO UPDATE
    SET revenue = EXCLUDED.revenue,
        average_previous_7_days = EXCLUDED.average_previous_7_days,
        ratio = EXCLUDED.ratio,
        is_anomaly = EXCLUDED.is_anomaly
"""


def connect():
    """Ouvre une connexion PostgreSQL vers le Data Warehouse."""
    return psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        dbname=config.PG_DATABASE,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
    )


# Chaque étape ci-dessous est idempotente. Le DAG Airflow les appelle une par une (une tâche
# chacune) ; la ligne de commande les enchaîne dans une seule transaction avec `load`.
def load_staging(dt, data, dims=True):
    """Étape 1 : charge les données brutes de la date dans staging."""
    with connect() as db, db.cursor() as cur:
        _stage(cur, dt, data, dims)


def transform_dwh(dt, dims=True):
    """Étape 2 : transforme staging en modèle en étoile (dimensions et faits)."""
    with connect() as db, db.cursor() as cur:
        _transform(cur, dt, dims)


def aggregate_analytics(dt):
    """Étape 3 : calcule les agrégats et les anomalies. Retourne le nombre de faits du jour."""
    with connect() as db, db.cursor() as cur:
        _aggregate(cur, dt)
        return _count_facts(cur, dt)


def load(dt, data, dims=True):
    """Enchaîne les trois étapes dans une seule transaction. Retourne le nombre de faits.

    `dims=False` ne traite que les commandes : les dimensions (vendeurs, produits,
    clients) ne dépendent pas de la date et ne sont chargées qu'une fois par backfill.
    """
    with connect() as db, db.cursor() as cur:
        _stage(cur, dt, data, dims)
        _transform(cur, dt, dims)
        _aggregate(cur, dt)
        return _count_facts(cur, dt)


def _stage(cur, dt, data, dims):
    if dims:
        _load_staging_dimensions(cur, data)
    _load_staging_orders(cur, dt, data["orders"])


def _transform(cur, dt, dims):
    if dims:
        cur.execute(UPSERT_DIM_SELLER)
        cur.execute(UPSERT_DIM_PRODUCT)
        cur.execute(UPSERT_DIM_CUSTOMER)
    cur.execute(
        INSERT_DIM_DATE,
        (int(dt.replace("-", "")), dt, int(dt[:4]), int(dt[5:7]), int(dt[8:10])),
    )
    _rebuild_fact_orders(cur, dt)


def _aggregate(cur, dt):
    cur.execute(UPSERT_DAILY_SUMMARY, (dt,))
    cur.execute(UPSERT_SALES_ANOMALIES, (dt,))


def _count_facts(cur, dt):
    cur.execute("SELECT COUNT(*) FROM dwh.fact_orders WHERE dt = %s", (dt,))
    return cur.fetchone()[0]


def _load_staging_dimensions(cur, data):
    """Upsert des vendeurs, produits et clients dans staging."""
    for kind, columns in STAGING_DIMENSIONS.items():
        values = [tuple(row.get(col) for col in columns) for row in data[kind]]
        updates = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns[1:])
        execute_values(
            cur,
            f"INSERT INTO staging.{kind} ({', '.join(columns)}) VALUES %s "
            f"ON CONFLICT ({columns[0]}) DO UPDATE SET {updates}",
            values,
        )


def _load_staging_orders(cur, dt, orders):
    """Remplace les commandes de la date dans staging (idempotent)."""
    cur.execute("DELETE FROM staging.orders WHERE dt = %s", (dt,))
    values = [
        (o["order_id"], dt, o["seller_id"], o["product_id"], o.get("customer_id"),
         o["quantity"], o["unit_price"], o["amount"], o.get("status"))
        for o in orders
    ]
    execute_values(
        cur,
        f"INSERT INTO staging.orders ({', '.join(STAGING_ORDER_COLUMNS)}) VALUES %s",
        values,
    )


def _rebuild_fact_orders(cur, dt):
    """Remplace les faits de la date dans dwh (idempotent : DELETE puis INSERT)."""
    cur.execute("DELETE FROM dwh.fact_orders WHERE dt = %s", (dt,))
    cur.execute(INSERT_FACT_ORDERS, (dt,))


# --- Suivi des exécutions (analytics.pipeline_runs) ---
def start_run(dt):
    """Enregistre le début d'un run et retourne son identifiant."""
    with connect() as db, db.cursor() as cur:
        cur.execute(
            "INSERT INTO analytics.pipeline_runs (run_date, status) "
            "VALUES (%s, 'running') RETURNING id",
            (dt,),
        )
        return cur.fetchone()[0]


def finish_run(run_id, rows):
    with connect() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE analytics.pipeline_runs "
            "SET status = 'success', finished_at = NOW(), rows_loaded = %s WHERE id = %s",
            (rows, run_id),
        )


def fail_run(run_id, error):
    with connect() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE analytics.pipeline_runs "
            "SET status = 'failed', finished_at = NOW(), error_message = %s WHERE id = %s",
            (str(error)[:1000], run_id),
        )


def record_run(dt, status, rows=None, error=None):
    """Enregistre en une fois un run terminé (utilisé par le DAG Airflow)."""
    with connect() as db, db.cursor() as cur:
        cur.execute(
            "INSERT INTO analytics.pipeline_runs "
            "(run_date, status, finished_at, rows_loaded, error_message) "
            "VALUES (%s, %s, NOW(), %s, %s)",
            (dt, status, rows, error and str(error)[:1000]),
        )


def count_facts(dt):
    with connect() as db, db.cursor() as cur:
        return _count_facts(cur, dt)
