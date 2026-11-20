"""Configuration du pipeline, lue depuis les variables d'environnement.

Aucun secret ni paramètre de connexion n'est écrit dans le code : ils viennent du fichier
.env (modèle : .env.example) que docker-compose.yml transmet aux conteneurs.
Une variable manquante arrête le programme avec un message explicite.
"""
import os


def _required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name} (voir .env.example)")
    return value


# --- API source ---
API_URL = _required("API_URL")
API_TOKEN = _required("API_TOKEN")
API_RETRIES = 3
API_TIMEOUT_SECONDS = 30

# --- Data Warehouse (PostgreSQL) ---
PG_HOST = _required("PGHOST")
PG_PORT = _required("PGPORT")
PG_DATABASE = _required("POSTGRES_DB")
PG_USER = _required("POSTGRES_USER")
PG_PASSWORD = _required("POSTGRES_PASSWORD")

# --- Data lake (MinIO, compatible S3) ---
MINIO_ENDPOINT = _required("MINIO_ENDPOINT")
MINIO_USER = _required("MINIO_ROOT_USER")
MINIO_PASSWORD = _required("MINIO_ROOT_PASSWORD")
BUCKET = "data-lake"

# --- Règles métier ---
COMMISSION_RATE = 0.12          # commission prélevée par la marketplace sur chaque vente
ANOMALY_WINDOW_DAYS = 7         # fenêtre de la moyenne mobile du chiffre d'affaires
ANOMALY_THRESHOLD = 0.7         # anomalie si le CA du jour < 70 % de la moyenne mobile

# --- Supervision ---
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
