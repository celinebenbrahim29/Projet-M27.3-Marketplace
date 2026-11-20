"""Configuration pytest : rend importables le package `marketplace` et le Hook Airflow.

Selon l'environnement (machine locale, conteneur `pipeline` ou conteneur Airflow),
ces dossiers n'ont pas le même emplacement : on ajoute ceux qui existent.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Dans les conteneurs, ces variables viennent du .env. En local, on fournit des valeurs
# factices pour pouvoir importer le code : les tests qui ont besoin de la vraie base
# s'ignorent d'eux-mêmes quand elle est injoignable.
for name, value in {
    "API_URL": "http://localhost:5000", "API_TOKEN": "test-token",
    "PGHOST": "localhost", "PGPORT": "5433", "POSTGRES_DB": "dwh",
    "POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test",
    "MINIO_ENDPOINT": "http://localhost:9000",
    "MINIO_ROOT_USER": "test", "MINIO_ROOT_PASSWORD": "test",
}.items():
    os.environ.setdefault(name, value)

IMPORT_DIRS = (
    ROOT / "pipeline",           # local et conteneur Airflow (/opt/airflow/pipeline)
    ROOT,                        # conteneur `pipeline` (/app/marketplace)
    ROOT / "airflow" / "plugins",  # local
    ROOT / "plugins",            # conteneur Airflow
)

for directory in IMPORT_DIRS:
    if directory.is_dir() and str(directory) not in sys.path:
        sys.path.insert(0, str(directory))
