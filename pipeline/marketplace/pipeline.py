"""Étapes du pipeline et exécution d'un run complet pour une date.

Étapes : extraction (API) -> contrôle qualité -> archivage raw (MinIO)
         -> staging -> dwh -> analytics.

Le DAG Airflow appelle `extract_to_raw` puis les étapes de `warehouse`, une tâche chacune.
La ligne de commande enchaîne tout avec `run`. Les deux passent par la couche raw :
les données chargées sont toujours celles archivées dans MinIO.
"""
import logging
import time
from datetime import date

from . import api_client, metrics, quality, storage, warehouse

LOG = logging.getLogger("marketplace")

DIMENSIONS = ("sellers", "products", "customers")


def kinds_for(dims):
    """Jeux de données à traiter : seules les commandes si `dims` est faux."""
    return (*DIMENSIONS, "orders") if dims else ("orders",)


def extract_to_raw(dt, fetch=None, dims=True):
    """Extrait les données de la source, les valide et les archive dans MinIO.

    `fetch(kind, dt)` fournit les données ; par défaut le client configuré par l'environnement.
    Dans Airflow, c'est le Hook qui est passé.
    """
    fetch = fetch or api_client.from_env().fetch_dataset
    s3 = storage.client()
    storage.ensure_bucket(s3)
    for kind in kinds_for(dims):
        rows = fetch(kind, dt)
        quality.validate(rows, kind, dt)
        storage.archive(s3, dt, kind, rows)


def read_raw(dt, dims=True):
    """Relit et valide les données archivées dans MinIO pour la date."""
    s3 = storage.client()
    data = {}
    for kind in kinds_for(dims):
        data[kind] = storage.read_raw(s3, dt, kind)
        quality.validate(data[kind], kind, dt)
    return data


def run(dt, replay=False, dims=True, fetch=None):
    """Exécute le pipeline pour la date `dt` (YYYY-MM-DD) et retourne le nombre de faits.

    replay : rejoue depuis les JSON déjà archivés dans MinIO, sans appeler l'API.
    dims   : charge aussi vendeurs, produits et clients (inutile pour chaque jour d'un backfill).
    fetch  : fonction `(kind, dt) -> liste` qui remplace le client API par défaut.
    """
    date.fromisoformat(dt)  # échoue tout de suite si le format de date est invalide
    started = time.monotonic()
    run_id = warehouse.start_run(dt)
    try:
        if not replay:
            extract_to_raw(dt, fetch, dims)
        rows = warehouse.load(dt, read_raw(dt, dims), dims)
        warehouse.finish_run(run_id, rows)
        metrics.record_success(rows)
        LOG.info("SUCCESS date=%s rows=%s", dt, rows)
        return rows
    except Exception as exc:
        metrics.record_failure()
        LOG.exception("FAILED date=%s", dt)
        warehouse.fail_run(run_id, exc)
        raise
    finally:
        metrics.DURATION.observe(time.monotonic() - started)
