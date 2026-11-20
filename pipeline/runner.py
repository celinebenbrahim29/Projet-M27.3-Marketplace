"""Point d'entrée en ligne de commande du pipeline.

Commandes :
  run      [--date D]            un run pour la date D (par défaut : hier)
  replay   [--date D]            rejoue la date D depuis les JSON archivés dans MinIO
  backfill [--days N] [--date D] charge N jours se terminant à D (par défaut : hier)
  serve                          expose les métriques Prometheus (processus permanent)
"""
import argparse
import logging
import time
from datetime import date, timedelta

from marketplace import config, metrics
from marketplace.pipeline import run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("marketplace")


def backfill(days, end):
    """Charge `days` jours consécutifs se terminant à `end`, du plus ancien au plus récent.

    Les dimensions ne dépendent pas de la date : on ne les charge qu'au premier jour.
    """
    for offset in reversed(range(days)):
        dt = (end - timedelta(days=offset)).isoformat()
        run(dt, dims=(offset == days - 1))


def serve():
    metrics.serve(config.METRICS_PORT)
    LOG.info("Metrics server started")
    while True:
        time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="Pipeline Marketplace")
    parser.add_argument("command", choices=["run", "replay", "backfill", "serve"])
    parser.add_argument("--date", help="date YYYY-MM-DD (par défaut : hier)")
    parser.add_argument("--days", type=int, default=7, help="durée du backfill en jours")
    args = parser.parse_args()

    end = date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1)

    if args.command == "serve":
        serve()
    elif args.command == "backfill":
        backfill(args.days, end)
    elif args.command == "replay":
        run(end.isoformat(), replay=True, dims=False)
    else:
        run(end.isoformat())


if __name__ == "__main__":
    main()
