"""Test de structure : tous les services de l'architecture sont déclarés dans le compose."""
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_architecture_services_are_declared():
    compose_file = ROOT / "docker-compose.yml"
    if not compose_file.exists():
        pytest.skip("docker-compose.yml absent de cet environnement")
    compose = compose_file.read_text(encoding="utf-8")
    for service in ("postgres-dwh", "minio", "api-marketplace", "metabase",
                    "prometheus", "grafana", "airflow-scheduler"):
        assert f"  {service}:" in compose
