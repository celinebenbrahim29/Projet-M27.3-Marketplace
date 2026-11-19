
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

source = (ROOT / "runner.py").read_text(encoding="utf-8")


def test_python_syntax():
    ast.parse(source)


def test_idempotent_delete_before_insert():
    assert source.index("DELETE FROM dwh.fact_orders") < source.index(
        "INSERT INTO dwh.fact_orders"
    )


def test_quality_checks_present():
    assert "duplicate IDs" in source
    assert "Future execution date" in source
    assert "Empty dataset" in source


def test_architecture_components():
    compose = (ROOT / "docker-compose.yml").read_text(
        encoding="utf-8"
    )

    for service in (
        "postgres-dwh",
        "minio",
        "api-marketplace",
        "metabase",
        "prometheus",
        "grafana",
    ):
        assert service + ":" in compose
