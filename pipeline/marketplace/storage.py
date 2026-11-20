"""Couche raw : archivage des JSON bruts dans MinIO.

Conserver la donnée brute permet de rejouer le pipeline sans rappeler l'API.
Chemin des objets : raw/<type>/dt=YYYY-MM-DD/data.json
"""
import json
import time

import boto3
from botocore.client import Config

from . import config


def client():
    """Retourne un client S3 pointant sur MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_USER,
        aws_secret_access_key=config.MINIO_PASSWORD,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3, attempts=15):
    """Crée le bucket s'il n'existe pas ; MinIO peut mettre quelques secondes à démarrer."""
    for attempt in range(attempts):
        try:
            s3.head_bucket(Bucket=config.BUCKET)
            return
        except Exception:
            try:
                s3.create_bucket(Bucket=config.BUCKET)
                return
            except Exception:
                if attempt == attempts - 1:
                    raise
                time.sleep(2)


def _key(kind, dt):
    return f"raw/{kind}/dt={dt}/data.json"


def archive(s3, dt, kind, rows):
    """Dépose le JSON brut d'un jeu de données dans la partition du jour."""
    s3.put_object(
        Bucket=config.BUCKET,
        Key=_key(kind, dt),
        Body=json.dumps(rows, ensure_ascii=False).encode(),
        ContentType="application/json",
    )


def read_raw(s3, dt, kind):
    """Relit un jeu de données archivé (utilisé par le mode replay)."""
    body = s3.get_object(Bucket=config.BUCKET, Key=_key(kind, dt))["Body"].read()
    return json.loads(body)
