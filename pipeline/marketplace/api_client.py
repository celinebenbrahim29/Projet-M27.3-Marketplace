"""Client de l'API Marketplace (authentification Bearer, nouvelles tentatives).

Cette classe ne dépend pas d'Airflow : la ligne de commande l'utilise avec les variables
d'environnement (`from_env`) et le Hook Airflow l'utilise avec la Connection `marketplace_api`.
"""
import time

import requests

from . import config


class MarketplaceClient:
    def __init__(self, base_url, token, retries=config.API_RETRIES,
                 timeout=config.API_TIMEOUT_SECONDS):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.retries = retries
        self.timeout = timeout

    def _get(self, endpoint, params=None):
        """GET authentifié qui retourne une liste JSON.

        Réessaie avec un délai croissant (1 s, 2 s) en cas d'erreur réseau, HTTP ou de format.
        """
        headers = {"Authorization": f"Bearer {self.token}"}
        for attempt in range(self.retries):
            try:
                response = requests.get(
                    f"{self.base_url}/{endpoint}",
                    headers=headers,
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, list):
                    raise ValueError(f"{endpoint} must return a JSON list")
                return data
            except (requests.RequestException, ValueError):
                if attempt == self.retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def get_orders(self, dt):
        return self._get("orders", {"date": dt})

    def get_sellers(self):
        return self._get("sellers")

    def get_products(self):
        return self._get("products")

    def get_customers(self):
        return self._get("customers")

    def fetch_dataset(self, kind, dt):
        """Récupère un jeu de données par son nom. Seules les commandes dépendent de la date."""
        if kind == "orders":
            return self.get_orders(dt)
        return {
            "sellers": self.get_sellers,
            "products": self.get_products,
            "customers": self.get_customers,
        }[kind]()

    def health(self):
        """Vérifie que l'API répond (endpoint public, sans token). Lève une erreur sinon."""
        response = requests.get(f"{self.base_url}/health", timeout=self.timeout)
        response.raise_for_status()
        return response.json()


def from_env():
    """Client configuré par les variables d'environnement (usage ligne de commande)."""
    return MarketplaceClient(config.API_URL, config.API_TOKEN)
