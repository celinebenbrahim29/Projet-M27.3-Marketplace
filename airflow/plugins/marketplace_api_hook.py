"""Hook Airflow pour l'API Marketplace.

Le Hook lit l'URL et le token dans la Connection Airflow `marketplace_api`
(host = URL de l'API, password = token Bearer) : aucun secret n'est écrit dans le code.
La communication HTTP (authentification, nouvelles tentatives) est déléguée à
`MarketplaceClient`, qui ne dépend pas d'Airflow et sert aussi à la ligne de commande.
"""
from airflow.hooks.base import BaseHook

from marketplace.api_client import MarketplaceClient


class MarketplaceAPIHook(BaseHook):
    conn_name_attr = "marketplace_conn_id"
    default_conn_name = "marketplace_api"
    conn_type = "http"
    hook_name = "Marketplace API"

    def __init__(self, marketplace_conn_id=default_conn_name):
        super().__init__()
        self.marketplace_conn_id = marketplace_conn_id
        self._client = None

    def get_conn(self):
        """Retourne le client HTTP configuré avec la Connection Airflow."""
        if self._client is None:
            conn = self.get_connection(self.marketplace_conn_id)
            self._client = MarketplaceClient(base_url=conn.host, token=conn.password)
        return self._client

    def get_orders(self, date):
        return self.get_conn().get_orders(date)

    def get_sellers(self):
        return self.get_conn().get_sellers()

    def get_products(self):
        return self.get_conn().get_products()

    def get_customers(self):
        return self.get_conn().get_customers()

    def fetch_dataset(self, kind, date):
        """Récupère un jeu de données par son nom (signature attendue par le pipeline)."""
        return self.get_conn().fetch_dataset(kind, date)

    def check_health(self):
        """Lève une erreur si l'API ne répond pas."""
        return self.get_conn().health()
