# Marketplace Analytics — End-to-End Data Engineering Project

Projet de Data Engineering et de Business Intelligence permettant de simuler, collecter, stocker, transformer, analyser et visualiser les données d'une marketplace.

L'objectif est de construire une architecture complète intégrant un Data Lake, un Data Warehouse, des pipelines ETL, des dashboards métier et un système de monitoring.

## 1. Présentation du projet

Marketplace Analytics est une plateforme de traitement et d'analyse de données e-commerce.

Elle permet de :

- Simuler les données d'une marketplace via une API REST.
- Collecter et stocker les données brutes dans MinIO.
- Transformer et charger les données dans PostgreSQL.
- Construire un Data Warehouse avec un modèle dimensionnel.
- Calculer les indicateurs de performance commerciale.
- Détecter les anomalies et les comportements suspects.
- Visualiser les résultats dans Metabase et Power BI.
- Superviser les traitements avec Prometheus et Grafana.
- Intégrer Apache Airflow pour orchestrer les pipelines.

## 2. Technologies utilisées

| Technologie | Utilisation |
|---|---|
| Python | Développement des pipelines ETL |
| Flask | API REST Marketplace |
| Docker Compose | Conteneurisation des services |
| MinIO | Data Lake et stockage objet |
| PostgreSQL | Staging et Data Warehouse |
| SQL | Transformation et analyse des données |
| Metabase | Business Intelligence |
| Power BI | Reporting et visualisation métier |
| Prometheus | Collecte des métriques |
| Grafana | Monitoring technique |
| Apache Airflow | Orchestration des pipelines |
| Pytest | Tests automatisés |

## 3. Architecture du projet

L'architecture suit les principales étapes d'une chaîne de traitement de données :

```text
                  API Marketplace
                         |
                         v
                   Pipeline Python
                         |
                         v
                  MinIO Data Lake
                    Bronze Layer
                         |
                         v
                 PostgreSQL Staging
                         |
                         v
                 PostgreSQL DWH
                  Star Schema
                         |
                         v
                 Analytics & KPIs
                         |
                 +-------+-------+
                 |               |
                 v               v
              Metabase        Power BI


           SUPERVISION TECHNIQUE

            Pipeline Python
                   |
                   v
               Prometheus
                   |
                   v
                Grafana


           ORCHESTRATION (EN COURS)

              Apache Airflow
                   |
                   v
              Pipeline Python
```

## 4. Structure du projet

```text
marketplace-ready/
|
|-- airflow/
|   |-- dags/
|       |-- marketplace_dag.py
|
|-- api/
|   |-- app.py
|   |-- Dockerfile
|
|-- pipeline/
|   |-- runner.py
|   |-- Dockerfile
|   |-- requirements.txt
|
|-- sql/
|   |-- 001_schema.sql
|
|-- prometheus/
|   |-- prometheus.yml
|
|-- grafana/
|   |-- provisioning/
|
|-- powerbi/
|   |-- MEASURES_DAX.md
|
|-- tests/
|   |-- test_pipeline.py
|
|-- docs/
|   |-- METABASE.md
|
|-- docker-compose.yml
|-- requirements.txt
|-- .env.example
|-- .gitignore
|-- README.md
```

## 5. Modélisation des données

Le projet utilise trois schémas PostgreSQL principaux.

### Staging

Zone intermédiaire contenant les données extraites de la source.

Tables :

- `staging.customers`
- `staging.products`
- `staging.sellers`
- `staging.orders`

### Data Warehouse

Modèle dimensionnel destiné à l'analyse.

Tables :

- `dwh.dim_customer`
- `dwh.dim_product`
- `dwh.dim_seller`
- `dwh.dim_date`
- `dwh.fact_orders`

### Analytics

Tables et vues permettant de calculer les KPIs et d'identifier les anomalies.

Principaux objets :

- `analytics.daily_summary`
- `analytics.pipeline_runs`
- `analytics.sales_anomalies`
- `analytics.category_daily`
- `analytics.seller_daily`
- `analytics.inactive_sellers`
- `analytics.suspicious_prices`

## 6. Pipeline ETL

Le pipeline Python réalise les opérations suivantes :

**Extraction**

Récupération des données de commandes depuis l'API Marketplace.

**Stockage**

Conservation des données brutes dans MinIO.

**Transformation**

Nettoyage, validation et préparation des données pour les analyses.

**Chargement**

Insertion des données dans PostgreSQL et alimentation du Data Warehouse.

**Analytics**

Calcul des agrégations commerciales et des indicateurs de qualité.

Le pipeline comprend également des contrôles sur les doublons, les dates et les jeux de données vides.

## 7. Indicateurs de performance

Les principaux indicateurs calculés sont :

| KPI | Description |
|---|---|
| Revenue | Chiffre d'affaires |
| Total Orders | Nombre de commandes |
| Average Basket | Panier moyen |
| Commission | Commissions de la marketplace |
| Sales by Category | Ventes par catégorie |
| Seller Performance | Performance des vendeurs |
| Revenue Anomalies | Variations anormales du chiffre d'affaires |
| Inactive Sellers | Vendeurs inactifs |

## 8. Détection des anomalies

Le projet intègre plusieurs contrôles.

**Anomalies de chiffre d'affaires**

Comparaison du chiffre d'affaires quotidien avec la moyenne des sept jours précédents.

**Vendeurs inactifs**

Identification des vendeurs sans vente récente.

**Prix suspects**

Détection des produits dont le prix est inférieur ou égal à zéro ou dépasse un seuil défini.

## 9. Visualisation et Business Intelligence

### Metabase

Metabase permet de consulter les tables analytiques et de créer des visualisations métier.

Accès local : `http://localhost:3000`

### Power BI

Power BI est prévu pour construire un reporting interactif comprenant :

- Une vue globale des ventes.
- L'évolution du chiffre d'affaires.
- Les performances par catégorie.
- Les performances des vendeurs.
- L'analyse des commissions.
- Les anomalies commerciales.

Les mesures DAX sont documentées dans `powerbi/MEASURES_DAX.md`.

## 10. Monitoring avec Prometheus et Grafana

Prometheus collecte les métriques techniques du pipeline et de PostgreSQL.

Grafana permet de suivre :

- Le statut du pipeline.
- La disponibilité de PostgreSQL.
- La disponibilité de Prometheus.
- Le nombre d'exécutions réussies.
- Le nombre d'exécutions échouées.
- Le nombre de lignes chargées.
- La durée moyenne des traitements.
- La date de la dernière exécution réussie.

Accès :

- Prometheus : `http://localhost:9090`
- Grafana : `http://localhost:3001`

## 11. Orchestration avec Apache Airflow

*Statut : intégration en cours.*

Apache Airflow est ajouté à l'architecture pour centraliser la planification et l'orchestration des traitements.

Objectifs :

- Déclencher les traitements quotidiennement.
- Superviser les exécutions.
- Gérer les nouvelles tentatives en cas d'échec.
- Consulter les logs.
- Permettre les exécutions manuelles et historiques.

Le DAG `marketplace_daily_pipeline` est en cours de configuration.

Accès prévu : `http://localhost:8080`

## 12. Installation et exécution

### Prérequis

- Docker Desktop
- Docker Compose
- Git
- VS Code
- Power BI Desktop (pour la visualisation Power BI)

### Configuration

Cloner le projet :

```bash
git clone https://github.com/USERNAME/marketplace-ready.git
cd marketplace-ready
```

Créer le fichier `.env` à partir de `.env.example` et renseigner les variables nécessaires.

### Démarrer les services

```bash
docker compose up -d --build
```

Vérifier leur état :

```bash
docker compose ps
```

### Exécuter un chargement manuel

```bash
docker compose run --rm pipeline python runner.py backfill --date 2026-04-07 --days 7
```

### Vérifier les logs

```bash
docker compose logs --tail=50 pipeline
```

## 13. Tests automatisés

Le projet utilise Pytest pour vérifier :

- La syntaxe Python.
- La présence de la logique d'idempotence.
- La présence des contrôles de qualité.
- La configuration des composants Docker.

Exécution :

```bash
docker compose run --rm pipeline python -m pytest -v
```

Les quatre tests de base ont été exécutés avec succès lors du développement.

## 14. Résultats obtenus

Lors des premiers tests du pipeline :

- 500 clients générés.
- 300 produits générés.
- 60 vendeurs générés.
- 240 commandes chargées par journée.
- 8 journées de données contrôlées.
- 1 920 commandes présentes dans le Data Warehouse.
- 4 tests Pytest réussis.
- Monitoring Prometheus opérationnel.
- Dashboard Grafana configuré.

Ces résultats correspondent à un état de validation du projet et peuvent évoluer avec les prochains chargements.

## 15. Améliorations prévues

- Finaliser l'orchestration Airflow.
- Construire les dashboards Power BI.
- Enrichir les tests de qualité des données.
- Ajouter des alertes automatiques.
- Améliorer la documentation technique.
- Automatiser davantage les contrôles de fiabilité.

## 16. Conclusion

Ce projet illustre la mise en place d'une plateforme Data Engineering de bout en bout, depuis la collecte des données jusqu'à leur exploitation décisionnelle.

Il mobilise les concepts de Data Lake, Data Warehouse, ETL, modélisation dimensionnelle, Business Intelligence, qualité des données, conteneurisation et observabilité.

L'architecture est conçue pour être évolutive et pour faciliter l'ajout de nouveaux traitements et outils.

---

**Projet académique — Data Engineering, Data Warehouse & Business Intelligence**

**Auteur : Groupe 1**
