# Marketplace Analytics

Plateforme data de bout en bout pour **Maelys Marketplace**, une marketplace e-commerce de 2 400 vendeurs et 180 000 produits : ingestion automatique des données du backoffice, Data Warehouse en étoile, agrégats et détection d'anomalies, orchestration Airflow, BI (Metabase) et supervision (Prometheus, Grafana).

Équipe : Ben Brahim Céline, Bahroun Yasmine, Gaaya Aziz, Bouzid Fatma.

## Sommaire

1. [Le projet en bref](#1-le-projet-en-bref)
2. [Architecture](#2-architecture)
3. [Lancer le projet](#3-lancer-le-projet)
4. [Vérifier chaque étape](#4-vérifier-chaque-étape)
5. [Visualiser : Metabase, Grafana](#5-visualiser--metabase-grafana)
6. [Accès et identifiants](#6-accès-et-identifiants)
7. [Modèle de données](#7-modèle-de-données)
8. [Le pipeline en détail](#8-le-pipeline-en-détail)
9. [Organisation du dépôt](#9-organisation-du-dépôt)
10. [Tests](#10-tests)
11. [Choix techniques et limites](#11-choix-techniques-et-limites)
12. [Dépannage](#12-dépannage)

---

## 1. Le projet en bref

**Problème.** Chez Maelys, un analyste passe 2 jours par semaine à extraire les données du backoffice à la main. Les rapports sont en retard et les erreurs de copier-coller fréquentes.

**Solution.** Un pipeline ELT automatique et reproductible (Docker Compose) :

1. **Extraction** des commandes, vendeurs, produits et clients depuis l'API du backoffice (ici une API Flask simulée).
2. **Archivage brut** des JSON dans un data lake (MinIO), pour pouvoir rejouer un traitement sans rappeler l'API.
3. **Chargement** dans PostgreSQL : `staging` (données brutes typées), `dwh` (modèle en étoile), `analytics` (agrégats pour les dashboards).
4. **Contrôles qualité** avant tout chargement, **détection d'anomalies** du chiffre d'affaires.
5. **Orchestration** quotidienne par Airflow, **dashboards** Metabase, **supervision** Prometheus et Grafana.

**Données simulées** (volumes de l'énoncé). 2 400 vendeurs, 180 000 produits, 500 clients et 700 commandes par jour, soit environ 4,2 M€ de chiffre d'affaires par mois. L'API est **déterministe** : une même date renvoie toujours les mêmes commandes, ce qui permet de prouver l'idempotence en rejouant un jour.

## 2. Architecture

```mermaid
flowchart LR
    API[API Marketplace<br/>Flask simulée] -->|JSON| P[Pipeline Python<br/>package marketplace]
    AF[Airflow<br/>DAG @daily] -.orchestre.-> P
    P -->|raw JSON dt=YYYY-MM-DD| M[(MinIO<br/>data lake)]
    M --> S[(PostgreSQL<br/>staging)]
    S --> D[(dwh<br/>modèle en étoile)]
    D --> A[(analytics<br/>agrégats)]
    A --> MB[Metabase<br/>dashboards]
    P -.métriques.-> PR[Prometheus] -.-> G[Grafana]
    PG[postgres-exporter] -.-> PR
```

| Service | Rôle |
|---|---|
| `api-marketplace` | Source de données simulée, protégée par un token Bearer |
| `minio` | Data lake : JSON bruts, bucket `data-lake` |
| `postgres-dwh` | Data Warehouse (schémas `staging`, `dwh`, `analytics`) |
| `pipeline` | Code du pipeline et ligne de commande ; expose les métriques Prometheus |
| `airflow-*` | Orchestration : base, initialisation, webserver, scheduler |
| `metabase` | BI : dashboards sur le Data Warehouse |
| `prometheus`, `grafana`, `postgres-exporter` | Supervision technique |

**Règle d'idempotence.** Pour chaque date, le pipeline **supprime puis réinsère** ses lignes (`DELETE` + `INSERT` sur `staging.orders` et `dwh.fact_orders`). Rejouer un jour redonne exactement le même résultat, sans doublon.

## 3. Lancer le projet

### Prérequis

- **Docker Desktop** (Docker Compose v2) démarré.
- Environ **8 Go de RAM** alloués à Docker et quelques Go de disque.
- Ports libres sur la machine : `3000`, `3001`, `5000`, `5433`, `8000`, `8080`, `9000`, `9001`, `9090`.

### Étape 1 : récupérer le projet

```bash
git clone https://github.com/celinebenbrahim29/Projet-M27.3-Marketplace.git
cd Projet-M27.3-Marketplace
```

### Étape 2 : créer le fichier `.env`

Il contient les mots de passe et paramètres. Le modèle fourni contient des valeurs de formation prêtes à l'emploi.

```bash
# Windows (PowerShell ou cmd)
copy .env.example .env

# Linux / macOS
cp .env.example .env
```

> Si le port 8080 est déjà utilisé sur votre machine, changez `AIRFLOW_PORT` dans `.env` (par exemple `8081`). Airflow sera alors sur ce port.

### Étape 3 : démarrer tous les services

```bash
docker compose up -d --build
```

Le **premier démarrage est long** (construction des images, téléchargements, initialisation d'Airflow) : comptez plusieurs minutes, jusqu'à une quinzaine sur une machine lente. Suivez l'avancement :

```bash
docker compose ps
```

Attendez que tous les services soient `Up` (ou `healthy`) ; seul `airflow-init` doit être `Exited (0)` : il s'arrête de lui-même après avoir initialisé Airflow.

### Étape 4 : charger un an de données

```bash
docker compose run --rm pipeline python runner.py backfill --days 365
```

Le pipeline traite 365 jours, du plus ancien à hier. La première journée charge aussi les 2 400 vendeurs, 180 000 produits et 500 clients ; les suivantes ne traitent que les commandes. Chaque ligne `SUCCESS date=… rows=700` confirme une journée chargée. Prévoyez quelques minutes.

C'est tout : le projet est démarré et les données sont en base. La section suivante explique comment vérifier chaque étape.

## 4. Vérifier chaque étape

Les requêtes SQL passent par `psql` dans le conteneur : **rien à installer**. Elles sont indépendantes de la date du jour.

> Les commandes utilisent l'utilisateur `dwh_user` et la base `dwh` (valeurs par défaut de `.env.example`).

### 4.1 Les services tournent

```bash
docker compose ps
```
Tous les services sont `Up` ou `healthy`.

### 4.2 La source : l'API

```bash
curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/orders?date=AAAA-MM-JJ"
```
(remplacez `AAAA-MM-JJ` par une date, par exemple celle d'hier). Une liste JSON de **700 commandes**. Sans le header `Authorization`, l'API répond `401 Unauthorized` : le token est obligatoire. (Sous Windows, utilisez `curl.exe`.)

### 4.3 La donnée brute : MinIO

1. Ouvrez http://localhost:9001 (identifiants dans la section 6).
2. **Object Browser**, bucket **`data-lake`**, dossier `raw/`.
3. `raw/orders/` contient un dossier **par jour** (`dt=YYYY-MM-DD`) avec un `data.json` : c'est le JSON tel que l'API l'a envoyé.
4. `raw/sellers/`, `raw/products/` et `raw/customers/` n'existent que pour **la première journée** du backfill, car les dimensions ne sont chargées qu'une fois.

### 4.4 Le suivi des exécutions

```bash
docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT status, COUNT(*) FROM analytics.pipeline_runs GROUP BY status;"
```
`success` : **au moins 365** (un par jour chargé, plus les rejeux). **Aucun `failed`.**

### 4.5 Les volumes dans chaque couche

```bash
docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT 'staging.orders' AS table, COUNT(*) FROM staging.orders UNION ALL SELECT 'dwh.fact_orders', COUNT(*) FROM dwh.fact_orders UNION ALL SELECT 'dwh.dim_seller', COUNT(*) FROM dwh.dim_seller UNION ALL SELECT 'dwh.dim_product', COUNT(*) FROM dwh.dim_product UNION ALL SELECT 'dwh.dim_customer', COUNT(*) FROM dwh.dim_customer UNION ALL SELECT 'analytics.daily_summary', COUNT(*) FROM analytics.daily_summary;"
```

| Table | Attendu |
|---|---|
| `staging.orders` | 255 500 (365 jours × 700) |
| `dwh.fact_orders` | 255 500 |
| `dwh.dim_seller` | 2 400 |
| `dwh.dim_product` | 180 000 |
| `dwh.dim_customer` | 500 |
| `analytics.daily_summary` | 365 |

### 4.6 La transformation : staging puis DWH

Même commande dans les deux couches. Dans `staging`, la donnée est brute ; dans `dwh`, elle est jointe aux dimensions et la **commission de 12 %** est calculée.

```bash
docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT order_id, seller_id, product_id, amount FROM staging.orders WHERE dt = (SELECT MAX(dt) FROM staging.orders) LIMIT 3;"

docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT f.order_id, s.seller_name, p.category, f.amount, f.commission FROM dwh.fact_orders f JOIN dwh.dim_seller s USING (seller_key) JOIN dwh.dim_product p USING (product_key) WHERE f.dt = (SELECT MAX(dt) FROM dwh.fact_orders) LIMIT 3;"
```
Même `order_id` dans les deux résultats. Dans le second, `commission` = `amount` × 0,12 (par exemple 143,98 € donne 17,28 €).

### 4.7 Les agrégats et les anomalies

```bash
docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT * FROM analytics.daily_summary ORDER BY dt DESC LIMIT 5;"

docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT dt, revenue, average_previous_7_days, ratio, is_anomaly FROM analytics.sales_anomalies ORDER BY dt DESC LIMIT 5;"
```
Chaque jour : 700 commandes, un CA d'environ 140 000 € et un panier moyen d'environ 200 €. Dans `sales_anomalies`, `is_anomaly` vaut `true` si le CA du jour est inférieur à 70 % de la moyenne des 7 jours précédents (il reste vide pendant les 7 premiers jours, faute d'historique).

### 4.8 L'idempotence

Rejouer un même jour ne doit pas créer de doublons. Comptez, rejouez, recomptez :

```bash
docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT COUNT(*) FROM dwh.fact_orders;"
docker compose run --rm pipeline python runner.py run
docker compose run --rm pipeline python runner.py run
docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT COUNT(*) FROM dwh.fact_orders;"
```
`run` sans date traite **hier**. Les deux comptages sont **identiques** (255 500), jamais augmentés de 700 ou 1 400.

Contrôle d'unicité sur toute la base :
```bash
docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT COUNT(*) AS total, COUNT(DISTINCT order_id) AS uniques FROM dwh.fact_orders;"
```
Les deux nombres sont égaux.

### 4.9 Le rejeu depuis le data lake

```bash
docker compose run --rm pipeline python runner.py replay
```
Recharge hier **depuis les JSON de MinIO**, sans appeler l'API : `SUCCESS … rows=700`.

### 4.10 L'orchestration : Airflow

1. Ouvrez http://localhost:8080 (ou votre `AIRFLOW_PORT`), identifiants section 6.
2. Le DAG **`marketplace_daily_pipeline`** est en pause à la création : activez-le avec l'interrupteur.
3. Cliquez sur **▶ Trigger DAG**, puis ouvrez l'onglet **Graph**. Les 6 tâches passent au vert :

   `check_marketplace_api` → `extract_to_raw` → `load_staging` → `transform_dwh` → `aggregate_analytics` → `record_run`

4. Cliquez sur une tâche puis **Logs** : `check_marketplace_api` affiche `Retrieving connection 'marketplace_api'` (le Hook) puis le statut de l'API ; `aggregate_analytics` affiche `Pipeline termine : 700 lignes chargees`.

Sans passer par l'interface (remplacez la date par un jour des 365 derniers jours ; le DAG traite alors la veille de cette date, déjà en base : le résultat ne change pas, grâce à l'idempotence) :
```bash
docker compose exec airflow-scheduler airflow dags test marketplace_daily_pipeline AAAA-MM-JJ
```

Le DAG traite la **veille** de l'intervalle, chaque jour (`@daily`, `catchup=False`). L'historique, lui, se charge avec le backfill de l'étape 4.

### 4.11 Les tests

```bash
docker compose run --rm pipeline python -m pytest -v
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -v -p no:cacheprovider
```
Tous les tests passent. Quelques-uns sont ignorés volontairement : les tests du Hook et du DAG exigent Airflow (absent du conteneur `pipeline`), et le test du compose exige le fichier `docker-compose.yml` (absent du conteneur Airflow).

## 5. Visualiser : Metabase, Grafana

### Metabase (dashboards métier) : http://localhost:3000

Les dashboards se créent dans l'interface et sont conservés dans le volume Docker `metabase-data` (ils ne sont pas versionnés dans Git).

1. **Compte administrateur** : créez-le au premier accès (le premier démarrage prend environ une minute).
2. **Ajouter les données** : type **PostgreSQL**, hôte **`postgres-dwh`** (le nom du service Docker, pas `localhost`), port **`5432`** (le port interne, pas 5433), base `dwh`, utilisateur et mot de passe de la section 6.
3. **Nouvelle question SQL** (New, puis SQL query) :

```sql
-- CA par jour (courbe)
SELECT dt, revenue FROM analytics.daily_summary ORDER BY dt;
```
```sql
-- Top 10 vendeurs (barres)
SELECT seller_name, SUM(revenue) AS revenue
FROM analytics.seller_daily GROUP BY seller_name ORDER BY revenue DESC LIMIT 10;
```
```sql
-- Commissions par jour (courbe)
SELECT dt, commission FROM analytics.daily_summary ORDER BY dt;
```
```sql
-- CA par catégorie (camembert)
SELECT category, SUM(revenue) AS revenue
FROM analytics.category_daily GROUP BY category ORDER BY revenue DESC;
```
```sql
-- Jours à CA anormalement bas
SELECT dt, revenue, average_previous_7_days, ratio
FROM analytics.sales_anomalies WHERE is_anomaly ORDER BY dt DESC;
```

4. **Sauvegardez** chaque question, regroupez-les dans des dashboards (Executive Summary, Top Sellers, Finance, Catégories) et ajoutez un filtre de date sur `dt`. Le guide détaillé est dans [docs/METABASE.md](docs/METABASE.md).

Des vues SQL prêtes à l'emploi existent dans `analytics` : `seller_daily`, `category_daily`, `inactive_sellers` (vendeurs sans vente depuis plus de 7 jours) et `suspicious_prices` (prix nuls ou supérieurs à 10 000 €). Les mesures DAX pour Power BI sont dans [powerbi/MEASURES_DAX.md](powerbi/MEASURES_DAX.md).

### Grafana et Prometheus (supervision technique)

- **Grafana** : http://localhost:3001. La source de données Prometheus est préconfigurée.
- **Prometheus** : http://localhost:9090 (interrogez par exemple `marketplace_pipeline_runs_total`).

Prometheus collecte les métriques de PostgreSQL (via `postgres-exporter`) et celles du service `pipeline`. Les exécutions lancées par `docker compose run` ou par Airflow sont des processus distincts : elles ne sont pas comptées dans ces métriques. **L'historique complet des exécutions se lit dans `analytics.pipeline_runs`** (étape 4.4).

## 6. Accès et identifiants

Les valeurs ci-dessous sont celles par défaut de `.env.example` (valeurs de formation).

| Service | URL | Identifiant | Mot de passe (variable du `.env`) |
|---|---|---|---|
| Airflow | http://localhost:8080 | `admin` | `admin_2026` (`AIRFLOW_ADMIN_PASSWORD`) |
| Metabase | http://localhost:3000 | compte à créer | — |
| MinIO (console) | http://localhost:9001 | `minio_admin` (`MINIO_ROOT_USER`) | `minio_password_2026` (`MINIO_ROOT_PASSWORD`) |
| Grafana | http://localhost:3001 | `admin` | `admin_2026` (`GRAFANA_ADMIN_PASSWORD`) |
| Prometheus | http://localhost:9090 | — | — |
| API Marketplace | http://localhost:5000 | header `Authorization: Bearer <API_TOKEN>` | `formation-token-2026` (`API_TOKEN`) |
| PostgreSQL DWH | `localhost:5433`, base `dwh` | `dwh_user` (`POSTGRES_USER`) | `dwh_password_2026` (`POSTGRES_PASSWORD`) |

Le port 5433 de PostgreSQL est celui de la machine hôte pour un client SQL (DBeaver, pgAdmin…). Entre conteneurs, l'adresse est `postgres-dwh:5432`.

## 7. Modèle de données

Schéma en étoile (Kimball), défini dans [sql/001_schema.sql](sql/001_schema.sql) et créé automatiquement au premier démarrage de PostgreSQL.

```mermaid
erDiagram
    dim_seller ||--o{ fact_orders : seller_key
    dim_product ||--o{ fact_orders : product_key
    dim_customer ||--o{ fact_orders : customer_key
    dim_date ||--o{ fact_orders : date_key
    fact_orders {
        text order_id PK
        date dt
        int seller_key FK
        int product_key FK
        int customer_key FK
        int date_key FK
        int quantity
        numeric amount
        numeric commission
    }
    dim_seller {
        int seller_key PK
        int seller_id
        text seller_name
        text region
    }
    dim_product {
        int product_key PK
        int product_id
        text product_name
        text category
        numeric price
    }
    dim_customer {
        int customer_key PK
        int customer_id
        text customer_name
    }
    dim_date {
        int date_key PK
        date full_date
        int year
        int month
        int day
    }
```

| Schéma | Contenu |
|---|---|
| `staging` | `sellers`, `products`, `customers`, `orders` : données brutes typées |
| `dwh` | `dim_seller`, `dim_product`, `dim_customer`, `dim_date`, `fact_orders` |
| `analytics` | Tables `daily_summary`, `sales_anomalies`, `pipeline_runs` ; vues `seller_daily`, `category_daily`, `inactive_sellers`, `suspicious_prices` |

## 8. Le pipeline en détail

Pour une date `dt` :

| # | Étape | Où | Ce qui se passe |
|---|---|---|---|
| 1 | Extraction | `api_client.py` | Appel de l'API avec le token Bearer ; 3 tentatives en cas d'erreur |
| 2 | Qualité | `quality.py` | Jeu non vide, identifiants uniques, date non future, montants positifs, quantités > 0 |
| 3 | Raw | `storage.py` | JSON brut déposé dans `data-lake/raw/<type>/dt=<date>/data.json` |
| 4 | Staging | `warehouse.py` | Chargement dans `staging.*` (commandes : `DELETE` puis `INSERT`) |
| 5 | DWH | `warehouse.py` | Dimensions (upsert) ; faits : `DELETE` puis `INSERT` des commandes `completed`, commission de 12 % |
| 6 | Analytics | `warehouse.py` | `daily_summary` et `sales_anomalies` (CA < 70 % de la moyenne des 7 jours précédents) |

**Deux façons de l'exécuter, mêmes étapes.**

- **Ligne de commande** (enchaîne tout en une transaction) :
  ```bash
  docker compose run --rm pipeline python runner.py run      [--date AAAA-MM-JJ]   # un jour (défaut : hier)
  docker compose run --rm pipeline python runner.py replay   [--date AAAA-MM-JJ]   # rejoue depuis MinIO, sans l'API
  docker compose run --rm pipeline python runner.py backfill [--days N] [--date AAAA-MM-JJ]   # historique
  ```
- **Airflow** : une tâche par étape, avec relances (2 tentatives), historique et logs. Les tâches échangent les données par MinIO et PostgreSQL (pas par XCom). `record_run` journalise le run dans `analytics.pipeline_runs`, puis fait échouer le run si une étape a échoué.

**Le Hook Airflow.** `MarketplaceAPIHook` lit l'URL et le token dans la **Connection Airflow `marketplace_api`**, créée automatiquement au démarrage à partir du `.env` : aucun secret dans le code. Il délègue les appels HTTP à `MarketplaceClient`, que la ligne de commande réutilise : la logique de retry n'existe qu'à un seul endroit.

## 9. Organisation du dépôt

```
api/                    API Flask simulée (données déterministes)
pipeline/
  runner.py             ligne de commande : run, replay, backfill, serve
  marketplace/
    config.py           variables d'environnement (obligatoires) et règles métier
    api_client.py       client de l'API, indépendant d'Airflow
    quality.py          contrôles qualité
    storage.py          archivage raw dans MinIO
    warehouse.py        staging, dwh, analytics et suivi des runs
    metrics.py          métriques Prometheus
    pipeline.py         enchaînement d'un run
airflow/
  dags/                 DAG quotidien : une tâche par étape
  plugins/              MarketplaceAPIHook
sql/001_schema.sql      schémas, tables et vues (créés au premier démarrage)
prometheus/, grafana/   configuration de la supervision
powerbi/, docs/         mesures DAX, guide Metabase
tests/                  tests pytest
docker-compose.yml      tous les services
.env.example            modèle des variables d'environnement
```

Les données générées (base PostgreSQL, objets MinIO, logs Airflow, dashboards Metabase, métriques) vivent dans des **volumes Docker nommés**, jamais dans le dépôt.

## 10. Tests

| Fichier | Ce qu'il vérifie |
|---|---|
| `test_quality.py` | Les contrôles qualité rejettent : jeu vide, doublons, identifiant manquant, date future, montant négatif, quantité nulle |
| `test_api_client.py` | Token Bearer, nouvelles tentatives puis succès, abandon après les tentatives, réponse invalide (`requests` est simulé) |
| `test_idempotence.py` | Charger deux fois le même jour donne le même nombre de faits, sur la vraie base ; nettoyage après passage |
| `test_hook.py` | Le Hook lit l'URL et le token dans la Connection Airflow |
| `test_dag.py` | Aucune erreur d'import, `catchup=False`, ordre des 6 tâches, planification `@daily` |
| `test_project.py` | Tous les services sont déclarés dans le compose |

Commandes : voir l'étape 4.11.

## 11. Choix techniques et limites

**Choix**

- **MinIO comme couche raw** : conserver le JSON brut permet de rejouer un traitement sans rappeler l'API.
- **Trois schémas PostgreSQL** (`staging`, `dwh`, `analytics`) : un rôle clair par couche.
- **`DELETE` + `INSERT` par date** plutôt qu'un upsert : plus simple, plus rapide par lots, état connu après chaque run.
- **Pas de Spark ni de dbt** : ≈ 700 commandes par jour tiennent largement dans PostgreSQL.
- **Airflow orchestre, il ne transforme pas** : la logique reste dans le package `marketplace`, testable sans Airflow ; le DAG en découpe les étapes en tâches.
- **Aucun secret dans le code** : tout vient du `.env`, et une variable manquante arrête le programme avec un message explicite.

**Limites connues**

- Les dashboards Metabase se créent à la main (section 5) : ils ne sont pas provisionnés automatiquement.
- Airflow est en **2.10.5** avec `LocalExecutor` (le cahier des charges mentionne Airflow 3 avec Celery).
- Un seul DAG, découpé en tâches (le cahier propose trois à quatre DAGs).
- Le service MinIO utilise une image communautaire (`ghcr.io/intikhab49/minio-from-source`), lente à démarrer.
- Les métriques Prometheus du pipeline ne couvrent que le processus `pipeline` ; l'historique des exécutions se lit dans `analytics.pipeline_runs`.

**Pistes d'amélioration** : provisionnement automatique des dashboards, alertes en cas de run en échec ou d'anomalie, DAG séparé pour les dimensions, transformations en dbt.

## 12. Dépannage

| Symptôme | Cause probable et solution |
|---|---|
| `Bind for 0.0.0.0:8080 failed` ou `port is already allocated` | Le port est pris. Pour Airflow, changez `AIRFLOW_PORT` dans `.env` puis `docker compose up -d`. Pour un autre service, libérez le port |
| `Variable d'environnement manquante : X` | Le fichier `.env` est absent ou incomplet. `copy .env.example .env` (ou `cp`), puis `docker compose up -d` |
| Un service reste en `starting` ou `unhealthy` longtemps | Normal au premier démarrage (MinIO et Airflow sont lents). Attendez quelques minutes avec `docker compose ps` ; en cas de doute, `docker compose logs <service> --tail 30` |
| `airflow-webserver` reste en `Created` | Il attend la fin de `airflow-init`. Patientez, puis relancez `docker compose up -d` si besoin |
| Metabase : *Cannot connect* | Utilisez l'hôte `postgres-dwh` et le port `5432` (et non `localhost` ni `5433`) |
| Les dashboards ou tables sont vides | Le backfill n'a pas été lancé : étape 4 de la section 3 |
| `401 Unauthorized` sur l'API | Le header `Authorization: Bearer <API_TOKEN>` est absent ou le token est faux |
| Besoin de tout recommencer à zéro | `docker compose down -v` **supprime les volumes** (données, dashboards Metabase). Puis recommencez à l'étape 3 |

Arrêter le projet en conservant les données : `docker compose down`. Le relancer : `docker compose up -d`.

---

**Projet académique : Data Engineering, Data Warehouse et Business Intelligence.**
