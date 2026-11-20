# Power BI — Mesures DAX

Dans Power BI Desktop, connecter PostgreSQL `localhost:5433`, base `dwh`, schéma `analytics` et importer `daily_summary`, `seller_daily`, `category_daily`, `sales_anomalies`. Préférer Import pour ce TP.

Créer les mesures sur `daily_summary` :

```dax
CA Total = SUM(daily_summary[revenue])
Nombre Commandes = SUM(daily_summary[total_orders])
Panier Moyen = DIVIDE([CA Total], [Nombre Commandes], 0)
Commissions = SUM(daily_summary[commission])
```

Pages proposées :
1. Executive Overview : cartes CA, commandes, panier, commissions, série temporelle CA.
2. Sellers & Categories : top 10 vendeurs (seller_daily), CA par catégorie (category_daily).
3. Anomalies : table des dates, moyenne 7 jours, ratio, statut (sales_anomalies).

**Important** : ne pas joindre `daily_summary`, `seller_daily` et `category_daily` directement sur `dt` dans un seul modèle avec relations plusieurs-à-plusieurs ; utiliser une dimension Date partagée ou des pages indépendantes.

Le fichier `.pbix` ne peut être produit fidèlement qu'en enregistrant le rapport depuis Power BI Desktop. Aucun `.pbix` préconstruit n'est inclus.
