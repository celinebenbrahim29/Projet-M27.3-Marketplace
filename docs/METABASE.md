# Metabase — configuration

1. Ouvrir http://localhost:3000 et créer le compte administrateur.
2. Ajouter une base PostgreSQL : hôte `postgres-dwh`, port `5432`, base `dwh`, utilisateur et mot de passe du `.env`.
3. Créer 4 dashboards :
   - Executive : `analytics.daily_summary` (CA journalier, commandes, panier moyen).
   - Top Sellers : `analytics.seller_daily` (top 10 par somme de revenue).
   - Finance : `analytics.daily_summary` (commissions par jour).
   - Catégories : `analytics.category_daily` (CA par catégorie).
4. Créer une question anomalies depuis `analytics.sales_anomalies` et l'ajouter au dashboard Executive.
5. Ajouter des filtres dates. Les dashboards Metabase doivent être créés dans l'interface et sont persistés dans le volume Docker.
