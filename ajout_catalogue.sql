
BEGIN;

-- 1. Ajouter les vendeurs jusqu'à 2 500
INSERT INTO dwh.dim_seller
    (seller_id, seller_name, region)
SELECT
    n,
    'Vendeur ' || n,
    (ARRAY[
        'Île-de-France', 'Occitanie', 'Bretagne',
        'Normandie', 'Grand Est', 'Nouvelle-Aquitaine',
        'Auvergne-Rhône-Alpes', 'Hauts-de-France',
        'Pays de la Loire', 'PACA'
    ])[1 + (n % 10)]
FROM generate_series(1, 2500) AS n
ON CONFLICT (seller_id) DO NOTHING;

-- 2. Ajouter les produits jusqu'à 180 000
INSERT INTO dwh.dim_product
    (product_id, product_name, category, price)
SELECT
    n,
    'Produit ' || n,
    (ARRAY[
        'Mode', 'Électronique', 'Maison',
        'Beauté', 'Sport', 'Jardin',
        'Alimentation', 'Accessoires'
    ])[1 + (n % 8)],
    ROUND((10 + random() * 490)::numeric, 2)
FROM generate_series(1, 180000) AS n
ON CONFLICT (product_id) DO NOTHING;

COMMIT;

-- 3. Vérification
SELECT 'Vendeurs' AS element, COUNT(*) AS total
FROM dwh.dim_seller
UNION ALL
SELECT 'Produits', COUNT(*)
FROM dwh.dim_product
UNION ALL
SELECT 'Commandes', COUNT(*)
FROM dwh.fact_orders;
