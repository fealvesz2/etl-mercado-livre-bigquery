CREATE OR REPLACE VIEW `meli_gold.vw_price_monitoring`
OPTIONS(
  description="Camada Gold - Monitoramento diario de precos, descontos, ofertas e exigencia de catalogo/GTIN por categoria (janela padrao 30d)"
) AS
WITH prices_window AS (
  SELECT *
  FROM `meli_silver.fact_daily_item_prices`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 30 DAY)
),
categories_window AS (
  SELECT *
  FROM `meli_silver.dim_categories`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 30 DAY)
)
SELECT
  f.snapshot_date,
  f.data_mode,
  d.parent_category_id,
  d.parent_category_name,
  f.category_id,
  COALESCE(f.category_name, d.category_name) AS category_name,
  COUNT(DISTINCT f.record_id) AS monitored_records_count,
  ROUND(AVG(f.price), 2) AS avg_price,
  MIN(f.price) AS min_price,
  MAX(f.price) AS max_price,
  ROUND(AVG(f.discount_pct), 2) AS avg_discount_pct,
  COUNTIF(COALESCE(f.discount_pct, 0) > 0) AS discounted_items_count,
  SUM(COALESCE(f.available_quantity, 0)) AS total_available_offers,
  SUM(COALESCE(f.sold_quantity, 0)) AS total_sold_units,
  ROUND(SAFE_DIVIDE(COUNTIF(COALESCE(f.catalog_listing, FALSE)), COUNT(*)) * 100, 2) AS catalog_adoption_pct,
  COUNTIF(COALESCE(f.requires_gtin, FALSE)) AS gtin_required_count
FROM prices_window AS f
LEFT JOIN categories_window AS d
  ON f.snapshot_date = d.snapshot_date
 AND f.category_id = d.category_id
GROUP BY
  f.snapshot_date,
  f.data_mode,
  d.parent_category_id,
  d.parent_category_name,
  f.category_id,
  category_name;

CREATE OR REPLACE VIEW `meli_gold.vw_logistics_share`
OPTIONS(
  description="Camada Gold - Distribuicao de modalidades logisticas e cobertura de frete gratis / Mercado Envios (janela padrao 30d)"
) AS
WITH prices_window AS (
  SELECT *
  FROM `meli_silver.fact_daily_item_prices`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 30 DAY)
),
categories_window AS (
  SELECT *
  FROM `meli_silver.dim_categories`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 30 DAY)
),
aggregated AS (
  SELECT
    f.snapshot_date,
    f.data_mode,
    COALESCE(d.parent_category_id, f.category_id) AS parent_category_id,
    COALESCE(d.parent_category_name, f.category_name, 'Outros') AS parent_category_name,
    COALESCE(f.logistic_type, 'unknown') AS logistic_type,
    COUNT(*) AS records_count,
    COUNTIF(COALESCE(f.supports_carrier_shipping, FALSE)) AS carrier_supported_count,
    COUNTIF(COALESCE(f.free_shipping, FALSE)) AS free_shipping_count,
    COUNTIF(f.free_shipping IS NOT NULL) AS free_shipping_known_count,
    SUM(COALESCE(f.available_quantity, 0)) AS total_offers_in_modality
  FROM prices_window AS f
  LEFT JOIN categories_window AS d
    ON f.snapshot_date = d.snapshot_date
   AND f.category_id = d.category_id
  GROUP BY
    f.snapshot_date,
    f.data_mode,
    parent_category_id,
    parent_category_name,
    logistic_type
)
SELECT
  snapshot_date,
  data_mode,
  parent_category_id,
  parent_category_name,
  logistic_type,
  records_count,
  ROUND(
    SAFE_DIVIDE(records_count, SUM(records_count) OVER (PARTITION BY snapshot_date, data_mode)) * 100,
    2
  ) AS share_pct,
  carrier_supported_count,
  free_shipping_count,
  ROUND(SAFE_DIVIDE(free_shipping_count, NULLIF(free_shipping_known_count, 0)) * 100, 2) AS free_shipping_pct,
  total_offers_in_modality
FROM aggregated;

CREATE OR REPLACE VIEW `meli_gold.vw_trends_vs_offers`
OPTIONS(
  description="Camada Gold - Cruzamento entre tendencias/descoberta de dominios e oferta ativa/barreira tecnica por categoria (janela padrao 30d)"
) AS
WITH trends_window AS (
  SELECT *
  FROM `meli_silver.fact_daily_trends`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 30 DAY)
),
categories_window AS (
  SELECT *
  FROM `meli_silver.dim_categories`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 30 DAY)
)
SELECT
  t.snapshot_date,
  t.trend_source,
  t.search_term,
  t.rank_position,
  t.category_id,
  COALESCE(t.category_name, d.category_name) AS category_name,
  d.parent_category_id,
  d.parent_category_name,
  t.domain_id,
  t.domain_name,
  t.inferred_brand,
  t.inferred_model,
  d.total_items AS category_active_offers,
  d.is_leaf,
  d.listing_allowed,
  d.required_attributes_count,
  d.catalog_required_attributes_count,
  d.minimum_price AS category_min_price,
  CASE
    WHEN d.total_items IS NULL THEN 'NAO_ENRIQUECIDA'
    WHEN d.total_items >= 500000 THEN 'ALTA_CONCORRENCIA'
    WHEN d.total_items >= 50000 THEN 'MEDIA_CONCORRENCIA'
    ELSE 'NICHO_ESPECIFICO'
  END AS competition_bucket,
  ROUND(
    SAFE_DIVIDE(1000000.0, NULLIF(d.total_items, 0) * t.rank_position),
    4
  ) AS opportunity_score_ratio
FROM trends_window AS t
LEFT JOIN categories_window AS d
  ON t.snapshot_date = d.snapshot_date
 AND t.category_id = d.category_id;
