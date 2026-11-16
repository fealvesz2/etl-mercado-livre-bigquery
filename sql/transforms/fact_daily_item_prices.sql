BEGIN TRANSACTION;

DELETE FROM `meli_silver.fact_daily_item_prices`
WHERE snapshot_date = @execution_date;

INSERT INTO `meli_silver.fact_daily_item_prices` (
  snapshot_date,
  record_id,
  category_id,
  category_name,
  data_mode,
  title,
  user_product_id,
  price,
  base_price,
  original_price,
  discount_pct,
  currency_id,
  available_quantity,
  sold_quantity,
  condition,
  status,
  catalog_listing,
  logistic_type,
  free_shipping,
  supports_carrier_shipping,
  requires_gtin,
  required_attrs_count,
  catalog_required_attrs_count,
  permalink,
  ingested_at,
  payload_hash
)
WITH oauth_items AS (
  SELECT
    execution_date AS snapshot_date,
    COALESCE(JSON_VALUE(raw_payload, '$.item.id'), payload_hash) AS record_id,
    COALESCE(JSON_VALUE(raw_payload, '$.item.category_id'), category_id, 'UNKNOWN') AS category_id,
    CAST(NULL AS STRING) AS category_name,
    'OAUTH_MULTIGET' AS data_mode,
    JSON_VALUE(raw_payload, '$.item.title') AS title,
    JSON_VALUE(raw_payload, '$.item.user_product_id') AS user_product_id,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.item.price') AS NUMERIC) AS price,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.item.base_price') AS NUMERIC) AS base_price,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.item.original_price') AS NUMERIC) AS original_price,
    CASE
      WHEN SAFE_CAST(JSON_VALUE(raw_payload, '$.item.original_price') AS NUMERIC)
        > SAFE_CAST(JSON_VALUE(raw_payload, '$.item.price') AS NUMERIC)
      THEN ROUND(
        SAFE_DIVIDE(
          SAFE_CAST(JSON_VALUE(raw_payload, '$.item.original_price') AS NUMERIC)
            - SAFE_CAST(JSON_VALUE(raw_payload, '$.item.price') AS NUMERIC),
          SAFE_CAST(JSON_VALUE(raw_payload, '$.item.original_price') AS NUMERIC)
        ) * 100,
        2
      )
      ELSE CAST(0 AS NUMERIC)
    END AS discount_pct,
    COALESCE(JSON_VALUE(raw_payload, '$.item.currency_id'), 'BRL') AS currency_id,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.item.available_quantity') AS INT64) AS available_quantity,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.item.sold_quantity') AS INT64) AS sold_quantity,
    JSON_VALUE(raw_payload, '$.item.condition') AS condition,
    JSON_VALUE(raw_payload, '$.item.status') AS status,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.item.catalog_listing') AS BOOL) AS catalog_listing,
    COALESCE(JSON_VALUE(raw_payload, '$.item.shipping.logistic_type'), 'unknown') AS logistic_type,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.item.shipping.free_shipping') AS BOOL) AS free_shipping,
    COALESCE(
      JSON_VALUE(raw_payload, '$.item.shipping.logistic_type')
        IN ('fulfillment', 'xd_drop_off', 'cross_docking', 'drop_off'),
      FALSE
    ) AS supports_carrier_shipping,
    CAST(NULL AS BOOL) AS requires_gtin,
    CAST(NULL AS INT64) AS required_attrs_count,
    CAST(NULL AS INT64) AS catalog_required_attrs_count,
    JSON_VALUE(raw_payload, '$.item.permalink') AS permalink,
    ingested_at,
    payload_hash
  FROM `meli_bronze.raw_items_search`
  WHERE execution_date = @execution_date
    AND JSON_VALUE(raw_payload, '$.source') = 'items_multiget'
),
public_niches AS (
  SELECT
    execution_date AS snapshot_date,
    COALESCE(JSON_VALUE(raw_payload, '$.category_id'), category_id, payload_hash) AS record_id,
    COALESCE(JSON_VALUE(raw_payload, '$.category_id'), category_id, 'UNKNOWN') AS category_id,
    JSON_VALUE(raw_payload, '$.category_name') AS category_name,
    'PUBLIC_NICHE_SPEC' AS data_mode,
    JSON_VALUE(raw_payload, '$.category_name') AS title,
    CAST(NULL AS STRING) AS user_product_id,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.minimum_price') AS NUMERIC) AS price,
    CAST(NULL AS NUMERIC) AS base_price,
    CAST(NULL AS NUMERIC) AS original_price,
    CAST(0 AS NUMERIC) AS discount_pct,
    COALESCE(JSON_VALUE(raw_payload, '$.settings.minimum_price_currency'), 'BRL') AS currency_id,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.total_items_in_this_category') AS INT64) AS available_quantity,
    CAST(NULL AS INT64) AS sold_quantity,
    ARRAY_TO_STRING(JSON_VALUE_ARRAY(raw_payload, '$.settings.item_conditions'), ',') AS condition,
    JSON_VALUE(raw_payload, '$.settings.status') AS status,
    JSON_VALUE(raw_payload, '$.settings.catalog_domain') IS NOT NULL AS catalog_listing,
    CASE
      WHEN 'carrier' IN UNNEST(JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options')) THEN 'carrier'
      WHEN 'custom' IN UNNEST(JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options')) THEN 'custom'
      ELSE COALESCE(
        JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options')[SAFE_OFFSET(0)],
        'unknown'
      )
    END AS logistic_type,
    CAST(NULL AS BOOL) AS free_shipping,
    'carrier' IN UNNEST(JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options')) AS supports_carrier_shipping,
    (
      SELECT COALESCE(
        LOGICAL_OR(
          JSON_VALUE(attr, '$.id') = 'GTIN'
          AND (
            COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.required') AS BOOL), FALSE)
            OR COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.catalog_required') AS BOOL), FALSE)
            OR COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.conditional_required') AS BOOL), FALSE)
          )
        ),
        FALSE
      )
      FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
    ) AS requires_gtin,
    COALESCE(
      SAFE_CAST(JSON_VALUE(raw_payload, '$.attribute_summary.required') AS INT64),
      (
        SELECT COUNTIF(COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.required') AS BOOL), FALSE))
        FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
      )
    ) AS required_attrs_count,
    COALESCE(
      SAFE_CAST(JSON_VALUE(raw_payload, '$.attribute_summary.catalog_required') AS INT64),
      (
        SELECT COUNTIF(COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.catalog_required') AS BOOL), FALSE))
        FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
      )
    ) AS catalog_required_attrs_count,
    JSON_VALUE(raw_payload, '$.permalink') AS permalink,
    ingested_at,
    payload_hash
  FROM `meli_bronze.raw_items_search`
  WHERE execution_date = @execution_date
    AND JSON_VALUE(raw_payload, '$.source') = 'public_leaf_enrichment'
),
unified AS (
  SELECT * FROM oauth_items
  UNION ALL
  SELECT * FROM public_niches
)
SELECT *
FROM unified
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, record_id
  ORDER BY ingested_at DESC, payload_hash ASC
) = 1;

COMMIT TRANSACTION;
