BEGIN TRANSACTION;

DELETE FROM `meli_silver.dim_categories`
WHERE snapshot_date = @execution_date;

INSERT INTO `meli_silver.dim_categories` (
  snapshot_date,
  category_id,
  category_name,
  parent_category_id,
  parent_category_name,
  category_level,
  is_leaf,
  listing_allowed,
  catalog_domain,
  total_items,
  children_count,
  total_attributes_count,
  required_attributes_count,
  catalog_required_attributes_count,
  shipping_options,
  buying_modes,
  minimum_price,
  permalink,
  ingested_at,
  payload_hash
)
WITH macro_categories AS (
  SELECT
    execution_date AS snapshot_date,
    COALESCE(JSON_VALUE(raw_payload, '$.id'), category_id) AS category_id,
    COALESCE(JSON_VALUE(raw_payload, '$.name'), 'UNKNOWN') AS category_name,
    COALESCE(JSON_VALUE(raw_payload, '$.path_from_root[0].id'), category_id) AS parent_category_id,
    COALESCE(JSON_VALUE(raw_payload, '$.path_from_root[0].name'), JSON_VALUE(raw_payload, '$.name')) AS parent_category_name,
    'MACRO' AS category_level,
    COALESCE(ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_payload, '$.children_categories')) = 0, FALSE) AS is_leaf,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.listing_allowed') AS BOOL) AS listing_allowed,
    JSON_VALUE(raw_payload, '$.settings.catalog_domain') AS catalog_domain,
    COALESCE(SAFE_CAST(JSON_VALUE(raw_payload, '$.total_items_in_this_category') AS INT64), 0) AS total_items,
    COALESCE(ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_payload, '$.children_categories')), 0) AS children_count,
    CAST(NULL AS INT64) AS total_attributes_count,
    CAST(NULL AS INT64) AS required_attributes_count,
    CAST(NULL AS INT64) AS catalog_required_attributes_count,
    JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options') AS shipping_options,
    JSON_VALUE_ARRAY(raw_payload, '$.settings.buying_modes') AS buying_modes,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.minimum_price') AS NUMERIC) AS minimum_price,
    JSON_VALUE(raw_payload, '$.permalink') AS permalink,
    ingested_at,
    payload_hash
  FROM `meli_bronze.raw_categories`
  WHERE execution_date = @execution_date
),
leaf_niches AS (
  SELECT
    execution_date AS snapshot_date,
    COALESCE(JSON_VALUE(raw_payload, '$.category_id'), category_id) AS category_id,
    COALESCE(JSON_VALUE(raw_payload, '$.category_name'), 'UNKNOWN') AS category_name,
    JSON_VALUE(raw_payload, '$.path_from_root[0].id') AS parent_category_id,
    JSON_VALUE(raw_payload, '$.path_from_root[0].name') AS parent_category_name,
    'NICHE_OR_LEAF' AS category_level,
    COALESCE(SAFE_CAST(JSON_VALUE(raw_payload, '$.is_leaf') AS BOOL), FALSE) AS is_leaf,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.listing_allowed') AS BOOL) AS listing_allowed,
    JSON_VALUE(raw_payload, '$.settings.catalog_domain') AS catalog_domain,
    COALESCE(SAFE_CAST(JSON_VALUE(raw_payload, '$.total_items_in_this_category') AS INT64), 0) AS total_items,
    CAST(0 AS INT64) AS children_count,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.total_attributes_count') AS INT64) AS total_attributes_count,
    (
      SELECT COUNTIF(COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.required') AS BOOL), FALSE))
      FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
    ) AS required_attributes_count,
    (
      SELECT COUNTIF(COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.catalog_required') AS BOOL), FALSE))
      FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
    ) AS catalog_required_attributes_count,
    JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options') AS shipping_options,
    JSON_VALUE_ARRAY(raw_payload, '$.settings.buying_modes') AS buying_modes,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.minimum_price') AS NUMERIC) AS minimum_price,
    CAST(NULL AS STRING) AS permalink,
    ingested_at,
    payload_hash
  FROM `meli_bronze.raw_items_search`
  WHERE execution_date = @execution_date
    AND JSON_VALUE(raw_payload, '$.source') = 'public_leaf_enrichment'
),
unified AS (
  SELECT * FROM macro_categories
  UNION ALL
  SELECT * FROM leaf_niches
)
SELECT *
FROM unified
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, category_id
  ORDER BY ingested_at DESC, payload_hash ASC
) = 1;

COMMIT TRANSACTION;
