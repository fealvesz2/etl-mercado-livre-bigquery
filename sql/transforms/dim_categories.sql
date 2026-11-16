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
  category_depth,
  is_leaf,
  listing_allowed,
  catalog_domain,
  vertical,
  sub_vertical,
  shipping_profile,
  total_items,
  children_count,
  total_attributes_count,
  required_attributes_count,
  catalog_required_attributes_count,
  conditional_required_attributes_count,
  variation_attributes_count,
  shipping_options,
  buying_modes,
  minimum_price,
  maximum_price,
  max_pictures_per_item,
  max_variations_allowed,
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
    COALESCE(ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_payload, '$.path_from_root')), 1) AS category_depth,
    COALESCE(ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_payload, '$.children_categories')) = 0, FALSE) AS is_leaf,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.listing_allowed') AS BOOL) AS listing_allowed,
    JSON_VALUE(raw_payload, '$.settings.catalog_domain') AS catalog_domain,
    JSON_VALUE(raw_payload, '$.settings.vertical') AS vertical,
    JSON_VALUE(raw_payload, '$.settings.sub_vertical') AS sub_vertical,
    JSON_VALUE(raw_payload, '$.settings.shipping_profile') AS shipping_profile,
    COALESCE(SAFE_CAST(JSON_VALUE(raw_payload, '$.total_items_in_this_category') AS INT64), 0) AS total_items,
    COALESCE(ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_payload, '$.children_categories')), 0) AS children_count,
    CAST(NULL AS INT64) AS total_attributes_count,
    CAST(NULL AS INT64) AS required_attributes_count,
    CAST(NULL AS INT64) AS catalog_required_attributes_count,
    CAST(NULL AS INT64) AS conditional_required_attributes_count,
    CAST(NULL AS INT64) AS variation_attributes_count,
    JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options') AS shipping_options,
    JSON_VALUE_ARRAY(raw_payload, '$.settings.buying_modes') AS buying_modes,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.minimum_price') AS NUMERIC) AS minimum_price,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.maximum_price') AS NUMERIC) AS maximum_price,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.max_pictures_per_item') AS INT64) AS max_pictures_per_item,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.max_variations_allowed') AS INT64) AS max_variations_allowed,
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
    CASE
      WHEN COALESCE(
        SAFE_CAST(JSON_VALUE(raw_payload, '$.category_depth') AS INT64),
        ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_payload, '$.path_from_root')),
        2
      ) = 1 THEN 'MACRO'
      ELSE 'NICHE_OR_LEAF'
    END AS category_level,
    COALESCE(
      SAFE_CAST(JSON_VALUE(raw_payload, '$.category_depth') AS INT64),
      ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_payload, '$.path_from_root'))
    ) AS category_depth,
    COALESCE(SAFE_CAST(JSON_VALUE(raw_payload, '$.is_leaf') AS BOOL), FALSE) AS is_leaf,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.listing_allowed') AS BOOL) AS listing_allowed,
    JSON_VALUE(raw_payload, '$.settings.catalog_domain') AS catalog_domain,
    JSON_VALUE(raw_payload, '$.settings.vertical') AS vertical,
    JSON_VALUE(raw_payload, '$.settings.sub_vertical') AS sub_vertical,
    JSON_VALUE(raw_payload, '$.settings.shipping_profile') AS shipping_profile,
    COALESCE(SAFE_CAST(JSON_VALUE(raw_payload, '$.total_items_in_this_category') AS INT64), 0) AS total_items,
    COALESCE(SAFE_CAST(JSON_VALUE(raw_payload, '$.children_count') AS INT64), 0) AS children_count,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.total_attributes_count') AS INT64) AS total_attributes_count,
    COALESCE(
      SAFE_CAST(JSON_VALUE(raw_payload, '$.attribute_summary.required') AS INT64),
      (
        SELECT COUNTIF(COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.required') AS BOOL), FALSE))
        FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
      )
    ) AS required_attributes_count,
    COALESCE(
      SAFE_CAST(JSON_VALUE(raw_payload, '$.attribute_summary.catalog_required') AS INT64),
      (
        SELECT COUNTIF(COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.catalog_required') AS BOOL), FALSE))
        FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
      )
    ) AS catalog_required_attributes_count,
    COALESCE(
      SAFE_CAST(JSON_VALUE(raw_payload, '$.attribute_summary.conditional_required') AS INT64),
      (
        SELECT COUNTIF(COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.conditional_required') AS BOOL), FALSE))
        FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
      )
    ) AS conditional_required_attributes_count,
    COALESCE(
      SAFE_CAST(JSON_VALUE(raw_payload, '$.attribute_summary.variation_attributes') AS INT64),
      (
        SELECT COUNTIF(
          COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.allow_variations') AS BOOL), FALSE)
          OR COALESCE(SAFE_CAST(JSON_VALUE(attr, '$.variation_attribute') AS BOOL), FALSE)
        )
        FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.key_attributes')) AS attr
      )
    ) AS variation_attributes_count,
    JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options') AS shipping_options,
    JSON_VALUE_ARRAY(raw_payload, '$.settings.buying_modes') AS buying_modes,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.minimum_price') AS NUMERIC) AS minimum_price,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.maximum_price') AS NUMERIC) AS maximum_price,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.max_pictures_per_item') AS INT64) AS max_pictures_per_item,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.settings.max_variations_allowed') AS INT64) AS max_variations_allowed,
    JSON_VALUE(raw_payload, '$.permalink') AS permalink,
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
  ORDER BY
    CASE WHEN total_attributes_count IS NOT NULL THEN 0 ELSE 1 END ASC,
    ingested_at DESC,
    payload_hash ASC
) = 1;

COMMIT TRANSACTION;
