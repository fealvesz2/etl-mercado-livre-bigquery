BEGIN TRANSACTION;

DELETE FROM `meli_silver.fact_daily_trends`
WHERE snapshot_date = @execution_date;

INSERT INTO `meli_silver.fact_daily_trends` (
  snapshot_date,
  trend_source,
  search_term,
  rank_position,
  category_id,
  category_name,
  domain_id,
  domain_name,
  inferred_brand,
  inferred_model,
  trend_url,
  ingested_at,
  payload_hash
)
WITH discovery_trends AS (
  SELECT
    execution_date AS snapshot_date,
    'domain_discovery' AS trend_source,
    COALESCE(JSON_VALUE(raw_payload, '$.query'), 'UNKNOWN') AS search_term,
    pos + 1 AS rank_position,
    COALESCE(JSON_VALUE(match_obj, '$.category_id'), category_id, 'UNKNOWN') AS category_id,
    JSON_VALUE(match_obj, '$.category_name') AS category_name,
    JSON_VALUE(match_obj, '$.domain_id') AS domain_id,
    JSON_VALUE(match_obj, '$.domain_name') AS domain_name,
    (
      SELECT ANY_VALUE(JSON_VALUE(attr, '$.value_name'))
      FROM UNNEST(JSON_QUERY_ARRAY(match_obj, '$.attributes')) AS attr
      WHERE JSON_VALUE(attr, '$.id') = 'BRAND'
    ) AS inferred_brand,
    (
      SELECT ANY_VALUE(JSON_VALUE(attr, '$.value_name'))
      FROM UNNEST(JSON_QUERY_ARRAY(match_obj, '$.attributes')) AS attr
      WHERE JSON_VALUE(attr, '$.id') = 'MODEL'
    ) AS inferred_model,
    CAST(NULL AS STRING) AS trend_url,
    ingested_at,
    payload_hash
  FROM `meli_bronze.raw_trends`,
  UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.matches')) AS match_obj WITH OFFSET AS pos
  WHERE execution_date = @execution_date
    AND JSON_VALUE(raw_payload, '$.source') = 'domain_discovery'
),
official_trends AS (
  SELECT
    execution_date AS snapshot_date,
    'official_trends' AS trend_source,
    COALESCE(JSON_VALUE(trend_obj, '$.keyword'), 'UNKNOWN') AS search_term,
    pos + 1 AS rank_position,
    COALESCE(category_id, 'MLB') AS category_id,
    CAST(NULL AS STRING) AS category_name,
    CAST(NULL AS STRING) AS domain_id,
    CAST(NULL AS STRING) AS domain_name,
    CAST(NULL AS STRING) AS inferred_brand,
    CAST(NULL AS STRING) AS inferred_model,
    JSON_VALUE(trend_obj, '$.url') AS trend_url,
    ingested_at,
    payload_hash
  FROM `meli_bronze.raw_trends`,
  UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.trends')) AS trend_obj WITH OFFSET AS pos
  WHERE execution_date = @execution_date
    AND JSON_VALUE(raw_payload, '$.source') = 'official_trends'
),
unified AS (
  SELECT * FROM discovery_trends
  UNION ALL
  SELECT * FROM official_trends
)
SELECT *
FROM unified
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, trend_source, search_term, rank_position, category_id
  ORDER BY ingested_at DESC, payload_hash ASC
) = 1;

COMMIT TRANSACTION;
