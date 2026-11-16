BEGIN TRANSACTION;

DELETE FROM `meli_silver.dim_payment_methods`
WHERE snapshot_date = @execution_date;

INSERT INTO `meli_silver.dim_payment_methods` (
  snapshot_date,
  payment_method_id,
  payment_method_name,
  payment_type_id,
  supported_payment_types,
  site_id,
  currency_id,
  min_accreditation_days,
  max_accreditation_days,
  total_financial_cost,
  max_installments,
  max_installments_no_interest,
  min_installment_allowed_amount,
  max_installment_allowed_amount,
  is_recommended,
  labels,
  payer_costs,
  ingested_at,
  payload_hash
)
WITH parsed AS (
  SELECT
    execution_date AS snapshot_date,
    COALESCE(JSON_VALUE(raw_payload, '$.payment_method.id'), category_id) AS payment_method_id,
    COALESCE(JSON_VALUE(raw_payload, '$.payment_method.name'), 'UNKNOWN') AS payment_method_name,
    COALESCE(JSON_VALUE(raw_payload, '$.payment_method.payment_type_id'), 'unknown') AS payment_type_id,
    JSON_VALUE_ARRAY(raw_payload, '$.supported_payment_types') AS supported_payment_types,
    JSON_VALUE(raw_payload, '$.payment_method.site_id') AS site_id,
    COALESCE(JSON_VALUE(raw_payload, '$.currency_reference.id'), 'BRL') AS currency_id,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.payment_method.min_accreditation_days') AS INT64) AS min_accreditation_days,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.payment_method.max_accreditation_days') AS INT64) AS max_accreditation_days,
    SAFE_CAST(JSON_VALUE(raw_payload, '$.payment_method.total_financial_cost') AS NUMERIC) AS total_financial_cost,
    JSON_VALUE_ARRAY(raw_payload, '$.payment_method.labels') AS labels,
    ARRAY(
      SELECT AS STRUCT
        SAFE_CAST(JSON_VALUE(pc, '$.installments') AS INT64) AS installments,
        SAFE_CAST(JSON_VALUE(pc, '$.installment_rate') AS NUMERIC) AS installment_rate,
        JSON_VALUE_ARRAY(pc, '$.installment_rate_collector') AS installment_rate_collector,
        SAFE_CAST(JSON_VALUE(pc, '$.min_allowed_amount') AS NUMERIC) AS min_allowed_amount,
        SAFE_CAST(JSON_VALUE(pc, '$.max_allowed_amount') AS NUMERIC) AS max_allowed_amount
      FROM UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.payment_method.payer_costs')) AS pc
    ) AS payer_costs,
    ingested_at,
    payload_hash
  FROM `meli_bronze.raw_payment_methods`
  WHERE execution_date = @execution_date
    AND JSON_VALUE(raw_payload, '$.source') = 'payment_method_detail'
)
SELECT
  snapshot_date,
  payment_method_id,
  payment_method_name,
  payment_type_id,
  supported_payment_types,
  site_id,
  currency_id,
  min_accreditation_days,
  max_accreditation_days,
  total_financial_cost,
  (SELECT MAX(pc.installments) FROM UNNEST(payer_costs) AS pc) AS max_installments,
  (SELECT MAX(IF(COALESCE(pc.installment_rate, 0) = 0, pc.installments, NULL)) FROM UNNEST(payer_costs) AS pc) AS max_installments_no_interest,
  (SELECT MIN(pc.min_allowed_amount) FROM UNNEST(payer_costs) AS pc) AS min_installment_allowed_amount,
  (SELECT MAX(pc.max_allowed_amount) FROM UNNEST(payer_costs) AS pc) AS max_installment_allowed_amount,
  'recommended_method' IN UNNEST(labels) AS is_recommended,
  labels,
  payer_costs,
  ingested_at,
  payload_hash
FROM parsed
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, payment_method_id
  ORDER BY ingested_at DESC, payload_hash ASC
) = 1;

COMMIT TRANSACTION;
