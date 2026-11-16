import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from src.silver_transformer import (
    DEFAULT_MAX_BYTES_BILLED,
    SILVER_TABLES,
    SilverTransformer,
)


class TestSilverTransformer(unittest.TestCase):
    def test_sql_scripts_use_qualify_and_have_no_hardcoded_project(self) -> None:
        self.assertEqual(
            SILVER_TABLES,
            ("dim_categories", "fact_daily_item_prices", "fact_daily_trends", "dim_payment_methods"),
        )
        for table_name in SILVER_TABLES:
            sql_path = Path("sql/transforms") / f"{table_name}.sql"
            self.assertTrue(sql_path.exists(), f"Missing SQL file: {sql_path}")
            content = sql_path.read_text(encoding="utf-8")
            self.assertIn("BEGIN TRANSACTION;", content)
            self.assertIn("COMMIT TRANSACTION;", content)
            self.assertIn("@execution_date", content)
            self.assertIn("QUALIFY ROW_NUMBER() OVER", content)
            self.assertNotIn("dev-plataforma-geral", content)

    def test_silver_sql_contracts_cover_deterministic_logistics_and_enriched_fields(self) -> None:
        prices_sql = Path("sql/transforms/fact_daily_item_prices.sql").read_text(encoding="utf-8")
        self.assertIn("WHEN 'carrier' IN UNNEST(JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options')) THEN 'carrier'", prices_sql)
        self.assertIn("WHEN 'custom' IN UNNEST(JSON_VALUE_ARRAY(raw_payload, '$.settings.shipping_options')) THEN 'custom'", prices_sql)

        dim_cat_sql = Path("sql/transforms/dim_categories.sql").read_text(encoding="utf-8")
        self.assertIn("JSON_VALUE(raw_payload, '$.permalink') AS permalink", dim_cat_sql)
        self.assertIn("$.children_count", dim_cat_sql)
        self.assertIn("category_depth", dim_cat_sql)

        bronze_ddl = Path("sql/01_bronze_ddl.sql").read_text(encoding="utf-8")
        self.assertIn("meli_bronze.raw_payment_methods", bronze_ddl)
        self.assertIn("require_partition_filter=TRUE", bronze_ddl)

        silver_ddl = Path("sql/02_silver_ddl.sql").read_text(encoding="utf-8")
        self.assertIn("meli_silver.dim_payment_methods", silver_ddl)
        self.assertIn("ADD COLUMN IF NOT EXISTS category_depth", silver_ddl)

    def test_invalid_dataset_identifier_raises_value_error(self) -> None:
        mock_client = MagicMock()
        mock_client.project = "test-project"
        with self.assertRaises(ValueError):
            SilverTransformer(
                project_id="test-project",
                silver_dataset="meli_silver; DROP TABLE",
                client=mock_client,
            )

    def test_run_all_passes_parameter_finops_limit_and_labels(self) -> None:
        mock_client = MagicMock()
        mock_client.project = "test-project"
        mock_job = MagicMock()
        mock_job.job_id = "job_123"
        mock_job.total_bytes_billed = 10_485_760
        mock_client.query.return_value = mock_job

        transformer = SilverTransformer(project_id="test-project", client=mock_client)
        target_date = date(2026, 11, 15)
        results = transformer.run_all(execution_date=target_date)

        self.assertEqual(tuple(results.keys()), SILVER_TABLES)
        self.assertEqual(mock_client.query.call_count, 4)

        first_call = mock_client.query.call_args_list[0]
        job_config = first_call.kwargs["job_config"]
        self.assertEqual(job_config.maximum_bytes_billed, DEFAULT_MAX_BYTES_BILLED)
        self.assertEqual(job_config.labels["datacloud"], "antigravity")
        self.assertEqual(job_config.labels["layer"], "silver")
        param = job_config.query_parameters[0]
        self.assertEqual(param.name, "execution_date")
        self.assertEqual(param.type_, "DATE")
        self.assertEqual(param.value, target_date)


if __name__ == "__main__":
    unittest.main()
