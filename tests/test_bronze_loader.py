import unittest
from datetime import date
from unittest.mock import MagicMock

from google.cloud import bigquery

from src.bronze_loader import BronzeLoader


class TestBronzeLoader(unittest.TestCase):
    def test_prepare_rows_adds_execution_date_without_mutating_input(self) -> None:
        raw_record = {
            "endpoint": "/categories/MLB1000",
            "category_id": "MLB1000",
            "ingested_at": "2026-11-15T12:00:00+00:00",
            "payload_hash": "abc123",
            "raw_payload": {"id": "MLB1000", "name": "Eletronicos"},
        }

        rows = BronzeLoader.prepare_rows([raw_record], date(2026, 11, 15))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["execution_date"], "2026-11-15")
        self.assertIsInstance(rows[0]["raw_payload"], dict)
        self.assertNotIn("execution_date", raw_record)

    def test_load_partition_skips_empty_records(self) -> None:
        mock_client = MagicMock()
        mock_client.project = "test-project"
        loader = BronzeLoader(project_id="test-project", client=mock_client)

        loaded = loader.load_partition("raw_categories", [], execution_date=date(2026, 11, 15))

        self.assertEqual(loaded, 0)
        mock_client.load_table_from_json.assert_not_called()

    def test_load_partition_uses_decorator_write_truncate_and_json_schema(self) -> None:
        mock_client = MagicMock()
        mock_client.project = "test-project"
        mock_job = MagicMock()
        mock_job.output_rows = 1
        mock_client.load_table_from_json.return_value = mock_job

        loader = BronzeLoader(project_id="test-project", dataset_id="meli_bronze", client=mock_client)
        records = [
            {
                "endpoint": "/categories/MLB1000",
                "category_id": "MLB1000",
                "ingested_at": "2026-11-15T12:00:00+00:00",
                "payload_hash": "abc123",
                "raw_payload": {"id": "MLB1000"},
            }
        ]

        loaded = loader.load_partition("raw_categories", records, execution_date=date(2026, 11, 15))

        self.assertEqual(loaded, 1)
        mock_client.load_table_from_json.assert_called_once()
        _, destination = mock_client.load_table_from_json.call_args[0]
        job_config = mock_client.load_table_from_json.call_args[1]["job_config"]

        self.assertEqual(destination, "test-project.meli_bronze.raw_categories$20261115")
        self.assertEqual(job_config.write_disposition, bigquery.WriteDisposition.WRITE_TRUNCATE)
        self.assertEqual(job_config.source_format, bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
        self.assertFalse(job_config.autodetect)
        raw_field = next(f for f in job_config.schema if f.name == "raw_payload")
        self.assertEqual(raw_field.field_type, "JSON")
        mock_job.result.assert_called_once()

    def test_load_all_loads_all_four_bronze_tables_with_decorators(self) -> None:
        mock_client = MagicMock()
        mock_client.project = "test-project"
        mock_job = MagicMock()
        mock_job.output_rows = 1
        mock_client.load_table_from_json.return_value = mock_job

        sample_row = [
            {
                "endpoint": "/test",
                "category_id": "MLB1",
                "ingested_at": "2026-11-16T00:00:00+00:00",
                "payload_hash": "hash",
                "raw_payload": {"ok": True},
            }
        ]

        loader = BronzeLoader(project_id="test-project", dataset_id="meli_bronze", client=mock_client)
        counts = loader.load_all(
            sample_row,
            sample_row,
            sample_row,
            payment_records=sample_row,
            execution_date=date(2026, 11, 16),
        )

        self.assertEqual(
            counts,
            {
                "raw_categories": 1,
                "raw_trends": 1,
                "raw_items_search": 1,
                "raw_payment_methods": 1,
            },
        )
        destinations = [call.args[1] for call in mock_client.load_table_from_json.call_args_list]
        self.assertEqual(
            destinations,
            [
                "test-project.meli_bronze.raw_categories$20261116",
                "test-project.meli_bronze.raw_trends$20261116",
                "test-project.meli_bronze.raw_items_search$20261116",
                "test-project.meli_bronze.raw_payment_methods$20261116",
            ],
        )


if __name__ == "__main__":
    unittest.main()
