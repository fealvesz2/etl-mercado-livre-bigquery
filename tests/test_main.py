import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from src.main import parse_args, run_pipeline


class TestMainOrchestrator(unittest.TestCase):
    def test_gold_views_sql_exists_and_has_no_hardcoded_project(self) -> None:
        gold_path = Path("sql/03_gold_views.sql")
        self.assertTrue(gold_path.exists())
        content = gold_path.read_text(encoding="utf-8")
        self.assertIn("vw_price_monitoring", content)
        self.assertIn("vw_logistics_share", content)
        self.assertIn("vw_trends_vs_offers", content)
        self.assertNotIn("dev-plataforma-geral", content)

    def test_parse_args_supports_execution_date_and_skip_extract(self) -> None:
        args = parse_args(["--execution-date", "2026-11-15", "--skip-extract", "--log-level", "DEBUG"])
        self.assertEqual(args.execution_date, date(2026, 11, 15))
        self.assertTrue(args.skip_extract)
        self.assertEqual(args.log_level, "DEBUG")

    def test_run_pipeline_executes_extract_bronze_and_silver_in_order(self) -> None:
        mock_client = MagicMock()
        mock_client.is_oauth = False
        mock_client.fetch_categories.return_value = [{"id": 1}]
        mock_client.fetch_trends_and_domains.return_value = [{"id": 2}]
        mock_client.fetch_items_or_niches_snapshot.return_value = [{"id": 3}]

        mock_loader = MagicMock()
        mock_loader.load_all.return_value = {
            "raw_categories": 5,
            "raw_trends": 10,
            "raw_items_search": 19,
        }

        mock_transformer = MagicMock()
        mock_transformer.run_all.return_value = {
            "dim_categories": {"bytes_billed": 31_457_280},
            "fact_daily_item_prices": {"bytes_billed": 20_971_520},
            "fact_daily_trends": {"bytes_billed": 20_971_520},
        }

        target = date(2026, 11, 15)
        summary = run_pipeline(
            execution_date=target,
            meli_client=mock_client,
            bronze_loader=mock_loader,
            silver_transformer=mock_transformer,
        )

        self.assertEqual(summary["execution_date"], "2026-11-15")
        self.assertEqual(summary["mode"], "PUBLIC")
        mock_loader.load_all.assert_called_once_with(
            [{"id": 1}], [{"id": 2}], [{"id": 3}], execution_date=target
        )
        mock_transformer.run_all.assert_called_once_with(execution_date=target)
        self.assertEqual(summary["total_silver_billed_mib"], 70.0)

    def test_run_pipeline_skip_extract_runs_only_silver(self) -> None:
        mock_client = MagicMock()
        mock_loader = MagicMock()
        mock_transformer = MagicMock()
        mock_transformer.run_all.return_value = {
            "dim_categories": {"bytes_billed": 10_485_760},
        }

        target = date(2026, 11, 15)
        summary = run_pipeline(
            execution_date=target,
            skip_extract=True,
            meli_client=mock_client,
            bronze_loader=mock_loader,
            silver_transformer=mock_transformer,
        )

        mock_client.fetch_categories.assert_not_called()
        mock_loader.load_all.assert_not_called()
        mock_transformer.run_all.assert_called_once_with(execution_date=target)
        self.assertEqual(summary["bronze_loaded_rows"], {})


if __name__ == "__main__":
    unittest.main()
