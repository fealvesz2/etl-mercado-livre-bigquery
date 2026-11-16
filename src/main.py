import argparse
import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional, Sequence

from src.bronze_loader import BronzeLoader
from src.meli_client import MeliClient
from src.silver_transformer import SilverTransformer

logger = logging.getLogger(__name__)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Analisa argumentos de linha de comando para execucao local ou Cloud Run Job."""
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Medallion Mercado Livre (MLB) -> BigQuery"
    )
    parser.add_argument(
        "--execution-date",
        type=date.fromisoformat,
        default=None,
        help="Data logica UTC da particao no formato YYYY-MM-DD (padrao: hoje em UTC).",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Pula extracao/Bronze e reprocessa apenas transformacoes Silver da particao.",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="Nivel de log da execucao.",
    )
    return parser.parse_args(argv)


def run_pipeline(
    execution_date: Optional[date] = None,
    skip_extract: bool = False,
    meli_client: Optional[MeliClient] = None,
    bronze_loader: Optional[BronzeLoader] = None,
    silver_transformer: Optional[SilverTransformer] = None,
) -> Dict[str, Any]:
    """Orquestra Extract -> Bronze Batch Load -> Silver SQL Transform de forma idempotente."""
    target_date = execution_date or datetime.now(timezone.utc).date()
    summary: Dict[str, Any] = {
        "execution_date": target_date.isoformat(),
        "skip_extract": skip_extract,
        "mode": None,
        "bronze_loaded_rows": {},
        "silver_bytes_billed": {},
    }

    if not skip_extract:
        client = meli_client or MeliClient()
        loader = bronze_loader or BronzeLoader()
        try:
            summary["mode"] = "OAUTH" if client.is_oauth else "PUBLIC"
            logger.info(
                "Iniciando etapa Extract + Bronze Load | data=%s | modo=%s",
                summary["execution_date"],
                summary["mode"],
            )
            categories = client.fetch_categories()
            trends = client.fetch_trends_and_domains()
            items_or_niches = client.fetch_items_or_niches_snapshot(categories, trends)
            payments = client.fetch_payment_methods()
        finally:
            if meli_client is None:
                client.close()

        bronze_counts = loader.load_all(
            categories,
            trends,
            items_or_niches,
            payment_records=payments,
            execution_date=target_date,
        )
        summary["bronze_loaded_rows"] = bronze_counts
    else:
        logger.info("Etapa Extract/Bronze ignorada (--skip-extract) para data=%s.", summary["execution_date"])

    transformer = silver_transformer or SilverTransformer()
    silver_stats = transformer.run_all(execution_date=target_date)
    summary["silver_bytes_billed"] = {
        table: int(stats["bytes_billed"]) for table, stats in silver_stats.items()
    }
    summary["total_silver_billed_mib"] = round(
        sum(summary["silver_bytes_billed"].values()) / (1024 * 1024), 2
    )

    logger.info("Pipeline Medallion concluido com sucesso: %s", json.dumps(summary))
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Ponto de entrada CLI retornando codigo de saida POSIX."""
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = run_pipeline(
        execution_date=args.execution_date,
        skip_extract=args.skip_extract,
    )
    print(f"PIPELINE ETL MELI OK | {json.dumps(summary, ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
