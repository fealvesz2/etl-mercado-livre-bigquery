import logging
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv
from google.cloud import bigquery

logger = logging.getLogger(__name__)

DEFAULT_MAX_BYTES_BILLED = 52_428_800  # 50 MiB FinOps guardrail
SILVER_TABLES: Tuple[str, ...] = (
    "dim_categories",
    "fact_daily_item_prices",
    "fact_daily_trends",
)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SilverTransformer:
    """Executa transformacoes SQL parametrizadas Bronze -> Silver com trava FinOps."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        bronze_dataset: Optional[str] = None,
        silver_dataset: Optional[str] = None,
        location: Optional[str] = None,
        sql_dir: Path = Path("sql/transforms"),
        max_bytes_billed: Optional[int] = None,
        client: Optional[bigquery.Client] = None,
    ) -> None:
        load_dotenv()
        resolved_project = project_id or os.getenv("GCP_PROJECT_ID") or None
        self.bronze_dataset = self._validate_identifier(
            bronze_dataset or os.getenv("BQ_DATASET_BRONZE", "meli_bronze")
        )
        self.silver_dataset = self._validate_identifier(
            silver_dataset or os.getenv("BQ_DATASET_SILVER", "meli_silver")
        )
        self.location = location or os.getenv("BQ_LOCATION", "southamerica-east1")
        self.sql_dir = sql_dir
        self.max_bytes_billed = (
            max_bytes_billed
            if max_bytes_billed is not None
            else int(os.getenv("BQ_MAX_BYTES_BILLED", DEFAULT_MAX_BYTES_BILLED))
        )
        self.client = (
            client
            if client is not None
            else bigquery.Client(project=resolved_project, location=self.location)
        )
        self.project_id = self.client.project

    @staticmethod
    def _validate_identifier(name: str) -> str:
        if not _IDENTIFIER_RE.match(name):
            raise ValueError(f"Identificador BigQuery invalido: {name!r}")
        return name

    def _load_sql(self, table_name: str) -> str:
        sql_path = self.sql_dir / f"{table_name}.sql"
        sql = sql_path.read_text(encoding="utf-8")
        if self.bronze_dataset != "meli_bronze":
            sql = sql.replace("`meli_bronze.", f"`{self.bronze_dataset}.")
        if self.silver_dataset != "meli_silver":
            sql = sql.replace("`meli_silver.", f"`{self.silver_dataset}.")
        return sql

    def run_transform(
        self,
        table_name: str,
        execution_date: Optional[date] = None,
    ) -> Dict[str, object]:
        """Executa transformacao transacional idempotente para uma tabela Silver."""
        target_date = execution_date or datetime.now(timezone.utc).date()
        sql = self._load_sql(table_name)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("execution_date", "DATE", target_date),
            ],
            maximum_bytes_billed=self.max_bytes_billed,
            labels={"domain": "meli", "layer": "silver", "datacloud": "antigravity"},
        )

        logger.info(
            "Transformando %s.%s (particao=%s, max_bytes_billed=%d)...",
            self.silver_dataset,
            table_name,
            target_date.isoformat(),
            self.max_bytes_billed,
        )
        query_job = self.client.query(sql, job_config=job_config, location=self.location)
        query_job.result()

        bytes_billed = int(query_job.total_bytes_billed or 0)
        logger.info(
            "Tabela %s.%s atualizada (job_id=%s, bytes_billed=%d).",
            self.silver_dataset,
            table_name,
            query_job.job_id,
            bytes_billed,
        )
        return {
            "job_id": query_job.job_id,
            "bytes_billed": bytes_billed,
            "execution_date": target_date.isoformat(),
        }

    def run_all(
        self, execution_date: Optional[date] = None
    ) -> Dict[str, Dict[str, object]]:
        """Executa todas as transformacoes Silver para a mesma execution_date."""
        target_date = execution_date or datetime.now(timezone.utc).date()
        return {
            table_name: self.run_transform(table_name, execution_date=target_date)
            for table_name in SILVER_TABLES
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    transformer = SilverTransformer()
    results = transformer.run_all()
    total_mb = sum(int(v["bytes_billed"]) for v in results.values()) / (1024 * 1024)
    print(
        f"VALIDACAO E4-T02 OK | Projeto: {transformer.project_id} | "
        f"Total billed: {total_mb:.2f} MiB | Tabelas: {list(results.keys())}"
    )
