import logging
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv
from google.cloud import bigquery

from src.meli_client import MeliClient

logger = logging.getLogger(__name__)

BRONZE_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("execution_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("endpoint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("payload_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("raw_payload", "JSON", mode="REQUIRED"),
]


class BronzeLoader:
    """Carrega snapshots brutos em particoes BigQuery via Batch Load Jobs ($0)."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        location: Optional[str] = None,
        client: Optional[bigquery.Client] = None,
    ) -> None:
        load_dotenv()
        resolved_project = project_id or os.getenv("GCP_PROJECT_ID") or None
        self.location = location or os.getenv("BQ_LOCATION", "southamerica-east1")
        self.dataset_id = dataset_id or os.getenv("BQ_DATASET_BRONZE", "meli_bronze")
        self.client = (
            client
            if client is not None
            else bigquery.Client(project=resolved_project, location=self.location)
        )
        self.project_id = self.client.project

    @staticmethod
    def prepare_rows(
        records: Sequence[Dict[str, Any]], execution_date: date
    ) -> List[Dict[str, Any]]:
        """Anexa execution_date sem mutar os dicionarios originais do extrator."""
        exec_date_str = execution_date.isoformat()
        return [
            {
                "execution_date": exec_date_str,
                "endpoint": record["endpoint"],
                "category_id": record.get("category_id"),
                "ingested_at": record["ingested_at"],
                "payload_hash": record["payload_hash"],
                "raw_payload": record["raw_payload"],
            }
            for record in records
        ]

    def load_partition(
        self,
        table_name: str,
        records: Sequence[Dict[str, Any]],
        execution_date: Optional[date] = None,
    ) -> int:
        """Sobrescreve atomicamente a particao $YYYYMMDD via Load Job gratuito."""
        if not records:
            logger.warning("Nenhum registro para carregar em %s.%s.", self.dataset_id, table_name)
            return 0

        target_date = execution_date or datetime.now(timezone.utc).date()
        partition_id = target_date.strftime("%Y%m%d")
        destination = f"{self.project_id}.{self.dataset_id}.{table_name}${partition_id}"
        rows = self.prepare_rows(records, target_date)

        job_config = bigquery.LoadJobConfig(
            schema=BRONZE_SCHEMA,
            autodetect=False,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            labels={"domain": "meli", "layer": "bronze", "datacloud": "antigravity"},
        )

        logger.info("Iniciando Batch Load (%d linhas) -> %s", len(rows), destination)
        load_job = self.client.load_table_from_json(rows, destination, job_config=job_config)
        load_job.result()

        loaded = int(load_job.output_rows or len(rows))
        logger.info("Particao %s carregada com sucesso (%d linhas).", destination, loaded)
        return loaded

    def load_all(
        self,
        category_records: Sequence[Dict[str, Any]],
        trend_records: Sequence[Dict[str, Any]],
        item_records: Sequence[Dict[str, Any]],
        payment_records: Sequence[Dict[str, Any]] = (),
        execution_date: Optional[date] = None,
    ) -> Dict[str, int]:
        """Carrega as tabelas Bronze para a mesma execution_date."""
        target_date = execution_date or datetime.now(timezone.utc).date()
        batches = {
            "raw_categories": category_records,
            "raw_trends": trend_records,
            "raw_items_search": item_records,
            "raw_payment_methods": payment_records,
        }
        return {
            table_name: self.load_partition(table_name, rows, execution_date=target_date)
            for table_name, rows in batches.items()
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    with MeliClient() as meli:
        categories = meli.fetch_categories()
        trends = meli.fetch_trends_and_domains()
        items_or_niches = meli.fetch_items_or_niches_snapshot(categories, trends)
        payments = meli.fetch_payment_methods()

    loader = BronzeLoader()
    counts = loader.load_all(categories, trends, items_or_niches, payment_records=payments)
    print(f"VALIDACAO E6-T03 OK | Projeto: {loader.project_id} | Linhas carregadas: {counts}")
