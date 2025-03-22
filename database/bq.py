import logging
import os
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BigQueryManager:
    def __init__(self, dataset_name: str = "tw_stock", table_name: str = "daily_stock_price"):
        self.project_id = os.getenv("PROJECT_ID", "kdan-data-engineer-test")
        self.dataset_id = f"{self.project_id}.{dataset_name}"
        self.table_id = f"{self.dataset_id}.{table_name}"

        json_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        try:
            if json_key_path and os.path.exists(json_key_path):
                logger.info("Using local service account credentials...")
                credentials = service_account.Credentials.from_service_account_file(json_key_path)
                self.client = bigquery.Client(credentials=credentials, project=self.project_id)
            else:
                logger.info("Using default GCP environment credentials...")
                self.client = bigquery.Client(project=self.project_id)
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def check_if_dataset_exists(self):
        try:
            self.client.get_dataset(self.dataset_id)
            logger.info(f"Dataset '{self.dataset_id}' already exists.")
        except Exception:
            dataset = bigquery.Dataset(self.dataset_id)
            dataset.location =  os.getenv("REGION", "asia-east1")
            self.client.create_dataset(dataset)
            logger.info(f"Created dataset '{self.dataset_id}'")

    def check_if_table_exists(self):
        try:
            self.client.get_table(self.table_id)
            logger.info(f"Table '{self.table_id}' already exists.")
        except Exception:
            schema = [
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("stock_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("close_price", "FLOAT"),
                bigquery.SchemaField("source", "STRING"),
            ]
            table = bigquery.Table(self.table_id, schema=schema)
            self.client.create_table(table)
            logger.info(f"Created table '{self.table_id}'")

        return self.client, self.table_id
    
    def insert_if_not_exists(self, records):
        """
        Insert only if no new data exists with the same date, stock_id and source.
        """
        if not records:
            logger.info("No data to insert.")
            return
        try:
            values_sql = ",\n".join([
                f"STRUCT(DATE('{r['date']}') AS date, '{r['stock_id']}' AS stock_id, {r['close_price']} AS close_price, '{r['source']}' AS source)"
                for r in records
            ])


            insert_sql = f"""
            INSERT INTO `{self.table_id}` (date, stock_id, close_price, source)
            SELECT * FROM UNNEST([
            {values_sql}
            ]) AS new_data
            WHERE NOT EXISTS (
            SELECT 1 FROM `{self.table_id}` AS existing
            WHERE existing.date = CAST(new_data.date AS DATE)
                AND existing.stock_id = new_data.stock_id
                AND existing.source = new_data.source
            )
            """

            self.client.query(insert_sql).result()
            logger.info("Insert completed successfully. Only new records were inserted.")
        except Exception as e:
            logger.error(f"BigQuery insert failed: {e}")
            raise
