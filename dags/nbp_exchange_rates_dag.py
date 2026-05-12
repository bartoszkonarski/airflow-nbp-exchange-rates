from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable

from abc import ABC, abstractmethod
from datetime import timedelta
import logging
from pathlib import Path
import pandas as pd
import pendulum
import requests

class StorageClient(ABC):
    @abstractmethod
    def save(self, data: bytes, target: str):
        pass
    
    @abstractmethod
    def file_exists(self, target: str) -> bool:
        pass


class LocalStorageClient(StorageClient):
    def save(self, data: bytes, target: str):
        path = Path(target)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "wb") as f:
            f.write(data)

    def file_exists(self, target: str) -> bool:
        return Path(target).exists()

logger = logging.getLogger("airflow.task")

@dag(
    dag_id='exchange_rates_etl',
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2026, 5, 1, tz="Europe/Warsaw"),
    catchup=True,
    tags=["nbp"]
)
def nbp_exchange_rates_dag():
    @task(
        retries=3, 
        retry_delay=timedelta(minutes=15),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1)
    )
    def download_rates_to_bronze(ds):
        BRONZE_LOCATION = Variable.get("BRONZE_LOCATION", default_var='/opt/airflow/data/bronze')
        NBP_API_BASE_URL = Variable.get("NBP_API_BASE_URL", default_var="https://api.nbp.pl/api/exchangerates/tables/A")

        logger.info(f"Fetching exchange rates from NBP API: {NBP_API_BASE_URL}/{ds}")
        response = requests.get(f"{NBP_API_BASE_URL}/{ds}/?format=xml")
        if response.status_code == 404:
            raise AirflowSkipException(f"No exchange rates data available for date: {ds} (Rates are not published on weekends and bank holidays)")
        
        response.raise_for_status()

        xml_file_path = f"{BRONZE_LOCATION}/nbp/effective_date={ds}/exchange_rates_a.xml"

        logger.info(f"Saving exchange rates data to Bronze layer at: {xml_file_path}")
        storage_client = LocalStorageClient()
        storage_client.save(response.content, xml_file_path)
        logger.info(f"Exchange rates data successfully saved to Bronze layer: {xml_file_path}")

        return xml_file_path

    @task
    def cleanup_rates_to_silver(xml_file_path: str):
        SILVER_LOCATION = Variable.get("SILVER_LOCATION", default_var='/opt/airflow/data/silver')

        storage_client = LocalStorageClient()
        if not storage_client.file_exists(xml_file_path):
            raise AirflowFailException(f"Bronze layer file missing: {xml_file_path}")

    bronze_layer = download_rates_to_bronze()
    silver_layer = cleanup_rates_to_silver(bronze_layer)

    bronze_layer >> silver_layer

nbp_exchange_rates_dag()