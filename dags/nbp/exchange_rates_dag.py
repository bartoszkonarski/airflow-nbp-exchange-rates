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
from typing import List, Optional

class StorageClient(ABC):
    @abstractmethod
    def save(self, data: bytes, file_path: str):
        """Saves raw bytes data to the specified path."""
        pass
    
    @abstractmethod
    def load(self, file_path: str) -> bytes:
        """Loads raw bytes data from the specified file path."""
        pass

    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """Checks if a file exists at specified path."""
        pass

    @abstractmethod
    def save_df(self, df: pd.DataFrame, file_path: str, partition_cols: Optional[List[str]] = None, **kwargs):
        """
        Saves a pandas DataFrame to the specified path based on file extension.
        Supports partitioning for Parquet files via pyarrow.
        """
        pass


class LocalStorageClient(StorageClient):
    def save(self, data: bytes, file_path: str):
        """Saves raw bytes data to the specified path."""
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "wb") as f:
            f.write(data)

    def load(self, file_path: str) -> bytes:
        """Loads raw bytes data from the specified file path."""
        if not self.file_exists(file_path):
            raise AirflowFailException(f"Specified file: {file_path} does not exist")
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
        

    def file_exists(self, file_path: str) -> bool:
        """Checks if a file exists at the given path."""
        return Path(file_path).exists()
    
    def save_df(self, df: pd.DataFrame, file_path: str, partition_cols: Optional[List[str]] = None, **kwargs):
        """
        Saves a pandas DataFrame to the specified path based on file extension.
        Supports partitioning for Parquet files via pyarrow.
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        extension = path.suffix.lower()

        if extension == ".parquet":
            if partition_cols:
                df.to_parquet(
                    file_path,
                    engine="pyarrow",
                    index=False,
                    partition_cols=partition_cols,
                    existing_data_behavior="overwrite_or_ignore", 
                    **kwargs
                )
            else:
                df.to_parquet(file_path, engine="pyarrow", index=False, **kwargs)
        else: # More file formats handling can be added here in the future if needed
            raise ValueError(f"Unsupported file extension: {extension}")

logger = logging.getLogger("airflow.task")

@dag(
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 5, 1, tz="Europe/Warsaw"),
    catchup=True,
    tags=["nbp"]
)
def exchange_rates_dag():
    """### NBP Exchange Rates ETL DAG

    This process fetches daily exchange rates from the National Bank of Poland
    (NBP) public API, stores the raw XML payload into the Bronze storage layer, then
    process the data and saves it to the Silver layer parquet files.
    """
    @task(
        retries=3, 
        retry_delay=timedelta(minutes=15),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1)
    )
    def download_rates_to_bronze(ds: str) -> str:
        """Fetches XML exchange rates for a logical date from NBP API and saves them to the Bronze layer.

        :param ds: The logical date provided by Airflow (YYYY-MM-DD string).
        :return: The generated file path where the raw XML data was written.
        """
        BRONZE_LOCATION = Variable.get("BRONZE_LOCATION", default_var='/opt/airflow/data/bronze')
        NBP_API_BASE_URL = Variable.get("NBP_API_BASE_URL", default_var="https://api.nbp.pl/api")

        exchange_rates_url = f"{NBP_API_BASE_URL}/exchangerates/tables/a/{ds}/?format=xml"

        logger.info(f"Fetching exchange rates from NBP API: {exchange_rates_url}")
        response = requests.get(exchange_rates_url)
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
    def cleanup_rates_to_silver(xml_file_path: str, ds: str):
        SILVER_LOCATION = Variable.get("SILVER_LOCATION", default_var='/opt/airflow/data/silver')

        storage_client = LocalStorageClient()
        
        logger.info(f"Loading raw exchange rates data from Bronze layer: {xml_file_path}")
        xml_data = storage_client.load(xml_file_path)

        meta_df = pd.read_xml(xml_data, xpath=".//ExchangeRatesTable")
        try:
            effective_date = meta_df['EffectiveDate'][0]
            table_number = meta_df['No'][0]
        except KeyError as e:
            raise AirflowFailException(f"Missing expected metadata fields in XML: {e}")

        if effective_date != ds:
            raise AirflowFailException(f"Effective date in bronze layer XML file ({effective_date}) does not match expected date ({ds})")
        
        rates_df = pd.read_xml(xml_data, xpath=".//Rate")
        rates_df = rates_df.rename(columns={'Currency': 'currency_name', 'Code': 'currency_code', 'Mid': 'exchange_rate_to_pln'})
        rates_df['effective_date'] = effective_date
        rates_df['table_number'] = table_number

    bronze_layer = download_rates_to_bronze()
    silver_layer = cleanup_rates_to_silver(bronze_layer)

    bronze_layer >> silver_layer

exchange_rates_dag()