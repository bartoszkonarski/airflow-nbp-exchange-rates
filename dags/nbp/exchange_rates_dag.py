from datetime import timedelta
import logging

import pandas as pd
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable

from storage_clients import LocalStorageClient

logger = logging.getLogger("airflow.task")


@dag(
    schedule="0 7 * * *",
    start_date=pendulum.datetime(
        2002, 1, 2, tz="Europe/Warsaw"
    ),  # NBP exchange rates data is available from 2002-01-02 onwards
    catchup=False,
    tags=["nbp"],
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
        max_retry_delay=timedelta(hours=1),
    )
    def download_rates_to_bronze(**kwargs) -> str:
        """Fetches XML exchange rates for a logical date from NBP API and saves them to the Bronze layer."""
        BRONZE_LOCATION = Variable.get(
            "BRONZE_LOCATION", default_var="/opt/airflow/data/bronze"
        )
        NBP_API_BASE_URL = Variable.get(
            "NBP_API_BASE_URL", default_var="https://api.nbp.pl/api"
        )

        ds = kwargs.get("ds")
        exchange_rates_url = (
            f"{NBP_API_BASE_URL}/exchangerates/tables/a/{ds}/?format=xml"
        )

        logger.info(f"Fetching exchange rates from NBP API: {exchange_rates_url}")
        response = requests.get(exchange_rates_url)
        if response.status_code == 404:
            raise AirflowSkipException(
                f"No exchange rates data available for date: {ds} (Rates are not published on weekends and bank holidays)"
            )

        response.raise_for_status()

        xml_file_path = (
            f"{BRONZE_LOCATION}/nbp/effective_date={ds}/exchange_rates_a.xml"
        )

        logger.info(f"Saving exchange rates data to Bronze layer at: {xml_file_path}")
        storage_client = LocalStorageClient()
        storage_client.save(response.content, xml_file_path)
        logger.info(
            f"Exchange rates data successfully saved to Bronze layer: {xml_file_path}"
        )

        return xml_file_path

    @task
    def cleanup_rates_to_silver(xml_file_path: str, **kwargs):
        """Processes the Bronze layer XML exchange rates data, performs necessary transformations,
        then saves the cleaned data to the Silver layer in Parquet format.
        """
        SILVER_LOCATION = Variable.get(
            "SILVER_LOCATION", default_var="/opt/airflow/data/silver"
        )

        storage_client = LocalStorageClient()

        logger.info(
            f"Loading raw exchange rates data from Bronze layer: {xml_file_path}"
        )
        xml_data = storage_client.load(xml_file_path)

        meta_df = pd.read_xml(xml_data, xpath=".//ExchangeRatesTable")
        try:
            effective_date = meta_df["EffectiveDate"][0]
            table_number = meta_df["No"][0]
        except KeyError as e:
            raise AirflowFailException(f"Missing expected metadata fields in XML: {e}")

        ds = kwargs.get("ds")
        if effective_date != ds:
            raise AirflowFailException(
                f"Effective date in bronze layer XML file ({effective_date}) does not match expected date ({ds})"
            )

        rates_df = pd.read_xml(xml_data, xpath=".//Rate")
        rates_df = rates_df.rename(
            columns={
                "Currency": "currency_name",
                "Code": "currency_code",
                "Mid": "exchange_rate_to_pln",
            }
        )
        rates_df["effective_date"] = effective_date
        rates_df["table_number"] = table_number

        silver_file_path = f"{SILVER_LOCATION}/nbp/exchange_rates_a/effective_date={effective_date}/exchange_rates.parquet"
        logger.info(
            f"Saving cleaned exchange rates data to Silver layer at: {silver_file_path}"
        )
        storage_client.save_df(rates_df, silver_file_path)

    bronze_layer = download_rates_to_bronze()
    cleanup_rates_to_silver(bronze_layer)


exchange_rates_dag()
