from google.cloud import bigquery
from google.cloud import storage
from prefect import flow
import os

os.environ["GOOGLE_CLOUD_PROJECT"] = 'dataengineer-zoomcamp'

@flow()
def create_bq_table():

    client = bigquery.Client()

    dataset_ref = bigquery.DatasetReference('dataengineer-zoomcamp', 'dezoomcamp')
    table_id = "earthquakes"



    table = bigquery.Table(dataset_ref.table(table_id))
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [
    'gs://prefect-de-zoomcamp-alex/*.csv.gz.parquet'
    ]


    table.external_data_configuration = external_config

    table = client.create_table(table)

    new_table_id = "earthquakes_partitioned"

    schema = [
        bigquery.SchemaField("day", "integer"),
        bigquery.SchemaField("month", "integer"),
        bigquery.SchemaField("year", "integer"),
        bigquery.SchemaField("id", "string"),
        bigquery.SchemaField("lon", "float"),
        bigquery.SchemaField("lat", "float"),
        bigquery.SchemaField("mag", "float"),
        bigquery.SchemaField("place", "string"),
        bigquery.SchemaField("time", "Timestamp"),
        bigquery.SchemaField("tsunami", "integer"),
        bigquery.SchemaField("type", "string"),
        bigquery.SchemaField("nst", "float"),
        bigquery.SchemaField("dmin", "float"),
        bigquery.SchemaField("rms", "float"),
        bigquery.SchemaField("gap", "float"),
        bigquery.SchemaField("magType", "string")
    ]


    new_table = bigquery.Table(dataset_ref.table(new_table_id),schema=schema)
    new_table.range_partitioning = bigquery.RangePartitioning(
        field="month",
        range_=bigquery.PartitionRange(start=1, end=12, interval=1),
    )
    new_table = client.create_table(new_table)

create_bq_table()