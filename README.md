# DEZoomCampProject

# EARTHQUAKE_ANALYSIS_PIPELINE
A data pipeline to ingest earthqake datasets from https://earthquake.usgs.gov/ API then perfrom some data agregation using Pyspark, load data to BigQuery DWH and make Dashboard using Google Looker Studio.
ta cleaning and preprocessing and lastly perform sentiment analysis to build a general understanding of the sentiment of the tweets. 

## Project Description

This is mainly educational project which is aimed to show some skills in such data pipline tools as Prefect,Google Cloud Storage and Google Big Query, I was focued on the scope of data engineering (building the data pipeline) and data analytics (building a BI dashboard).I used Terraform as an infrastructer tool, Prefect to maintain data flows , Apache spark for data transformation and Google Looker Studio for creating Dashboard.


## Objective
  * Build data pipline for Earthquake data analysis.
  * Build a dashboard to visualize some data insights.

## Dataset
The dataset is available on [earthquake.usgs.gov] (https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2023-03-01&endtime=2023-03-30)


## Tools & Technology
* Terraform (IaC)
* Cloud: Google Cloud Platform (GCP)
  * Data Lake: Google Cloud Storage (GCS)
  * DWH: Google Big Query (GBQ)
  * Data Visualization: Google Looker Studio (GDS)
* Orchestration: Prefect
* Data Transformation: Apache Spark (Pyspark)
* Scripting Language: Python

## Data Pipeline
I have created Prefect pipline that consisted of two flows etl_web_to_gcs and transform_parquet. The first one is downloading data using public API and ingesting it to Google cloud storage , the second run Spark jobs that aggregate data and store it in buckets.


 * ingest_data:
    * download data from public API and store it in GCS bucket.
    * read the data from the GCS bucket and ingest it into a Spark dataframe.
    * perform some data aggregation.
    * upload the datafram data lake, google cloud storage (GCS) in parquet format.
 * create_bigquery_table:
    * gcs_bigquery: create the table in data warehouse, google big query (GBQ) by the parquet files in datalake.
 * create_insights:
    * read the data from the bigquery table and make a dashboard using Google Looker Studio
 
<img alt = "image" src = "https://i.ibb.co/zswN4FF/tweets-dag.png">

## Data Visualization
I created a visualization dashboard  showing the end result of the project. <br>
Dashboard: [url](https://lookerstudio.google.com/reporting/039a6daa-f3e6-4b56-b9d2-a77a16f50de4/page/OF3OD)

<img alt = "image" src = "https://i.ibb.co/4McMtSg/tweets-setiment-dashboard.png">

## Reproductivity

# Step 1: Setup the Base Environment <br>
Required Services: <br>
* [Google Cloud Platform](https://console.cloud.google.com/) - register an account with credit card GCP will free us $300 for 3 months
    * Create a service account & download the keys as json file. The json file will be useful for further steps.
    * Enable the API related to the services (Google Compute Engine, Google Cloud Storage, Cloud Composer, Cloud Dataproc & Google Big Query)
    * follow the Local Setup for Terraform and GCP [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp)

# Step 2: Provision your cloud infrastructure useing Terraform<br>
* copy your GCP project name into "project" var in variable.tf.
* run the following commands:
```console
terraform init
```
```console
terraform apply -auto-approve
```
> 
