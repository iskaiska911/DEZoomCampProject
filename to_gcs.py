from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import requests
import json
import os
from pyspark_scripts import transform_parquet

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket





os.environ["GOOGLE_CLOUD_PROJECT"] = 'dataengineer-zoomcamp'

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into Pandas dataframe"""
    dataset=requests.get(dataset_url)
    data = json.loads(dataset.text)
    df= pd.json_normalize(data, record_path=['features'])

    return df

@task(log_prints=True,retries=3)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df[['lon', 'lat', 'some_coordinate']] = pd.DataFrame(df['geometry.coordinates'].tolist(),index=df.index)
    df['time'] = pd.to_datetime(df['properties.time'].astype(str), unit='ms')
    df['updated'] = pd.to_datetime(df['properties.updated'].astype(str), unit='ms')
    df['year'] = pd.DatetimeIndex(df['time']).year
    df['month'] = pd.DatetimeIndex(df['time']).month
    df['day'] = pd.DatetimeIndex(df['time']).day
    df['properties.magType'] = df['properties.magType'].astype(str)
    df['properties.type'] = df['properties.type'].astype(str)

    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    result_df = df[
        ['id','lon', 'lat', 'properties.mag', 'properties.place', 'time', 'year', 'month', 'day',
         'properties.tsunami', 'properties.type', 'properties.nst', 'properties.dmin', 'properties.rms',
         'properties.gap', 'properties.magType']]

    result_df.rename(columns={'properties.mag': 'mag', 'properties.place': 'place','properties.tsunami':'tsunami',
                              'properties.type':'type','properties.nst':'nst','properties.dmin':'dmin',
                              'properties.rms':'rms','properties.gap':'gap','properties.magType':'magType'
                             }, inplace=True)

    return result_df

@task(retries=3)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(retries=3)
def write_gcs(path:Path):
    gcs_block=GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=f"{path}",to_path=path)
    return




@flow(name="From API to GCP")
def etl_web_to_gcs() -> None:
    """This is the main ETL function"""
    range_dates=[["2022-01-01","2022-01-31"],
     ["2022-02-01", "2022-02-28"],
     ["2022-03-01", "2022-03-31"],
     ["2022-04-01", "2022-04-30"],
     ["2022-05-01", "2022-05-31"],
     ["2022-06-01", "2022-06-30"],
     ["2022-07-01", "2022-07-30"],
     ["2022-08-01", "2022-08-31"],
     ["2022-09-01", "2022-09-30"],
     ["2022-10-01", "2022-10-31"],
     ["2022-11-01", "2022-11-30"],
     ["2022-12-01", "2022-12-31"]]



    for month in range_dates:
        dataset_file=f"earthquakes_{month[1]}.csv.gz"
        dataset_url=f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={month[0]}&endtime={month[1]}"

        df = fetch(dataset_url)
        path = write_local(clean(df), dataset_file)
        write_gcs(path)




if __name__ == '__main__':
    etl_web_to_gcs()
    transform_parquet()




