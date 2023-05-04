# Posts-questions and Posts-answers "schema"
from prefect import flow, task
from pyspark.sql.functions import col
from pyspark.sql import types, SparkSession
from pyspark import SparkConf, SparkContext
import sys
import os


gcs_bucket=os.getenv('GOOGLE_DATA_LAKE_BUCKET_NAME')
cread_path=os.getenv('CREDENTIAL_PATH')
allfiles = os.listdir('.')
files = [fname.replace('.csv.gz.parquet','') for fname in allfiles if fname.endswith('.csv.gz.parquet')]


def implement_schema(df):
        df2 = df.withColumn('id',col('id').cast(types.StringType())) \
                .withColumn('lon',col('lon').cast(types.FloatType())) \
                .withColumn('lat',col('lat').cast(types.FloatType())) \
                .withColumn('mag',col('mag').cast(types.FloatType())) \
                .withColumn('place',col('place').cast(types.StringType())) \
                .withColumn('time',col('time').cast(types.TimestampType())) \
                .withColumn('year',col('year').cast(types.IntegerType())) \
                .withColumn('month',col('month').cast(types.IntegerType())) \
                .withColumn('day',col('day').cast(types.IntegerType())) \
                .withColumn('tsunami',col('tsunami').cast(types.IntegerType())) \
                .withColumn('type',col('type').cast(types.StringType())) \
                .withColumn('nst',col('nst').cast(types.FloatType())) \
                .withColumn('dmin',col('dmin').cast(types.FloatType())) \
                .withColumn('rms',col('rms').cast(types.FloatType())) \
                .withColumn('gap',col('gap').cast(types.FloatType())) \
                .withColumn('magType',col('magType').cast(types.StringType()))



@flow()
def transform_parquet():
        allfiles = os.listdir('.')
        files = [fname.replace('.csv.gz.parquet', '') for fname in allfiles if fname.endswith('.csv.gz.parquet')]

        spark = SparkSession.builder \
                .master("local[*]") \
                .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
                .appName('GCSFilesRead') \
                .getOrCreate()

        spark._jsc.hadoopConfiguration() \
                .set("google.cloud.auth.service.account.json.keyfile",
                     cread_path)

        print(spark.version)
        print(len(files))

        for file in files:
                path = f"gs://{gcs_bucket}/{file}.csv.gz.parquet"

                save_path = f"gs://{gcs_bucket}/earthquakes_spark/{file}_parts.csv.gz.parquet"


                df = spark.read.parquet(path, header=True)

                implement_schema(df)

                df.registerTempTable('earthquakes')

                df_agg = spark.sql(
                        """SELECT place,month,count(*) from earthquakes
                           GROUP BY  place,month
                           order by 2 desc
                           """)


                df_agg.repartition(4).write.format("parquet").mode("overwrite").save(save_path)




transform_parquet()