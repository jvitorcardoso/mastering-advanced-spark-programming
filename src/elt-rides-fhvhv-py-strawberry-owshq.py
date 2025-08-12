"""
PySpark: elt-rides-fhvhv-py-strawberry-owshq
Author: Luan Moreno

move files to minio:
mc cp --recursive src/storage/tlc/fhvhv/2022 do-nyc1-orn-polaris-dev/owshq/tlc/fhvhv

executing job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-py-strawberry-owshq.py

execution plan:

with pruning: http://localhost:18080/history/app-20240702191056-0012/SQL/execution/?id=1
with pruning & predicate pushdown: http://localhost:18080/history/app-20240702191056-0012/SQL/execution/?id=2
"""

import time
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("elt-rides-fhvhv-py-strawberry-owshq") \
    .config("spark.executor.memory", "3g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://138.197.224.4") \
    .config("spark.hadoop.fs.s3a.access.key", "data-lake") \
    .config("spark.hadoop.fs.s3a.secret.key", "12620ee6-2162-11ee-be56-0242ac120002") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.fast.upload", True) \
    .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
    .config("fs.s3a.connection.maximum", 100) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

file_fhvhv = "s3a://owshq/tlc/fhvhv/2022/*.parquet"
file_zones = "s3a://owshq/tlc/zones.csv"

# TODO [without] column pruning
# TODO count without pruning: 212.416.083
# TODO time taken without pruning: 17.39959144592285 seconds
start_time = time.time()
df_fhvhv = spark.read.parquet(file_fhvhv)
print(f"count without pruning: {df_fhvhv.count()}")
end_time = time.time()
print(f"time taken without pruning: {end_time - start_time} seconds")

# TODO with column pruning
# TODO count with pruning: 212.416.083
# TODO time taken with pruning: 13.631216764450073 seconds
start_time = time.time()
fhvhv_cols = [
    "hvfhs_license_num", "PULocationID", "DOLocationID",
    "request_datetime", "pickup_datetime", "dropoff_datetime",
    "trip_miles", "trip_time"
]
df_fhvhv_colum_pruning = spark.read.parquet(file_fhvhv).select(*fhvhv_cols)
print(f"count with pruning: {df_fhvhv_colum_pruning.count()}")
end_time = time.time()
print(f"time taken with pruning: {end_time - start_time} seconds")

# TODO with column pruning & predicate pushdown
# TODO count with pruning & predicate pushdown: 14.751.591
# TODO time taken with pruning & predicate pushdown: 74.45825958251953 seconds
start_time = time.time()
df_fhvhv_colum_pruning_pushdown = spark.read.parquet(file_fhvhv).select(*fhvhv_cols) \
    .filter((col("pickup_datetime") >= "2022-01-01") & (col("pickup_datetime") < "2022-02-01"))
df_fhvhv_colum_pruning_pushdown.show()
df_fhvhv_colum_pruning_pushdown.explain(True)
print(f"count with pruning & predicate pushdown: {df_fhvhv_colum_pruning_pushdown.count()}")
end_time = time.time()
print(f"time taken with pruning & predicate pushdown: {end_time - start_time} seconds")

spark.stop()
