"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-cache-persist-dataset.py

execution plan:
http://localhost:18080/history/app-20240702202407-0022/jobs/

without cache & persist:
http://localhost:18080/history/app-20240702202407-0022/SQL/execution/?id=0

with cache:
http://localhost:18080/history/app-20240702202407-0022/jobs/job/?id=7

with persist:
http://localhost:18080/history/app-20240702202407-0022/jobs/job/?id=14
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
import time

spark = SparkSession \
    .builder \
    .appName("yelp-cache-persist-dataset") \
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

file_loc_reviews = "s3a://owshq/yelp/reviews/*.parquet"
file_loc_business = "s3a://owshq/yelp/business/*.parquet"

df_reviews = spark.read.parquet(file_loc_reviews)
df_business = spark.read.parquet(file_loc_business)

df_joined = df_reviews.join(df_business, "business_id")
df_transformed = df_joined.withColumn("category_length", F.length("category"))


def measure_time(df: DataFrame, action: str):
    start_time = time.time()
    if action == "count":
        df.count()
    elif action == "show":
        df.show()
    end_time = time.time()
    return end_time - start_time


# TODO without cache
# TODO 69.1096842288971 seconds
no_cache_time = measure_time(df_transformed, "count")
print(f"execution time without cache & persist: {no_cache_time} seconds")

# TODO with cache
# TODO initial execution: 167.98957204818726 seconds
# TODO subsequent execution: 0.6153709888458252 seconds
df_cached = df_transformed.cache()
cache_time_first = measure_time(df_cached, "count")
cache_time_second = measure_time(df_cached, "count")
print(f"initial execution time with cache: {cache_time_first} seconds")
print(f"subsequent execution time with cache: {cache_time_second} seconds")

spark.catalog.clearCache()

# TODO use persist
# TODO initial execution: 180.57323217391968 seconds
# TODO subsequent execution: 0.9096167087554932 second
df_persisted = df_transformed.persist(StorageLevel.DISK_ONLY)
persist_time_first = measure_time(df_persisted, "count")
persist_time_second = measure_time(df_persisted, "count")
print(f"initial execution time with persist: {persist_time_first} seconds")
print(f"subsequent execution time with persist: {persist_time_second} seconds")

spark.stop()
