"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-repartition-coalesce.py

execution plan:
http://localhost:18080/history/app-20240702205729-0024/jobs/
http://localhost:18080/history/app-20240703140150-0036/jobs/

3 Executors with 2 Cores Each x 4 Times = 24 [Partition = Tasks]
"""

from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName("yelp-repartition-coalesce") \
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


def measure_time_and_partitions(df, action="count"):
    start_time = time.time()
    if action == "count":
        df.count()
    elif action == "show":
        df.show()
    end_time = time.time()
    num_partitions = df.rdd.getNumPartitions()
    return end_time - start_time, num_partitions


# TODO initial DataFrame
# TODO initial dataframe partitions: 16, time: 7.2576 seconds
initial_time, initial_partitions = measure_time_and_partitions(df_reviews, "count")
print(f"initial dataframe partitions: {initial_partitions}, time: {initial_time:.4f} seconds")

# TODO repartition DataFrame
# TODO 24 Partitions => repartitioned dataframe partitions: 24, time: 4.5003 seconds
df_repartitioned = df_reviews.repartition(24)
repartition_time, repartition_partitions = measure_time_and_partitions(df_repartitioned, "count")
print(f"repartitioned dataframe partitions: {repartition_partitions}, time: {repartition_time:.4f} seconds")

# TODO coalesce DataFrame
# TODO 24 Partitions => coalesced dataframe partitions: 16, time: 3.1158 seconds
# TODO 12 Partitions => coalesced dataframe partitions: 12, time: 4.5969 seconds
df_coalesced = df_reviews.coalesce(12)
coalesce_time, coalesce_partitions = measure_time_and_partitions(df_coalesced, "count")
print(f"coalesced dataframe partitions: {coalesce_partitions}, time: {coalesce_time:.4f} seconds")

spark.stop()
