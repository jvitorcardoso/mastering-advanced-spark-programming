"""
Spark App: yelp-dataset-data-gen-skew
Author: Luan Moreno

Generate skewed data for the Yelp dataset.

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/gen/yelp-dataset-data-gen-skew.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("yelp-dataset-data-gen-skew") \
    .config("spark.executor.memory", "3g") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
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
df_reviews = spark.read.parquet(file_loc_reviews)

today = current_date()
skew_multiplier = 5

# TODO today is 02/07/2024
df_reviews_skew = df_reviews.withColumn("date", today)

df_reviews_current_date = df_reviews_skew
for _ in range(skew_multiplier - 1):
    df_reviews_current_date = df_reviews_current_date.union(df_reviews_skew)

df_reviews_last = df_reviews.union(df_reviews_current_date)

output_path = "s3a://owshq/yelp/skewed_reviews"
df_reviews_last.write.mode("overwrite").parquet(output_path)

df_reviews_last.count()

spark.stop()
