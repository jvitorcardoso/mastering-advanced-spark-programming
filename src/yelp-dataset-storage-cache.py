"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-dataset-storage-cache.py

TODO metrics:
Aggregated Spark stage metrics:
numStages => 12
numTasks => 347
elapsedTime => 376507 (6.3 min)
stageDuration => 372424 (6.2 min)
executorRunTime => 2102446 (35 min)
executorCpuTime => 1757143 (29 min)
executorDeserializeTime => 9151 (9 s)
executorDeserializeCpuTime => 7347 (7 s)
resultSerializationTime => 598 (0.6 s)
jvmGCTime => 32153 (32 s)
shuffleFetchWaitTime => 2902 (3 s)
shuffleWriteTime => 126240 (2.1 min)
resultSize => 1881323 (1837.2 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 66679004528
recordsRead => 834914005
bytesRead => 18851464946 (17.6 GB)
recordsWritten => 212416083
bytesWritten => 4756233397 (4.4 GB)
shuffleRecordsRead => 212416083
shuffleTotalBlocksFetched => 211
shuffleLocalBlocksFetched => 196
shuffleRemoteBlocksFetched => 15
shuffleTotalBytesRead => 23570036435 (22.0 GB)
shuffleLocalBytesRead => 22427517066 (20.9 GB)
shuffleRemoteBytesRead => 1142519369 (1089.6 MB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 23570036435 (22.0 GB)
shuffleRecordsWritten => 212416083

Average number of active tasks => 5.6

Stages and their duration:
Stage 0 duration => 853 (0.9 s)
Stage 1 duration => 564 (0.6 s)
Stage 2 duration => 704 (0.7 s)
Stage 3 duration => 428 (0.4 s)
Stage 4 duration => 22253 (22 s)
Stage 5 duration => 54 (54 ms)
Stage 6 duration => 51 (51 ms)
Stage 7 duration => 26046 (26 s)
Stage 8 duration => 20762 (21 s)
Stage 9 duration => 73548 (1.2 min)
Stage 11 duration => 226964 (3.8 min)
Stage 12 duration => 197 (0.2 s)
"""

from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName("yelp-dataset-storage-cache") \
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

# TODO file_loc_reviews = "s3a://owshq/yelp/reviews/*.parquet"
# TODO file_loc_business = "s3a://owshq/yelp/business/*.parquet"

file_loc_reviews = "./storage/yelp/review/*.parquet"
file_loc_business = "./storage/yelp/business/*.parquet"

df_reviews = spark.read.parquet(file_loc_reviews)
df_business = spark.read.parquet(file_loc_business)

# TODO filter state & join between reviews and business [business_id]
df_state_nyc = df_business.filter(df_business["city"] == "Las Vegas")
df_joined = df_reviews.join(df_state_nyc, df_reviews["business_id"] == df_reviews["business_id"])

# TODO measure time without cache
start_time = time.time()
df_joined.count()
first_execution_time = time.time() - start_time

# TODO cache dataset
df_joined.cache()

# TODO measure execution time with cache
start_time = time.time()
df_joined.count()
second_execution_time = time.time() - start_time

print(f"execution time without caching: {first_execution_time} seconds")
print(f"execution time with caching: {second_execution_time} seconds")

# TODO additional transformation
# TODO relation with [df_joined] dataset
avg_stars_by_city = df_joined.groupBy("city").avg("stars")
start_time = time.time()
avg_stars_by_city.show()
third_execution_time = time.time() - start_time

print(f"execution time for additional transformation with caching: {third_execution_time} seconds")

spark.stop()
