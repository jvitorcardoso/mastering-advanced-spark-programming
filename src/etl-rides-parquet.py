"""
input size:
- dfs.blocksize (default is 128MB).
- splittability depends on the format of the file [parquet].
- spark.hadoop.fs.s3a.multipart.size setting (104857600 bytes, or 100MB).

partition size:
- bundling files into fewer partitions to reduce overhead
- aligns with the default parallelism heuristic (2 * number of cores) [6 cores].

ideal partition size:
- 6 [cores] x 4 [times] = 24 partitions.

minio [s3]: 188
- mc ls do-nyc1-orn-polaris-dev/landing/com.owshq.data/mongodb/rides/parquet/ | wc -l

statistics:
Scan parquet [47 s]
number of files read: 188
scan time total (min, med, max (stageId: taskId))
2.9 m (25.7 s, 29.4 s, 30.6 s (stage 2.0: task 189))
metadata time: 3 ms
size of files read: 3.5 MiB
number of output rows: 18,800

executing job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/etl-rides-parquet.py

Bytes:
- 134217728 (128 MB)
- 67108864 (64 MB)
- 17825792 (17 MB)
- 4194304 (4 MB)

size of files read: 6.4 MiB [json]
size of files read: 3.5 MiB [parquet] = percentage of reduction: 45.31%
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("etl-rides-parquet") \
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

file_rides_loc = "s3a://landing/com.owshq.data/mongodb/rides/parquet/*.parquet"
df_rides = spark.read.parquet(file_rides_loc)

print(f"number of partitions: {df_rides.rdd.getNumPartitions()}")

df_rides.count()

spark.stop()
