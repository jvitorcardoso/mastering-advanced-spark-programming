"""
Spark App: yelp-dataset-shuffle
Author: Luan Moreno
Reference: https://medium.com/swlh/revealing-apache-spark-shuffling-magic-b2c304306142

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-dataset-shuffle.py

TODO without changes [time taken: 2.6 min]
TODO execution plan = http://localhost:18080/history/app-20240703181449-0048/jobs/
df_join_reviews_business = df_reviews.alias("r").join(df_business.alias("b"), col("r.business_id") == col("b.business_id"))

Aggregated Spark stage metrics:
numStages => 5
numTasks => 25
elapsedTime => 146000 (2.4 min)
stageDuration => 277062 (4.6 min)
executorRunTime => 779212 (13 min)
executorCpuTime => 32370 (32 s)
executorDeserializeTime => 1491 (1 s)
executorDeserializeCpuTime => 895 (0.9 s)
resultSerializationTime => 10 (10 ms)
jvmGCTime => 768 (0.8 s)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 2905 (3 s)
resultSize => 36796 (35.9 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 201850800
recordsRead => 25079845
bytesRead => 1378434921 (1314.6 MB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 771843
shuffleTotalBlocksFetched => 19
shuffleLocalBlocksFetched => 6
shuffleRemoteBlocksFetched => 13
shuffleTotalBytesRead => 63902406 (60.9 MB)
shuffleLocalBytesRead => 22452142 (21.4 MB)
shuffleRemoteBytesRead => 41450264 (39.5 MB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 2085002191 (1988.4 MB) TODO watch this value
shuffleRecordsWritten => 25079845

Average number of active tasks => 5.3

Stages and their duration:
Stage 0 duration => 1516 (2 s)
Stage 1 duration => 1579 (2 s)
Stage 2 duration => 132909 (2.2 min)
Stage 3 duration => 140458 (2.3 min)
Stage 6 duration => 600 (0.6 s)

TODO Change Request [1]: Working on the config settings [time taken: 2.4 min]
TODO execution plan = http://localhost:18080/history/app-20240703181952-0049/jobs/
"spark.sql.shuffle.partitions" = this parameter controls the number of partitions to use when shuffling data for joins or aggregations.
"spark.shuffle.file.buffer" = increasing this value can improve shuffle write performance by reducing the number of I/O operations [buffer].

.config("spark.sql.shuffle.partitions", "6") \
.config("spark.shuffle.file.buffer", "1000") \

Aggregated Spark stage metrics:
numStages => 5
numTasks => 25
elapsedTime => 138077 (2.3 min)
stageDuration => 266259 (4.4 min)
executorRunTime => 755112 (13 min)
executorCpuTime => 32376 (32 s)
executorDeserializeTime => 1715 (2 s)
executorDeserializeCpuTime => 977 (1.0 s)
resultSerializationTime => 7 (7 ms)
jvmGCTime => 639 (0.6 s)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 2152 (2 s)
resultSize => 33558 (32.8 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 843054768
recordsRead => 25079845
bytesRead => 1378434921 (1314.6 MB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 4103378
shuffleTotalBlocksFetched => 17
shuffleLocalBlocksFetched => 8
shuffleRemoteBlocksFetched => 9
shuffleTotalBytesRead => 334618969 (319.1 MB)
shuffleLocalBytesRead => 159932326 (152.5 MB)
shuffleRemoteBytesRead => 174686643 (166.6 MB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 2042223398 (1947.6 MB)
shuffleRecordsWritten => 25079845

Average number of active tasks => 5.5

Stages and their duration:
Stage 0 duration => 1588 (2 s)
Stage 1 duration => 545 (0.5 s)
Stage 2 duration => 130073 (2.2 min)
Stage 3 duration => 131878 (2.2 min)
Stage 6 duration => 2175 (2 s)

TODO Change Request [2]: Adding [bhj] as hint on join between reviews and business datasets [time taken: 35 sec]
TODO execution plan = http://localhost:18080/history/app-20240703182401-0050/jobs/
df_join_reviews_business = df_reviews.alias("r").join(broadcast(df_business.alias("b")), col("r.business_id") == col("b.business_id"))

Aggregated Spark stage metrics:
numStages => 4
numTasks => 9
elapsedTime => 26236 (26 s)
stageDuration => 23791 (24 s)
executorRunTime => 29323 (29 s)
executorCpuTime => 3899 (4 s)
executorDeserializeTime => 1337 (1 s)
executorDeserializeCpuTime => 753 (0.8 s)
resultSerializationTime => 44 (44 ms)
jvmGCTime => 171 (0.2 s)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 0 (0 ms)
resultSize => 67862530 (64.7 MB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 176160688
recordsRead => 590675
bytesRead => 132175477 (126.1 MB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 0
shuffleTotalBlocksFetched => 0
shuffleLocalBlocksFetched => 0
shuffleRemoteBlocksFetched => 0
shuffleTotalBytesRead => 0 (0 Bytes)
shuffleLocalBytesRead => 0 (0 Bytes)
shuffleRemoteBytesRead => 0 (0 Bytes)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes) [TODO watch this value]
shuffleBytesWritten => 0 (0 Bytes)
shuffleRecordsWritten => 0

Average number of active tasks => 1.1

Stages and their duration:
Stage 0 duration => 1703 (2 s)
Stage 1 duration => 1482 (1 s)
Stage 2 duration => 8172 (8 s)
Stage 3 duration => 12434 (12 s)
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from sparkmeasure import StageMetrics

spark = SparkSession \
    .builder \
    .appName("yelp-dataset-shuffle") \
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

# TODO spark measure stage metrics
stage_metrics = StageMetrics(spark)
stage_metrics.begin()

file_loc_reviews = "s3a://owshq/yelp/reviews/*.parquet"
file_loc_business = "s3a://owshq/yelp/business/*.parquet"

df_reviews = spark.read.parquet(file_loc_reviews)
df_business = spark.read.parquet(file_loc_business)

# TODO join between reviews and business [business_id]
start_time = time.time()

df_join_reviews_business = df_reviews.alias("r").join(df_business.alias("b"), col("r.business_id") == col("b.business_id"))
df_join_reviews_business.show()
df_join_reviews_business.explain(True)

end_time = time.time()
print(f"time taken: {end_time - start_time} seconds")

stage_metrics.end()
stage_metrics.print_report()

metrics = stage_metrics.aggregate_stagemetrics()
print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

spark.stop()

# TODO compare execution plans
# TODO 2.4 min [without changes] = http://localhost:18080/history/app-20240703181449-0048/SQL/execution/?id=0
# TODO 2.2 min [change request 1] = http://localhost:18080/history/app-20240703181952-0049/SQL/execution/?id=0
# TODO 22 sec [change request 2] = http://localhost:18080/history/app-20240703182401-0050/SQL/execution/?id=0
