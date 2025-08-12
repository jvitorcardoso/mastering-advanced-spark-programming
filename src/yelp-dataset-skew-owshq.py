"""
Salt:
in cryptography, a salt is random data that is used as an additional input to a one-way function that hashes data,
a password or passphrase — Wikipedia

Trigger Job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/yelp-dataset-skew-owshq.py

Base SQL Query: 11 minutes
TODO execution plan = http://localhost:18080/history/app-20240703184019-0053/SQL/execution/?id=3

SELECT r.review_id, r.user_id, r.date, b.name, b.city
FROM reviews r
JOIN business b
ON r.business_id = b.business_id
WHERE r.date = '2024-07-02' AND b.city = 'Las Vegas'

Aggregated Spark stage metrics:
numStages => 12
numTasks => 497
elapsedTime => 672575 (11 min)
stageDuration => 1287060 (21 min)
executorRunTime => 3928570 (1.1 h)
executorCpuTime => 197547 (3.3 min)
executorDeserializeTime => 4081 (4 s)
executorDeserializeCpuTime => 2509 (3 s)
resultSerializationTime => 443 (0.4 s)
jvmGCTime => 4494 (4 s)
shuffleFetchWaitTime => 24 (24 ms)
shuffleWriteTime => 25152 (25 s)
resultSize => 687932 (671.8 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 23895726672
recordsRead => 245519239
bytesRead => 6270959383 (5.8 GB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 122466411
shuffleTotalBlocksFetched => 16081
shuffleLocalBlocksFetched => 5357
shuffleRemoteBlocksFetched => 10724
shuffleTotalBytesRead => 7947950532 (7.4 GB) [TODO watch this value]
shuffleLocalBytesRead => 2647960073 (2.5 GB) [TODO watch this value]
shuffleRemoteBytesRead => 5299990459 (4.9 GB) [TODO watch this value]
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 7947950532 (7.4 GB) [TODO watch this value]
shuffleRecordsWritten => 122466411

Average number of active tasks => 5.8

Stages and their duration:
Stage 0 duration => 6345 (6 s)
Stage 1 duration => 717 (0.7 s)
Stage 2 duration => 892 (0.9 s)
Stage 3 duration => 24943 (25 s)
Stage 5 duration => 132 (0.1 s)
Stage 6 duration => 627656 (10 min) [TODO watch this value]
Stage 7 duration => 617312 (10 min) [TODO watch this value]
Stage 10 duration => 451 (0.5 s)
Stage 13 duration => 1016 (1 s)
Stage 16 duration => 1105 (1 s)
Stage 19 duration => 3834 (4 s)
Stage 22 duration => 2657 (3 s)

Salting Key: 6.6 minutes
# TODO execution plan = http://localhost:18080/history/app-20240703194945-0061/jobs/

Aggregated Spark stage metrics:
numStages => 8
numTasks => 298
elapsedTime => 421205 (7.0 min)
stageDuration => 418463 (7.0 min)
executorRunTime => 2437772 (41 min)
executorCpuTime => 68960 (1.1 min)
executorDeserializeTime => 3351 (3 s)
executorDeserializeCpuTime => 2017 (2 s)
resultSerializationTime => 213 (0.2 s)
jvmGCTime => 1784 (2 s)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 357 (0.4 s)
resultSize => 287575 (280.8 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 3280304624
recordsRead => 270012505
bytesRead => 3355836600 (3.1 GB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 176
shuffleTotalBlocksFetched => 176
shuffleLocalBlocksFetched => 65
shuffleRemoteBlocksFetched => 111
shuffleTotalBytesRead => 11966 (11.7 KB) [TODO watch this value]
shuffleLocalBytesRead => 4405 (4.3 KB) [TODO watch this value]
shuffleRemoteBytesRead => 7561 (7.4 KB) [TODO watch this value]
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 11966 (11.7 KB) [TODO watch this value]
shuffleRecordsWritten => 176

Average number of active tasks => 5.8

Stages and their duration:
Stage 0 duration => 6236 (6 s)
Stage 1 duration => 687 (0.7 s)
Stage 2 duration => 658 (0.7 s)
Stage 3 duration => 24503 (25 s)
Stage 5 duration => 163 (0.2 s)
Stage 6 duration => 4214 (4 s) [TODO watch this value]
Stage 7 duration => 381890 (6.4 min) [TODO watch this value]
Stage 9 duration => 112 (0.1 s)
"""

from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics
from pyspark.sql.functions import col, lit, rand, concat

spark = SparkSession \
    .builder \
    .appName("yelp-dataset-skew-owshq") \
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

file_loc_reviews = "s3a://owshq/yelp/skewed_reviews/*.parquet"
df_reviews = spark.read.parquet(file_loc_reviews)

file_loc_business = "s3a://owshq/yelp/business/*.parquet"
df_business = spark.read.parquet(file_loc_business)

df_reviews.createOrReplaceTempView("reviews")
df_business.createOrReplaceTempView("business")

# TODO 2016-02-15 = 14.454 [1 min]
# TODO 2024-07-02 = 122.466.330 [13 min] = how to solve this issue?
spark.sql("""
    SELECT r.date,
        COUNT(*) AS qtd
    FROM reviews r
    WHERE r.date = '2024-07-02'
    GROUP BY r.date
    ORDER BY qtd DESC
""").show()

# TODO base sql query using skewed date [2024-07-02], time taken = 11 minutes

# TODO salting technique [400 sweet spot]
salt_factor = 400

df_reviews_salt = df_reviews.withColumn(
    "salt_date", concat(col("date"), lit("_"), (rand() * salt_factor).cast("int"))).withColumn(
    "salt_business_id", concat(col("business_id"), lit("_"), (rand() * salt_factor).cast("int"))
)
print(df_reviews_salt.limit(10).show())

df_business_salt = df_business.withColumn(
    "salt_business_id", concat(col("business_id"), lit("_"), (rand() * salt_factor).cast("int"))
)
print(df_business_salt.limit(10).show())

df_reviews_salt.createOrReplaceTempView("reviews")
df_business_salt.createOrReplaceTempView("business")

# TODO use salted tables
# TODO add .coalesce(200) before writing into storage
df_join_skew = spark.sql("""
    SELECT r.review_id, r.user_id, r.date, b.name, b.city
    FROM reviews r
    JOIN business b
    ON r.salt_business_id = b.salt_business_id
    WHERE r.salt_date = '2024-07-02%'
""")
print(f"[salt]: {df_join_skew.take(10)}")

stage_metrics.end()
stage_metrics.print_report()

metrics = stage_metrics.aggregate_stagemetrics()
print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

spark.stop()
