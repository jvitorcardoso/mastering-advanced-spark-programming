"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/etl-dataset-spill.py

TODO execution plan: http://localhost:18080/history/app-20240702195034-0017/stages/stage/?id=13&attempt=0

TODO metrics spilled disk
Aggregated Spark stage metrics:
numStages => 12
numTasks => 179
elapsedTime => 1045081 (17 min)
stageDuration => 868406 (14 min)
executorRunTime => 4762135 (1.3 h)
executorCpuTime => 527143 (8.8 min)
executorDeserializeTime => 8301 (8 s)
executorDeserializeCpuTime => 4363 (4 s)
resultSerializationTime => 569 (0.6 s)
jvmGCTime => 14424 (14 s)
shuffleFetchWaitTime => 13 (13 ms)
shuffleWriteTime => 24736 (25 s)
resultSize => 232001 (226.6 KB)
diskBytesSpilled => 1704630027 (1625.7 MB) TODO watch this metric
memoryBytesSpilled => 11475610272 (10.7 GB) TODO watch this metric
peakExecutionMemory => 27283058032
recordsRead => 450000297
bytesRead => 0 (0 Bytes)
recordsWritten => 450000111
bytesWritten => 4411850127 (4.1 GB)
shuffleRecordsRead => 300000123
shuffleTotalBlocksFetched => 1414
shuffleLocalBlocksFetched => 454
shuffleRemoteBlocksFetched => 960
shuffleTotalBytesRead => 4026009692 (3.7 GB)
shuffleLocalBytesRead => 1214615777 (1158.3 MB)
shuffleRemoteBytesRead => 2811393915 (2.6 GB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 4026009692 (3.7 GB)
shuffleRecordsWritten => 300000123

Average number of active tasks => 4.6

Stages and their duration:
Stage 0 duration => 260913 (4.3 min)
Stage 1 duration => 5066 (5 s)
Stage 2 duration => 5807 (6 s)
Stage 3 duration => 12094 (12 s)
Stage 4 duration => 10418 (10 s)
Stage 5 duration => 10540 (11 s)
Stage 8 duration => 28620 (29 s)
Stage 13 duration => 262383 (4.4 min)
Stage 14 duration => 309 (0.3 s)
Stage 15 duration => 409 (0.4 s)
Stage 17 duration => 78 (78 ms)
Stage 18 duration => 271769 (4.5 min)
metrics elapsedTime = 1045081
"""

import time
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics, TaskMetrics

spark = SparkSession \
    .builder \
    .appName("etl-dataset-spill") \
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
# TODO task_metrics = TaskMetrics(spark)
stage_metrics.begin()
# TODO task_metrics.begin()

# TODO build transactions table
df_transactions = spark.range(0, 150_000_000, 1, 32) \
    .select('id',
            round(rand() * 10000, 2).alias('amount'),
            (col('id') % 10).alias('country_id'),
            (col('id') % 100).alias('store_id')
    )
df_transactions.write.format("parquet").mode("overwrite").save("s3a://owshq/transactions/")

# TODO build stores table
df_stores = spark.range(0, 99) \
    .select('id',
            round(rand() * 100, 0).alias('employees'),
            (col('id') % 10).alias('country_id'),
            expr('uuid()').alias('name')
    )
df_stores.write.format("parquet").mode("overwrite").save("s3a://owshq/stores/")

columns = ["id", "name"]
countries = [
     (0, "Italy"),
     (1, "Canada"),
     (2, "Mexico"),
     (3, "China"),
     (4, "Germany"),
     (5, "UK"),
     (6, "Japan"),
     (7, "Korea"),
     (8, "Australia"),
     (9, "France"),
     (10, "Spain"),
     (11, "USA")
]

# TODO build countries table
df_countries = spark.createDataFrame(data=countries, schema=columns)
df_countries.write.format("parquet").mode("overwrite").save("s3a://owshq/countries/")

# TODO explicitly turning off broadcast joins in order to demonstrate shuffle.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)

df_transactions.createOrReplaceTempView("transactions")
df_stores.createOrReplaceTempView("stores")
df_countries.createOrReplaceTempView("countries")

# TODO cache the views
spark.sql("CACHE TABLE transactions")
spark.sql("CACHE TABLE stores")
spark.sql("CACHE TABLE countries")

# TODO 1.4GB shuffle reads: one for each join
start_time = time.time()
joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM transactions
    LEFT JOIN stores
    ON transactions.store_id = stores.id
    LEFT JOIN countries
    ON transactions.country_id = countries.id
""")

# TODO spill might happen here
joined_df.write.format("parquet").mode("overwrite").save("s3a://owshq/transact_countries/")
end_time = time.time()
print(f"transact countries time: {end_time - start_time} seconds")

# TODO manually set shuffle partitions to 8
spark.conf.set("spark.sql.shuffle.partitions", 8)

start_time = time.time()
joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM transactions
    LEFT JOIN stores
    ON transactions.store_id = stores.id
    LEFT JOIN countries
    ON transactions.country_id = countries.id
""")

joined_df.write.format("parquet").mode("overwrite").save("s3a://owshq/transact_countries/")
end_time = time.time()
print(f"transact countries time [shuffle partitions to 8]: {end_time - start_time} seconds")

# TODO change the order of the join
# TODO can we see spill in this case?
start_time = time.time()
joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM transactions
    LEFT JOIN countries
    ON transactions.country_id = countries.id
    LEFT JOIN stores
    ON transactions.store_id = stores.id
""")

joined_df.write.format("parquet").mode("overwrite").save("s3a://owshq/transact_countries/")
end_time = time.time()
print(f"transact countries time [changing join order]: {end_time - start_time} seconds")

# TODO run ANALYZE on the joining columns and everything will just work
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

spark.sql("ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS country_id, store_id")
spark.sql("ANALYZE TABLE stores COMPUTE STATISTICS FOR COLUMNS id")
spark.sql("ANALYZE TABLE countries COMPUTE STATISTICS FOR COLUMNS id")

start_time = time.time()
joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM transactions
    LEFT JOIN countries
    ON transactions.country_id = countries.id
    LEFT JOIN stores
    ON transactions.store_id = stores.id
""")

joined_df.write.format("parquet").mode("overwrite").save("s3a://owshq/transact_countries/")
end_time = time.time()
print(f"transact countries time [with analyze]: {end_time - start_time} seconds")

stage_metrics.end()
# TODO task_metrics.end()
stage_metrics.print_report()

metrics = stage_metrics.aggregate_stagemetrics()
print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

spark.stop()
