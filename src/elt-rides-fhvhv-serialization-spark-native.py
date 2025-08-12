"""
Spark App: elt-rides-fhvhv-serialization-spark-native.py
Author: Luan Moreno

Trigger Job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-serialization-spark-native.py

TODO time taken [native spark udf]: 95.58636474609375 seconds
TODO http://localhost:18080/history/app-20240703205539-0005/jobs/

Aggregated Spark stage metrics:
numStages => 12
numTasks => 343
elapsedTime => 98195 (1.6 min)
stageDuration => 95374 (1.6 min)
executorRunTime => 507153 (8.5 min)
executorCpuTime => 453564 (7.6 min)
executorDeserializeTime => 5973 (6 s)
executorDeserializeCpuTime => 3571 (4 s)
resultSerializationTime => 227 (0.2 s)
jvmGCTime => 8927 (9 s) [TODO watch this value]
shuffleFetchWaitTime => 13 (13 ms)
shuffleWriteTime => 41218 (41 s)
resultSize => 1368771 (1336.7 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 34851591104
recordsRead => 834913802
bytesRead => 13603983058 (12.7 GB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 212416475
shuffleTotalBlocksFetched => 403
shuffleLocalBlocksFetched => 261
shuffleRemoteBlocksFetched => 142
shuffleTotalBytesRead => 7609678285 (7.1 GB) [TODO watch this value]
shuffleLocalBytesRead => 7511408235 (7.0 GB) [TODO watch this value]
shuffleRemoteBytesRead => 98270050 (93.7 MB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 7609678285 (7.1 GB) [TODO watch this value]
shuffleRecordsWritten => 212416475

Average number of active tasks => 5.2

Stages and their duration:
Stage 0 duration => 590 (0.6 s)
Stage 1 duration => 522 (0.5 s)
Stage 2 duration => 181 (0.2 s)
Stage 3 duration => 399 (0.4 s)
Stage 4 duration => 25568 (26 s) [TODO watch this value]
Stage 5 duration => 144 (0.1 s)
Stage 6 duration => 303 (0.3 s)
Stage 7 duration => 13780 (14 s)
Stage 8 duration => 12224 (12 s)
Stage 9 duration => 28509 (29 s)
Stage 11 duration => 13089 (13 s)
Stage 14 duration => 65 (65 ms)

TODO remove order by clause
http://localhost:18080/history/app-20240703210448-0007/jobs/
"""

import time
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics
from pyspark.sql.functions import col, when


# TODO this is a native spark function
def hvfhs_license_num(df):
    """
    Transform the hvfhs_license_num field based on the following logic:

    - HV0002: Juno
    - HV0003: Uber
    - HV0004: Via
    - HV0005: Lyft

    :param df: Input DataFrame with Hvfhs_license_num field
    :return: DataFrame with transformed Hvfhs_license_num field
    """

    transformed_df = df.withColumn("hvfhs_license_num",
         when(col("hvfhs_license_num") == "HV0002", "Juno")
        .when(col("hvfhs_license_num") == "HV0003", "Uber")
        .when(col("hvfhs_license_num") == "HV0004", "Via")
        .when(col("hvfhs_license_num") == "HV0005", "Lyft")
        .otherwise(col("hvfhs_license_num"))
    )

    return transformed_df


# TODO init spark session
spark = SparkSession \
    .builder \
    .appName("elt-rides-fhvhv-serialization-spark-native") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
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

file_fhvhv = "./storage/tlc/fhvhv/2022/*.parquet"
file_zones = "./storage/tlc/zones.csv"

# TODO file_fhvhv = "s3a://owshq/tlc/fhvhv/2022/*.parquet"
# TODO file_zones = "s3a://owshq/tlc/zones.csv"

df_fhvhv = spark.read.parquet(file_fhvhv)
df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)

# TODO: use native spark udf to transform license number.
start_time = time.time()

df_fhvhv = hvfhs_license_num(df_fhvhv)

df_fhvhv.createOrReplaceTempView("hvfhs")
df_zones.createOrReplaceTempView("zones")

# TODO: look at this order by? [DEMO: remove order clause]
df_rides = spark.sql("""
    SELECT hvfhs_license_num,
           zones_pu.Borough AS PU_Borough,
           zones_pu.Zone AS PU_Zone,
           zones_do.Borough AS DO_Borough,
           zones_do.Zone AS DO_Zone,
           request_datetime,
           pickup_datetime,
           dropoff_datetime,
           trip_miles,
           trip_time,
           base_passenger_fare,
           tolls,
           bcf,
           sales_tax,
           congestion_surcharge,
           tips,
           driver_pay,
           shared_request_flag,
           shared_match_flag
    FROM hvfhs
    INNER JOIN zones AS zones_pu
    ON CAST(hvfhs.PULocationID AS INT) = zones_pu.LocationID
    INNER JOIN zones AS zones_do
    ON hvfhs.DOLocationID = zones_do.LocationID
    ORDER BY request_datetime DESC
""")

df_rides.createOrReplaceTempView("rides")

df_hvfhs_license_num = spark.sql("""
    SELECT 
        hvfhs_license_num,
        SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
        SUM(trip_miles) AS total_trip_miles,
        SUM(trip_time) AS total_trip_time
    FROM 
        rides
    GROUP BY 
        hvfhs_license_num
    """)

df_rides.show()
df_hvfhs_license_num.show()

end_time = time.time()
print(f"time taken [native spark udf]: {end_time - start_time} seconds")

stage_metrics.end()
stage_metrics.print_report()

metrics = stage_metrics.aggregate_stagemetrics()
print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

spark.stop()
