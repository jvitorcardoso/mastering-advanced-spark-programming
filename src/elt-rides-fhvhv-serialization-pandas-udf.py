"""
Spark App: elt-rides-fhvhv-serialization-pandas-udf.py
Author: Luan Moreno

Trigger Job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-serialization-pandas-udf.py

TODO time taken [pandas udf]: 196.12682008743286 seconds
TODO execution plan: http://localhost:18080/history/app-20240703205017-0004/jobs/

Aggregated Spark stage metrics:
numStages => 12
numTasks => 346
elapsedTime => 198532 (3.3 min)
stageDuration => 196347 (3.3 min)
executorRunTime => 1079947 (18 min)
executorCpuTime => 427743 (7.1 min)
executorDeserializeTime => 5648 (6 s)
executorDeserializeCpuTime => 3604 (4 s)
resultSerializationTime => 363 (0.4 s)
jvmGCTime => 25631 (26 s) TODO watch out for this
shuffleFetchWaitTime => 39 (39 ms)
shuffleWriteTime => 43281 (43 s)
resultSize => 1549681 (1513.4 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 35087258448
recordsRead => 834913802
bytesRead => 13602617551 (12.7 GB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 212416481
shuffleTotalBlocksFetched => 411
shuffleLocalBlocksFetched => 268
shuffleRemoteBlocksFetched => 143
shuffleTotalBytesRead => 7609576042 (7.1 GB)
shuffleLocalBytesRead => 7491434363 (7.0 GB)
shuffleRemoteBytesRead => 118141679 (112.7 MB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 7609576042 (7.1 GB)
shuffleRecordsWritten => 212416481

Average number of active tasks => 5.4

Stages and their duration:
Stage 0 duration => 818 (0.8 s)
Stage 1 duration => 311 (0.3 s)
Stage 2 duration => 747 (0.7 s)
Stage 3 duration => 752 (0.8 s)
Stage 4 duration => 51499 (51 s)
Stage 5 duration => 104 (0.1 s)
Stage 6 duration => 121 (0.1 s)
Stage 7 duration => 36764 (37 s)
Stage 8 duration => 38249 (38 s)
Stage 9 duration => 49990 (50 s)
Stage 11 duration => 16895 (17 s)
Stage 14 duration => 97 (97 ms)
"""

import time
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def license_num(num):
    """
    :param num: The license number of the ride-sharing service
    :return: The name of the ride-sharing service associated with the given license number
    """

    if num == 'HV0002':
        return 'Juno'
    elif num == 'HV0003':
        return 'Uber'
    elif num == 'HV0004':
        return 'Via'
    elif num == 'HV0005':
        return 'Lyft'
    else:
        return 'Unknown'


# TODO init spark session
spark = SparkSession \
    .builder \
    .appName("elt-rides-fhvhv-serialization-pandas-udf") \
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

# TODO use the [pandas udf] to serialize the license number
start_time = time.time()

udf_license_num = udf(license_num, StringType())
spark.udf.register("license_num", udf_license_num)
df_fhvhv = df_fhvhv.withColumn('hvfhs_license_num', udf_license_num(df_fhvhv['hvfhs_license_num']))

df_fhvhv.createOrReplaceTempView("hvfhs")
df_zones.createOrReplaceTempView("zones")

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
print(f"time taken [pandas udf]: {end_time - start_time} seconds")

stage_metrics.end()
stage_metrics.print_report()

metrics = stage_metrics.aggregate_stagemetrics()
print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

spark.stop()
