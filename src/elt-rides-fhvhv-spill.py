"""
Spark App: elt-rides-fhvhv-spill
Author: Luan Moreno

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-spill.py

TODO change configs
.config("spark.executor.memory", "1g") \
.config("spark.driver.memory", "1g") \
.config("spark.sql.shuffle.partitions", "200") \
.config("spark.memory.fraction", "0.4") \
.config("spark.memory.storageFraction", "0.2") \
.config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
"""

import time
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics

spark = SparkSession \
    .builder \
    .appName("elt-rides-fhvhv-spill") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.memory.fraction", "0.4") \
    .config("spark.memory.storageFraction", "0.2") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
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

# TODO file_fhvhv = "s3a://owshq/tlc/fhvhv/2022/*.parquet"
# TODO file_zones = "s3a://owshq/tlc/zones.csv"
# TODO file_rides = "s3a://owshq/rides"

file_fhvhv = "./storage/tlc/fhvhv/2022/*.parquet"
file_zones = "./storage/tlc/zones.csv"
file_rides = "./storage/rides/delta/rides"

df_fhvhv = spark.read.parquet(file_fhvhv)
df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)

df_fhvhv.createOrReplaceTempView("hvfhs")
df_zones.createOrReplaceTempView("zones")

# TODO join between fhvhv and zones
start_time = time.time()
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
""")

df_rides.explain(True)

end_time = time.time()
print(f"time taken [join]: {end_time - start_time} seconds")

# TODO write rides to delta [minio]
start_time = time.time()

df_rides.write.mode("overwrite").format("delta").save(file_rides)

end_time = time.time()
print(f"time taken [write]: {end_time - start_time} seconds")

stage_metrics.end()
stage_metrics.print_report()

metrics = stage_metrics.aggregate_stagemetrics()
print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

spark.stop()
