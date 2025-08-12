"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/spark-basic-aqe.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("spark-basic-aqe") \
    .config("spark.executor.memory", "3g") \
    .config("spark.sql.adaptive.enabled", True) \
    .config("spark.sql.adaptive.coalescePartitions.enabled", True) \
    .config("spark.sql.adaptive.skewJoin.enabled", True) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

file_fhvhv = "../storage/tlc/fhvhv/2022/*.parquet"
df_fhvhv = spark.read.parquet(file_fhvhv)

file_zones = "../storage/tlc/zones.csv"
df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)

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
""")

df_rides.show()
df_rides.explain(True)

spark.stop()
