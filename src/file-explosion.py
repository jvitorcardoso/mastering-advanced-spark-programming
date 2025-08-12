"""
writing time:
- df_iot_data.write.format("parquet").mode("overwrite").partitionBy("id").save("s3a://owshq/iot/id")
- df_iot_data.write.format("parquet").mode("overwrite").save("s3a://owshq/iot/")

statistics partitioned by id [101 files]:
mc ls do-nyc1-orn-polaris-dev/owshq/iot_id | wc -l

number of output rows: 100

http://localhost:18080/history/app-20240702162149-0027/SQL/execution/?id=0
task commit time total (min, med, max (stageId: taskId))
10.4 s (5.0 s, 5.4 s, 5.4 s (stage 0.0: task 0))
number of written files: 100
job commit time: 3.8 m
number of output rows: 100
number of dynamic part: 100
written output: 80.3 KiB

statistics without partitioning [33]:
mc ls do-nyc1-orn-polaris-dev/owshq/iot | wc -l

number of output rows: 50,000,000

http://localhost:18080/history/app-20240702162149-0027/SQL/execution/?id=4

task commit time total (min, med, max (stageId: taskId))
2.5 m (2.8 s, 4.3 s, 10.1 s (stage 6.0: task 23))
number of written files: 32
job commit time: 1.7 m
number of output rows: 50,000,000
number of dynamic part: 0
written output: 772.3 MiB

executing job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/file-explosion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("file-explosion") \
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

df_iot_data_id = spark.range(0, 100) \
    .select(
      hash('id').alias('id'),
      rand().alias('value'),
      from_unixtime(lit(1701692381 + col('id'))).alias('time')
    )

df_iot_data_id.write.format("parquet").mode("overwrite").partitionBy("id").save("s3a://owshq/iot_id")
df_iot_data_id.createOrReplaceTempView("iot_id")

spark.sql("""
    SELECT * 
    FROM iot_id 
    WHERE id = 519220707
""").show()

spark.sql("""
    SELECT avg(value) 
    FROM iot_id 
    WHERE time >= "2023-12-04 12:19:00" AND time <= "2023-12-04 13:01:20"
""").show()

df_iot_data = spark.range(0,50000000, 1, 32) \
    .select(
      hash('id').alias('id'),
      rand().alias('value'),
      from_unixtime(lit(1701692381 + col('id'))).alias('time')
    )

df_iot_data.write.format("parquet").mode("overwrite").save("s3a://owshq/iot/")
df_iot_data.createOrReplaceTempView("iot")

spark.sql("""
    SELECT * 
    FROM iot 
    WHERE id = 519220707
""").show()

spark.sql("""
    SELECT avg(value) 
    FROM iot 
    WHERE time >= "2023-12-04 12:19:00" AND time <= "2023-12-04 13:01:20"
""").show()

spark.stop()
