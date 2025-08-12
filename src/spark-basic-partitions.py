"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/spark-basic-partitions.py

File Size:
- fhvhv_tripdata_2022-01.parquet 374.6 MB [6 Partitions by Default] = 62.4 MB

Spark Configuration:
- Executors = 3
- Executor Cores = 2
- Executor Memory = 3 GB
- Total Cores [Slots] = 6
- Total Memory = 9 GB

Partition:
- Desired Number of Partitions: 3-4 Times Number of CPU Cores
- Minimum = 6 Cores x 3 = 18 Partitions
- Maximum = 6 Cores x 4 = 24 Partitions

Total File Size [374.6 MB] / Desired Number of Partitions [24] = 15.6 MB

Use "spark.sql.files.maxPartitionBytes" when is suitable for all datasets being processed with same size [silver]
Use "repartition()" when you don't have control over the number of partitions in the dataset [size]

Bytes:
- 134217728 (128 MB)
- 67108864 (64 MB)
- 17825792 (17 MB)
- 4194304 (4 MB)
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("spark-basic-partitions") \
    .config("spark.executor.memory", "3g") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

file_fhvhv = "../storage/tlc/fhvhv/2022/fhvhv_tripdata_2022-01.parquet"
df_fhvhv = spark.read.parquet(file_fhvhv)
print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")

df_repart_fhvhv = df_fhvhv.repartition(24)
print(f"Number of partitions after repartitioning: {df_repart_fhvhv.rdd.getNumPartitions()}")

df_repart_fhvhv.show()

spark.stop()
