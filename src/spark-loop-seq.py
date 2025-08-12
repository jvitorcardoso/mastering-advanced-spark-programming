"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/spark-loop-seq.py

http://localhost:18080/history/app-20240702175510-0013/jobs/
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession \
    .builder \
    .appName("spark-loop-seq") \
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

bucket_name = "landing/com.owshq.data/mongodb/"
json_prefix = "stripe/json/"

json_files = [
    f"s3a://{bucket_name}/{json_prefix}/2024_07_02_10_49_03.json",
    f"s3a://{bucket_name}/{json_prefix}/2024_07_02_10_55_32.json",
    f"s3a://{bucket_name}/{json_prefix}/2024_07_02_10_55_43.json"
]

stripe_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("dt_current_timestamp", StringType(), True)
])

stripe_data = spark.createDataFrame([], schema=stripe_schema)

for json_file in json_files:
    df_stripe = spark.read.schema(stripe_schema).json(json_file)
    stripe_data_df = stripe_data.union(df_stripe)

stripe_data_df.createOrReplaceTempView("stripe_loop")
spark.sql("""
    SELECT user_id, COUNT(user_id) AS total
    FROM stripe_loop
    GROUP BY user_id
""").show()

# TODO the parallelism mode used by Spark is the default mode.
df_stripe_py = spark.read.json(json_files)
df_stripe_py.createOrReplaceTempView("stripe_parallel")
spark.sql("""
    SELECT user_id, COUNT(user_id) AS total
    FROM stripe_parallel
    GROUP BY user_id
""").show()

spark.stop()
