"""
SortMergeJoin [smj]

2.4 minutes
http://localhost:18080/history/app-20240702182329-0022/jobs/

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/join-yelp-smj.py
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("join-yelp-smj") \
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

file_loc_reviews = "s3a://owshq/yelp/reviews/*.parquet"
file_loc_business = "s3a://owshq/yelp/business/*.parquet"

df_reviews = spark.read.parquet(file_loc_reviews)
df_business = spark.read.parquet(file_loc_business)

# TODO SortMergeJoin [smj]
df_join_bhj = df_reviews.join(df_business, "business_id")
df_join_bhj.show()
df_join_bhj.explain(True)

spark.stop()
