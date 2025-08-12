"""
Overusing Collect

the collect() function:
retrieve the entire dataset from the distributed cluster to the driver node.

1. Data Analysis and Debugging:
- inspect the data to understand its structure

2. Driver-Side Operations:
- pandas_df = spark_df.collect().toPandas()

3. Output Generation:
- generating reports, visualizations, or exporting data to local files

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/spark-collect-fnc.py

reviews_dataset = df_reviews.collect() [1.8 minutes]
- java.lang.IllegalStateException: unread block data
- http://localhost:18080/history/app-20240702172145-0034/jobs/

reviews_dataset = df_reviews.take(5) [1.2 minutes]
- http://localhost:18080/history/app-20240702172425-0035/jobs/
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("spark-collect-fnc") \
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
file_loc_users = "s3a://owshq/yelp/users/*.parquet"
file_loc_business = "s3a://owshq/yelp/business/*.parquet"

df_reviews = spark.read.parquet(file_loc_reviews)
df_users = spark.read.parquet(file_loc_users)
df_business = spark.read.parquet(file_loc_business)

print(f"total rows [reviews]: {df_reviews.count()}")
print(f"total rows [users]: {df_users.count()}")
print(f"total rows [business]: {df_business.count()}")

df_reviews.show(5)
df_users.show(5)
df_business.show(5)

# TODO use of the collect() function
business_list = df_business.collect()
print(business_list[:5])

try:
    reviews_dataset = df_reviews.collect()
    # TODO reviews_dataset = df_reviews.take(5)
    print({len(reviews_dataset)})
except Exception as e:
    print(f"Error: {e}")

spark.stop()
