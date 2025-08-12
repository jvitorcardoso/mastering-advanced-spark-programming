"""
The Suffle
Shuffle is a Spark mechanism that redistributes data so that it's grouped differently across partitions. This typically
involves copying data across executors and machines and, while it's sometimes necessary,
it can be a complex and costly operation.

The Broadcast Join
Can be significantly faster than shuffle joins if one of the tables is very large and the other is small.
Unfortunately, broadcast joins only work if at least one of the tables are less than 100MB in size.
In case of joining bigger tables,  if we want to avoid the shuffle we may need to reconsider our schema to avoid
having to do the join in the first place.

Executing Job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/join-seq-shuffle.py

Duration: 4.3 min
http://localhost:18080/history/app-20240702195034-0017/SQL/execution/?id=7

Duration: 3.7 min
http://localhost:18080/history/app-20240702195034-0017/SQL/execution/?id=6
"""

import time
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("join-seq-shuffle") \
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

# TODO 150 million rows
df_transactions = spark.range(0, 150_000_000, 1, 32) \
    .select('id',
            round(rand() * 10000, 2).alias('amount'),
            (col('id') % 10).alias('country_id'),
            (col('id') % 100).alias('store_id')
    )
df_transactions.write.format("parquet").mode("overwrite").save("s3a://owshq/transactions/")

df_stores = spark.range(0, 99) \
    .select('id',
            round(rand() * 100, 0).alias('employees'),
            (col('id') % 10).alias('country_id'), expr('uuid()').alias('name')
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

df_countries = spark.createDataFrame(data=countries, schema=columns)
df_countries.write.format("parquet").mode("overwrite").save("s3a://owshq/countries/")

# TODO explicitly turning off broadcast joins in order to demonstrate shuffle.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)

df_transactions.createOrReplaceTempView("transactions")
df_stores.createOrReplaceTempView("stores")
df_countries.createOrReplaceTempView("countries")

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

joined_df.write.format("parquet").mode("overwrite").save("s3a://owshq/transact_countries/")
end_time = time.time()
print(f"transact countries time: {end_time - start_time} seconds")

# TODO broadcast join avoids the shuffle
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

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
print(f"transact countries time [broadcast]: {end_time - start_time} seconds")

spark.stop()
