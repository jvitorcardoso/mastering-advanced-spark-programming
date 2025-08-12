"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/spark-basic-query-execution.py

# Broadcast Join Hint [smaller] table ~ 100 MB ==?
# Join Strategy: SortMergeJoin [SMJ]
http://localhost:18080/history/app-20240701183330-0020/SQL/execution/?id=0
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder \
    .appName("spark-basic-query-execution") \
    .config("spark.executor.memory", "3g") \
    .config("spark.sql.adaptive.enabled", False) \
    .config("spark.sql.adaptive.coalescePartitions.enabled", False) \
    .config("spark.sql.adaptive.skewJoin.enabled", False) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

file_loc_business = "../storage/yelp/business/*.parquet"
df_business = spark.read.parquet(file_loc_business)

file_loc_reviews = "../storage/yelp/reviews/*.parquet"
df_reviews = spark.read.parquet(file_loc_reviews)

df_reviews.describe().show()

df_join_reviews_business = df_reviews.alias("r").join(broadcast(df_business.alias("b")), col("r.business_id") == col("b.business_id"))
df_join_reviews_business.explain(True)

spark.stop()
