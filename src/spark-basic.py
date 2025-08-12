"""
the cluster managers:

Standalone
$ spark-submit --master spark://<master-ip>:7077 your_script.py

YARN
$ spark-submit --master yarn --deploy-mode cluster your_script.py

Kubernetes
$ spark-submit --master k8s://<kubernetes-api> --deploy-mode cluster your_script.py

executing job:

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/spark-basic.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("spark-basic") \
    .getOrCreate()

data = [("Luan Moreno", 36), ("Mateus Oliveira", 37)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people WHERE Age > 30")
sqlDF.show()

spark.stop()
