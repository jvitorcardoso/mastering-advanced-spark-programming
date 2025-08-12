# Advanced Spark Programming: Expert
https://engenhariadedadosacademy.com/mastering/advanced-spark-programing/

### install required packages [locally]
```shell
pip install -r requirements.txt
```

### verify local spark installation
```shell
pyspark --version
spark-submit --help
```

### create .env file for variables
```shell
.env

APP_SRC_PATH=REPO_PATH/src
APP_STORAGE_PATH=REPO_PATH/storage
APP_LOG_PATH=REPO_PATH/logs
APP_METRICS_PATH=REPO_PATH/metrics
```

### build spark docker images [spark & history server]
```shell
docker build -t owshq-spark:3.5 -f Dockerfile . 
docker build -t owshq-spark-history-server:3.5 -f Dockerfile.history .
```

### run spark cluster & history server on docker
```shell
docker-compose up -d
docker ps

docker logs spark-master
docker logs spark-worker-1
docker logs spark-worker-2
docker logs spark-history-server
```

### download files & save on [storage] folder
```shell
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
https://www.yelp.com/dataset
```

### execute spark application
```shell
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/spark-basic.py
```

### access spark master & history server
```shell
http://localhost:8080/
http://localhost:18080/
```

### tier down resources
```shell
docker-compose down
```
