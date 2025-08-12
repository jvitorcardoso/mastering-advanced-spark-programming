# Manifests Deployment

### helm repo
```sh
helm repo add apache-airflow https://airflow.apache.org
helm repo add elastic https://helm.elastic.co
helm repo update
```

### minio [deepstorage]
```shell
kubectl apply -f app-manifests/deepstorage/minio-operator.yaml
kubectl apply -f app-manifests/deepstorage/minio-tenant.yaml
```

```shell
kubectl apply -f app-manifests/ingestion/strimzi-operator.yaml
kubectl apply -f app-manifests/ingestion/kafka-broker-ephemeral.yaml
kubectl apply -f app-manifests/ingestion/kafka-broker.yaml
kubectl apply -f app-manifests/ingestion/schema-registry.yaml

# service
kubectl apply -f yamls/services/svc-lb-schema-registry.yaml
```

### hive metastore [metastore]
```shell
kubectl apply -f app-manifests/metastore/hive-metastore.yaml
```

### trino [warehouse]
```shell
kubectl apply -f app-manifests/warehouse/trino.yaml
```
