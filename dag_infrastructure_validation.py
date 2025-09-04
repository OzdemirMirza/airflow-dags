# dags/infrastructure_validation.py
from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

@dag(
    dag_id="infrastructure_validation",
    start_date=pendulum.datetime(2025, 9, 3, tz="Europe/Istanbul"),
    schedule=None,
    catchup=False,
    tags=["infrastructure", "validation", "git-sync"],
)
def infrastructure_validation_dag():
    # 1) MinIO'ya test dosyası yaz
    create_test_file_in_minio = S3CreateObjectOperator(
        task_id="create_test_file_in_minio",
        aws_conn_id="minio_default",
        s3_bucket="earthquake-data",
        s3_key="validation/source_file.txt",
        data="Infrastructure validation successful!",
        replace=True,
    )

    # 2) Spark ile dosyayı oku (küçük kaynak değerleri!)
    read_file_with_spark = SparkKubernetesOperator(
        task_id="read_file_with_spark",
        namespace="spark-processing",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        application_file="""
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-validation-{{ ds_nodash }}
  namespace: spark-processing
spec:
  type: Python
  mode: cluster
  image: "bitnami/spark:3.5.0"
  imagePullPolicy: IfNotPresent
  sparkVersion: "3.5.0"

  mainApplicationFile: local:///opt/bitnami/spark/examples/src/main/python/wordcount.py
  arguments:
    - "s3a://earthquake-data/validation/source_file.txt"

  # AWS/MinIO s3a için gerekli paketleri al; ivy'yi /tmp'ye yaz
  deps:
    packages:
      - "org.apache.hadoop:hadoop-aws:3.3.4"
      - "com.amazonaws:aws-java-sdk-bundle:1.12.262"

  restartPolicy:
    type: Never

  driver:
    serviceAccount: spark-sa
    cores: 1
    memory: "384m"
  executor:
    instances: 1
    cores: 1
    memory: "384m"

  sparkConf:
    "spark.kubernetes.driver.request.cores": "100m"
    "spark.kubernetes.executor.request.cores": "100m"
    "spark.kubernetes.driver.memoryOverhead": "128m"
    "spark.kubernetes.executor.memoryOverhead": "128m"

    "spark.hadoop.fs.s3a.endpoint": "http://minio.minio.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    "spark.hadoop.fs.s3a.access.key": "{{ conn.minio_default.login }}"
    "spark.hadoop.fs.s3a.secret.key": "{{ conn.minio_default.password }}"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"

    "spark.jars.ivy": "/tmp/.ivy2"
    "spark.kubernetes.submission.localDir": "/tmp"
""",
    )

    create_test_file_in_minio >> read_file_with_spark

infrastructure_validation_dag()