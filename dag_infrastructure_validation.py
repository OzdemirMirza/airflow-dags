from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
# (İstersen işi bitince CRD’yi silmek için:)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesDeleteOperator

@dag(
    dag_id="infrastructure_validation",
    start_date=pendulum.datetime(2025, 9, 3, tz="Europe/Istanbul"),
    schedule=None,
    catchup=False,
    tags=["infra","spark","minio"],
)
def infrastructure_validation_dag():

    # 1) MinIO’ya test dosyası yaz
    create_test_file_in_minio = S3CreateObjectOperator(
        task_id="create_test_file_in_minio",
        aws_conn_id="minio_default",
        s3_bucket="earthquake-data",
        s3_key="validation/source_file.txt",
        data="Infrastructure validation successful!",
        replace=True,
    )

    # 2) SparkApplication CRD’yi oluştur (SUBMIT)
    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        namespace="spark-processing",
        kubernetes_conn_id="kubernetes_default",
        application_file="""
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-validation-{{ ds_nodash }}
  namespace: spark-processing
spec:
  type: Python
  mode: cluster
  image: bitnami/spark:3.5.0
  imagePullPolicy: IfNotPresent
  sparkVersion: "3.5.0"

  mainApplicationFile: local:///opt/bitnami/spark/examples/src/main/python/wordcount.py
  arguments:
    - "s3a://earthquake-data/validation/source_file.txt"

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
        do_xcom_push=False,   # sadece CRD yaratıyoruz, beklemiyoruz
    )

    # 3) SparkApplication durumunu bekle (COMPLETED/FAILED)
    wait_spark = SparkKubernetesSensor(
        task_id="wait_spark",
        namespace="spark-processing",
        kubernetes_conn_id="kubernetes_default",
        application_name="spark-validation-{{ ds_nodash }}",
        attach_log=True,         # driver loglarını UI’a akıtmaya çalışır
        timeout=60*30,           # 30 dk
        poke_interval=10,        # her 10 sn’de bir kontrol
        mode="reschedule",       # worker slotunu meşgul etmez
    )

    # 4) (Opsiyonel) İş bitince CRD’yi sil
    cleanup = SparkKubernetesDeleteOperator(
        task_id="cleanup_spark_app",
        namespace="spark-processing",
        kubernetes_conn_id="kubernetes_default",
        application_name="spark-validation-{{ ds_nodash }}",
        delete_on_termination=True,
        trigger_rule="all_done",
    )

    create_test_file_in_minio >> submit_spark >> wait_spark >> cleanup

infrastructure_validation_dag()