from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

NS = "spark-processing"

@dag(
    dag_id="infrastructure_validation",
    start_date=pendulum.datetime(2025, 9, 3, tz="Europe/Istanbul"),
    schedule=None,
    catchup=False,
    tags=["infra","spark","minio"],
)
def infra_validation():

    # 1) MinIO'ya test dosyası yaz
    write_minio = S3CreateObjectOperator(
        task_id="create_test_file_in_minio",
        aws_conn_id="minio_default",
        s3_bucket="earthquake-data",
        s3_key="validation/source_file.txt",
        data="hello\nworld\nspark\n",
        replace=True,
    )

    # 2) SparkApplication gönder (K8s CRD)
    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        namespace=NS,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        application_file="""
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-validation-{{ ts_nodash }}
  namespace: spark-processing
spec:
  type: Python
  mode: cluster
  image: bitnami/spark:3.5.0
  imagePullPolicy: IfNotPresent
  sparkVersion: "3.5.0"

  # Kod: ConfigMap'ten mount edip local:/// ile çalıştırıyoruz
  mainApplicationFile: local:///opt/spark-apps/app.py

  restartPolicy:
    type: Never

  # Hem driver hem executor aynı SA ve env ile
  driver:
    serviceAccount: spark-sa
    cores: 1
    memory: "768m"
    env:
      - name: USER
        value: spark
      - name: HADOOP_USER_NAME
        value: spark
    volumeMounts:
      - name: app-code
        mountPath: /opt/spark-apps

  executor:
    serviceAccount: spark-sa
    instances: 1
    cores: 1
    memory: "512m"
    env:
      - name: USER
        value: spark
      - name: HADOOP_USER_NAME
        value: spark
    volumeMounts:
      - name: app-code
        mountPath: /opt/spark-apps

  # ConfigMap'i her iki tarafa mount et
  volumes:
    - name: app-code
      configMap:
        name: spark-app

  # S3A bağımlılıkları (driver & executor otomatik indirir)
  deps:
    packages:
      - org.apache.hadoop:hadoop-aws:3.3.4
      - com.amazonaws:aws-java-sdk-bundle:1.12.262

  sparkConf:
    # MinIO / S3A
    spark.hadoop.fs.s3a.endpoint: http://minio.minio.svc.cluster.local:9000
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.hadoop.fs.s3a.access.key: "{{ conn.minio_default.login }}"
    spark.hadoop.fs.s3a.secret.key: "{{ conn.minio_default.password }}"
    spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem

    # Kerberos'a düşmesin
    spark.hadoop.security.authentication: simple

    # Küçük cluster kaynakları
    spark.kubernetes.driver.request.cores: "100m"
    spark.kubernetes.executor.request.cores: "100m"
    spark.kubernetes.driver.memoryOverhead: "256m"
    spark.kubernetes.executor.memoryOverhead: "256m"

    # Ivy/tmp
    spark.jars.ivy: /tmp/.ivy2
    spark.kubernetes.submission.localDir: /tmp

    # Debug için (executor düşse de log kalır)
    spark.kubernetes.executor.deleteOnTermination: "false"
""",
    )

    # 3) Spark bitti mi sensörle bekle
    wait_spark = SparkKubernetesSensor(
        task_id="wait_spark",
        namespace=NS,
        kubernetes_conn_id="kubernetes_default",
        application_name="spark-validation-{{ ts_nodash }}",
        attach_log=True,
        timeout=60*30,
        poke_interval=10,
        mode="reschedule",
    )

    write_minio >> submit_spark >> wait_spark

infra_validation()