from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

# KubernetesPodOperator için sürüme göre esnek import
try:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
except Exception:
    try:
        from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    except Exception:
        from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator  # çok eski sürüm fallback

NS_RUN = "spark-processing"

@dag(
    dag_id="infra_validation_local_spark",
    start_date=pendulum.datetime(2025, 9, 3, tz="Europe/Istanbul"),
    schedule=None,
    catchup=False,
    tags=["infra","spark","minio","k8s"],
)
def infra_validation_local():

    write_minio = S3CreateObjectOperator(
        task_id="create_test_file_in_minio",
        aws_conn_id="minio_default",
        s3_bucket="earthquake-data",
        s3_key="validation/source_file.txt",
        data="hello spark from airflow via minio",
        replace=True,
    )

    run_spark_local = KubernetesPodOperator(
        task_id="run_spark_local",
        name="run-spark-local",
        namespace=NS_RUN,
        kubernetes_conn_id="kubernetes_default",
        image="bitnami/spark:3.5.0",
        image_pull_policy="IfNotPresent",
        get_logs=True,
        is_delete_operator_pod=True,
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            "--conf", "spark.hadoop.security.authentication=simple",
            "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio.minio.svc.cluster.local:9000",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf", "spark.hadoop.fs.s3a.access.key={{ conn.minio_default.login }}",
            "--conf", "spark.hadoop.fs.s3a.secret.key={{ conn.minio_default.password }}",
            "--conf", "spark.executorEnv.HADOOP_USER_NAME=spark",
            "--conf", "spark.driver.extraJavaOptions=-Duser.name=spark",
            "--conf", "spark.executor.extraJavaOptions=-Duser.name=spark",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "local:///opt/bitnami/spark/examples/src/main/python/wordcount.py",
            "s3a://earthquake-data/validation/source_file.txt",
        ],
        env_vars={
            "USER": "spark",
            "HADOOP_USER_NAME": "spark",
            "JAVA_TOOL_OPTIONS": "-Duser.name=spark",
        },
        container_resources={
            "request_cpu": "100m",
            "request_memory": "512Mi",
            "limit_cpu": "500m",
            "limit_memory": "1Gi",
        },
        labels={"app": "spark-local-demo"},
    )

    write_minio >> run_spark_local

infra_validation_local()