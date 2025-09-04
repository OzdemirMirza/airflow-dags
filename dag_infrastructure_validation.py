from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

APP_NAME = "spark-validation-{{ ds_nodash }}"
NS = "spark-processing"

@dag(
    dag_id="infrastructure_validation",
    start_date=pendulum.datetime(2025, 9, 3, tz="Europe/Istanbul"),
    schedule=None,
    catchup=False,
    tags=["infra","spark","minio"],
)
def infrastructure_validation_dag():

    write_minio = S3CreateObjectOperator(
        task_id="create_test_file_in_minio",
        aws_conn_id="minio_default",
        s3_bucket="earthquake-data",
        s3_key="validation/source_file.txt",
        data="Infrastructure validation successful!",
        replace=True,
    )

    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        namespace=NS,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        application_file=f"""
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: {APP_NAME}
  namespace: {NS}
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
    "spark.hadoop.fs.s3a.access.key": "{{{{ conn.minio_default.login }}}}"
    "spark.hadoop.fs.s3a.secret.key": "{{{{ conn.minio_default.password }}}}"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"

    "spark.jars.ivy": "/tmp/.ivy2"
    "spark.kubernetes.submission.localDir": "/tmp"
"""
    )

    wait_spark = SparkKubernetesSensor(
        task_id="wait_spark",
        namespace=NS,
        kubernetes_conn_id="kubernetes_default",
        application_name=APP_NAME,
        attach_log=True,
        timeout=60*30,
        poke_interval=10,
        mode="reschedule",
    )

    @task(trigger_rule="all_done")
    def cleanup():
        """SparkApplication CRD’yi sil (DeleteOperator yerine)."""
        hook = KubernetesHook(conn_id="kubernetes_default")
        api = hook.get_conn()
        from kubernetes import client
        crd = client.CustomObjectsApi(api)
        try:
            crd.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=NS,
                plural="sparkapplications",
                name=pendulum.now("UTC").format("YYYYMMDD"),  # <- yanlış! sadece örnek
            )
        except Exception as e:
            # Task’ı fail etme; zaten all_done ile çalışıyoruz.
            import logging
            logging.info(f"Cleanup skip: {e}")

    # DÜZELTME: cleanup'ın doğru adı silmesi için Jinja kullan
    # Python task içinde Jinja çalışmadığından adı param ile verelim:
    @task(trigger_rule="all_done")
    def cleanup_with_name(app_name: str):
        hook = KubernetesHook(conn_id="kubernetes_default")
        api = hook.get_conn()
        from kubernetes import client
        crd = client.CustomObjectsApi(api)
        try:
            crd.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=NS,
                plural="sparkapplications",
                name=app_name,
            )
        except Exception as e:
            import logging
            logging.info(f"Cleanup skip: {e}")

    app_name = APP_NAME  # string içinde Jinja var, Airflow runtime'da render eder
    write_minio >> submit_spark >> wait_spark >> cleanup_with_name(app_name)

infrastructure_validation_dag()