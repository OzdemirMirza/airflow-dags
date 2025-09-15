from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="spark_pi",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["spark", "k8s"],
) as dag:

    # Spark Application submit
    submit_pi = SparkKubernetesOperator(
        task_id="submit_spark_pi",
        namespace="spark-operator",   # spark-operator CRD’nin çalıştığı namespace
        application_file="/kubernetes/spark-pi.yaml",  # DAG repo’sunda kubernetes/ altında
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    # Spark Application result check
    check_pi = SparkKubernetesSensor(
        task_id="check_spark_pi",
        namespace="spark-operator",
        application_name="{{ task_instance.xcom_pull(task_ids='submit_spark_pi')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    submit_pi >> check_pi