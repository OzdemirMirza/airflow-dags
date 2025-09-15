import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3,
}

with DAG(
    dag_id="spark_pi",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),  # güncel bir tarih ver
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["example"],
) as dag:

    submit = SparkKubernetesOperator(
        task_id="spark_transform_data",
        namespace="spark-operator",
        application_file="/kubernetes/spark-pi.yaml",  # senin yaml path’in
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    submit