from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

NS = "spark-processing"
APP_NAME = "pi-validation-{{ ts_nodash }}"

@dag(
    dag_id="pi_smoke",
    start_date=pendulum.datetime(2025, 9, 4, tz="Europe/Istanbul"),
    schedule=None,
    catchup=False,
    tags=["smoke","spark"],
)
def _pi_smoke():
    submit = SparkKubernetesOperator(
        task_id="submit_pi",
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

  mainApplicationFile: local:///opt/spark-apps/app.py
  arguments: ["4"]

  restartPolicy:
    type: Never

  driver:
    serviceAccount: spark-sa
    cores: 1
    memory: "512m"
    env:
      - name: USER
        value: "spark"
      - name: HADOOP_USER_NAME
        value: "spark"
      - name: JAVA_TOOL_OPTIONS
        value: "-Duser.name=spark"
    volumeMounts:
      - name: app-volume
        mountPath: /opt/spark-apps

  executor:
    instances: 1
    cores: 1
    memory: "512m"
    serviceAccount: spark-sa
    env:
      - name: USER
        value: "spark"
      - name: HADOOP_USER_NAME
        value: "spark"
      - name: JAVA_TOOL_OPTIONS
        value: "-Duser.name=spark"
    volumeMounts:
      - name: app-volume
        mountPath: /opt/spark-apps

  volumes:
    - name: app-volume
      configMap:
        name: spark-pi
        items:
          - key: app.py
            path: app.py

  sparkConf:
    "spark.kubernetes.driver.request.cores": "100m"
    "spark.kubernetes.executor.request.cores": "100m"
    "spark.kubernetes.executor.deleteOnTermination": "false"
    "spark.driver.extraJavaOptions": "-Duser.name=spark"
    "spark.executor.extraJavaOptions": "-Duser.name=spark"
"""
    )

    wait = SparkKubernetesSensor(
        task_id="wait_pi",
        namespace=NS,
        kubernetes_conn_id="kubernetes_default",
        application_name=APP_NAME,
        attach_log=True,
        timeout=60*15,
        poke_interval=10,
        mode="reschedule",
    )
    submit >> wait

pi_smoke = _pi_smoke()