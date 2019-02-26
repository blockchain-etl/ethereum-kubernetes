import logging
from datetime import datetime, timedelta
from uuid import uuid4

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.kubernetes import kube_client

import kubernetes

from web3 import Web3

LABEL_APP = "ethereum-snapshotter"
PREFIX = "ethereum-snapshotter"
START_DATE = datetime(2015, 7, 30)


class EthereumSyncSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(EthereumSyncSensor, self).__init__(*args, **kwargs)

        self.last_timestamp = None

    def poke(self, context):
        task_instance = context["task_instance"]
        target_timestamp = context["execution_date"]

        service_name = task_instance.xcom_pull("create_service_task")

        client = kube_client.get_kube_client()
        service = client.read_namespaced_service(service_name, "default")

        self.log.info("Poking Ethereum RPC...")

        web3 = Web3(Web3.HTTPProvider(f"http://{service.spec.cluster_ip}:8545"))

        block_number = web3.eth.blockNumber
        block = web3.eth.getBlock(block_number)
        timestamp = datetime.utcfromtimestamp(block["timestamp"])

        if self.last_timestamp and not timestamp > self.last_timestamp:
            raise Exception("Sync is not progressing.")

        self.last_timestamp = timestamp

        if timestamp > target_timestamp:
            self.log.info("Ethereum in sync: " + timestamp.isoformat())
            return True

        self.log.info("Ethereum not in sync: " + timestamp.isoformat())
        return False


def create_pvc(**kwargs):
    ds = kwargs["ds"]
    prev_ds = kwargs["prev_ds"]

    pvc_name = f"{PREFIX}-pvc-{str(uuid4())[:8]}"

    pvc_body = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc_name, "labels": {"app.kubernetes.io/name": LABEL_APP}},
        "spec": {
            "resources": {"requests": {"storage": "1Gi"}},
            "accessModes": ["ReadWriteOnce"],
        },
    }

    logging.info(f"Creating PVC for {ds}")

    if ds != START_DATE.date().isoformat():
        pvc_body["metadata"]["annotations"] = {
            "snapshot.alpha.kubernetes.io/snapshot": prev_ds
        }
        pvc_body["spec"]["storageClassName"] = "snapshot-promoter"

        logging.info(f"Restoring snapshot from {prev_ds}")

    client = kube_client.get_kube_client()
    client.create_namespaced_persistent_volume_claim("default", pvc_body)

    return pvc_name


def create_pod(**kwargs):
    task_instance = kwargs["task_instance"]
    pvc_name = task_instance.xcom_pull("create_pvc_task")

    pod_name = f"{PREFIX}-pod-{str(uuid4())[:8]}"

    pod_body = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "labels": {"app.kubernetes.io/name": LABEL_APP, "pod": pod_name},
        },
        "spec": {
            "restartPolicy": "Never",
            "securityContext": {"fsGroup": 1000},
            "containers": [
                {
                    "name": pod_name,
                    "image": "parity/parity:v2.2.10",
                    "args": ["--config=/config/parity.toml"],
                    "ports": [{"containerPort": 8545}],
                    "volumeMounts": [
                        {"name": "config", "mountPath": "/config"},
                        {"name": "data", "mountPath": "/data"},
                    ],
                }
            ],
            "volumes": [
                {"name": "config", "configMap": {"name": "parity-config"}},
                {"name": "data", "persistentVolumeClaim": {"claimName": pvc_name}},
            ],
        },
    }

    client = kube_client.get_kube_client()
    client.create_namespaced_pod("default", pod_body)

    return pod_name


def create_service(**kwargs):
    task_instance = kwargs["task_instance"]
    pod_name = task_instance.xcom_pull("create_pod_task")

    service_name = f"{PREFIX}-svc-{str(uuid4())[:8]}"

    service_body = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": service_name,
            "labels": {"app.kubernetes.io/name": LABEL_APP},
        },
        "spec": {
            "selector": {"pod": pod_name},
            "ports": [{"port": 8545, "protocol": "TCP", "targetPort": 8545}],
        },
    }

    client = kube_client.get_kube_client()
    client.create_namespaced_service("default", service_body)

    return service_name


def delete_pod(**kwargs):
    task_instance = kwargs["task_instance"]
    pod_name = task_instance.xcom_pull("create_pod_task")

    client = kube_client.get_kube_client()
    client.delete_namespaced_pod(
        pod_name, "default", body=kubernetes.client.V1DeleteOptions()
    )


def delete_service(**kwargs):
    task_instance = kwargs["task_instance"]
    service_name = task_instance.xcom_pull("create_service_task")

    client = kube_client.get_kube_client()
    client.delete_namespaced_service(
        service_name, "default", body=kubernetes.client.V1DeleteOptions()
    )


def create_snapshot(**kwargs):
    ds = kwargs["ds"]
    task_instance = kwargs["task_instance"]
    pvc_name = task_instance.xcom_pull("create_pvc_task")

    snapshot_body = {
        "apiVersion": "volumesnapshot.external-storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {"name": ds, "labels": {"app.kubernetes.io/name": LABEL_APP}},
        "spec": {"persistentVolumeClaimName": pvc_name},
    }

    client = kube_client.get_kube_client()
    custom_objects_api = kubernetes.client.CustomObjectsApi(client.api_client)

    custom_objects_api.create_namespaced_custom_object(
        "volumesnapshot.external-storage.k8s.io",
        "v1",
        "default",
        "volumesnapshots",
        snapshot_body,
    )


def delete_pvc(**kwargs):
    task_instance = kwargs["task_instance"]
    pvc_name = task_instance.xcom_pull("create_pvc_task")

    client = kube_client.get_kube_client()

    client.delete_namespaced_persistent_volume_claim(
        pvc_name, "default", body=kubernetes.client.V1DeleteOptions()
    )


dag = DAG(
    "ethereum_snapshotter",
    description="Ethereum snapshotter DAG",
    schedule_interval=timedelta(days=1),
    start_date=START_DATE,
)

create_pvc_operator = PythonOperator(
    task_id="create_pvc_task", python_callable=create_pvc, provide_context=True, dag=dag
)
create_pod_operator = PythonOperator(
    task_id="create_pod_task", python_callable=create_pod, provide_context=True, dag=dag
)
create_service_operator = PythonOperator(
    task_id="create_service_task",
    python_callable=create_service,
    provide_context=True,
    dag=dag,
)
# TODO: increase poke interval
wait_for_sync_operator = EthereumSyncSensor(
    task_id="wait_for_sync_task", poke_interval=5, provide_context=True, dag=dag
)
delete_pod_operator = PythonOperator(
    task_id="delete_pod_task", python_callable=delete_pod, provide_context=True, dag=dag
)
delete_service_operator = PythonOperator(
    task_id="delete_service_task",
    python_callable=delete_service,
    provide_context=True,
    dag=dag,
)
create_snapshot_operator = PythonOperator(
    task_id="create_snapshot_task",
    python_callable=create_snapshot,
    provide_context=True,
    dag=dag,
)
delete_pvc_operator = PythonOperator(
    task_id="delete_pvc_task", python_callable=delete_pvc, provide_context=True, dag=dag
)

(
    create_pvc_operator
    >> create_pod_operator
    >> create_service_operator
    >> wait_for_sync_operator
    >> delete_pod_operator
    >> create_snapshot_operator
    >> delete_service_operator
    >> delete_pvc_operator
)
