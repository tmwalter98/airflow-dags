"""Process SMS backup files as they land in a Ceph S3 bucket.

Event-driven: Ceph RGW publishes bucket-notification CloudEvents to a Kafka
topic on object creation. A KafkaMessageQueueTrigger watches that topic and
wakes Airflow's triggerer as soon as a matching event shows up, which
schedules this DAG. No polling schedule needed.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.providers.apache.kafka.triggers.msg_queue import KafkaMessageQueueTrigger
from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import Asset, AssetWatcher, dag, task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

AWS_CONN_ID = "aws_default"  # Ceph RGW S3-compatible endpoint, used for the actual download
KAFKA_CONN_ID = "kafka_default"
BUCKET_NAME = "tmwalter98"
BUCKET_PREFIX = "x"
KAFKA_TOPIC = "knative-broker-knative-eventing-default"

kafka_trigger = KafkaMessageQueueTrigger(
    topics=[KAFKA_TOPIC],
    kafka_config_id=KAFKA_CONN_ID,
    apply_function="kafka_filters.match_ceph_bucket_event",
    apply_function_kwargs={"bucket": BUCKET_NAME, "key_prefix": BUCKET_PREFIX},
)

sms_backup_asset = Asset(
    name=f"kafka://{KAFKA_TOPIC}/{BUCKET_NAME}/{BUCKET_PREFIX}",
    watchers=[AssetWatcher(name="sms_backup_kafka_watcher", trigger=kafka_trigger)],
)


@dag(
    schedule=[sms_backup_asset],
    catchup=False,
    tags=["sms", "kafka", "s3", "event-driven"],
    default_args=default_args,
    doc_md=__doc__,
)
def process_sms_backups():
    @task()
    def get_triggering_key(**context) -> str:
        """Pull the S3 key from the CloudEvent that fired this run."""
        logger.error(str(context))
        triggering_events = context["triggering_asset_events"]
        event = triggering_events[sms_backup_asset][-1]
        return event.extra["payload"]["key"]

    @task()
    def download_and_parse(key: str) -> dict:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        logger.error(key)

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        raw_bytes = hook.get_key(key, bucket_name=BUCKET_NAME).get()["Body"].read()

        # Replace with real SMS backup parsing.
        return {"key": key, "size_bytes": len(raw_bytes)}

    @task()
    def printer(key: str) -> str:
        logger.error(key)
        return key

    _review_before_load = HITLOperator(
        task_id="review_before_load",
        subject="Review parsed SMS backup before loading",
        body="{{ ti.xcom_pull(task_ids='download_and_parse') }}",
        options=["approve", "reject"],
    )

    key = get_triggering_key()

    # parsed = download_and_parse(key)
    # parsed >> review_before_load
    printer(key)


process_sms_backups()
