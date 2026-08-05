"""Match functions for Kafka message queue triggers.

Passed to a trigger's ``apply_function`` (dotted-path string) — called with
the raw confluent_kafka ``Message`` for every message polled off the topic.
Return a truthy value to fire the trigger (it becomes the event payload);
return None/falsy to keep polling.

Lives in plugins/, not dags/: apply_function is imported by the triggerer
process, which never parses DAG files and so never puts dags/ on sys.path.
Airflow does put plugins/ on sys.path for every component at startup, which
is what makes this importable there.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from confluent_kafka import Message


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def match_ceph_bucket_event(message: Message, bucket: str, key_prefix: str) -> dict[str, Any] | None:
    """Match a Ceph RGW bucket-notification CloudEvent for object creation under a prefix.

    Ceph RGW publishes a CloudEvents envelope whose `data` field holds an
    S3-style event body (`Records[].s3.bucket.name`, `Records[].s3.object.key`,
    `Records[].eventName`) — the same shape AWS S3 event notifications use.
    """
    logger.error(bucket)
    logger.error(key_prefix)
    envelope = json.loads(message.value().decode("utf-8"))
    body = envelope.get("data", envelope)

    for record in body.get("Records", []):
        event_name = record.get("eventName", "")
        s3_info = record.get("s3", {})
        object_key = s3_info.get("object", {}).get("key")
        bucket_name = s3_info.get("bucket", {}).get("name")

        if (
            event_name.startswith("s3:ObjectCreated")
            and bucket_name == bucket
            and object_key
            and object_key.startswith(key_prefix)
        ):
            return {"bucket": bucket_name, "key": object_key, "event_name": event_name}

    return None
