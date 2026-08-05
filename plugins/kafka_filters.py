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

    Unlike AWS S3 event notifications, Ceph RGW publishes one flat event per
    message (no `Records` wrapper), and `eventName` has no `s3:` scheme
    prefix — e.g. `"ObjectCreated:Put"` rather than `"s3:ObjectCreated:Put"`.
    """
    envelope = json.loads(message.value().decode("utf-8"))
    record = envelope.get("data", envelope)

    event_name = record.get("eventName", "")
    s3_info = record.get("s3", {})
    object_key = s3_info.get("object", {}).get("key")
    bucket_name = s3_info.get("bucket", {}).get("name")

    key_prefix_mask = object_key.startswith(key_prefix) if (bool(key_prefix) and isinstance(key_prefix, str)) else True
    if event_name.startswith("ObjectCreated") and bucket_name == bucket and object_key and key_prefix_mask:
        return {"bucket": bucket_name, "key": object_key, "event_name": event_name}

    return None
