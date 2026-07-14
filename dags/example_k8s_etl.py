"""Example ETL DAG using TaskFlow API and KubernetesPodOperator (Airflow 3.0)."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task

# Default args limited to universally accepted params to avoid TypeError
# with Airflow 3.0 strict validation.
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "k8s", "etl"],
    default_args=default_args,
    doc_md=__doc__,
)
def example_k8s_etl():
    @task()
    def extract() -> dict:
        """Pull raw data. Heavy imports are deferred inside the function."""

        # In practice this would call an API or read from a database.
        # `logical_date` is available via the task context in Airflow 3.0.
        raw = {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}
        return raw

    @task()
    def transform(raw: dict) -> list[dict]:
        """Clean and reshape extracted data."""
        return [{"user_id": u["id"], "username": u["name"].lower()} for u in raw["users"]]

    from airflow.providers.cncf.kubernetes.operators.pod import (
        KubernetesPodOperator,
    )

    load = KubernetesPodOperator(
        task_id="load",
        name="load-to-warehouse",
        image="python:3.13-slim",
        cmds=["echo"],
        arguments=["Load step complete — replace with real loader image."],
        on_finish_action="delete_pod",
    )

    # Dependency chain: extract >> transform >> load (K8s pod)
    transformed = transform(extract())
    transformed >> load


example_k8s_etl()
