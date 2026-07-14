"""Validate that all DAGs in dags/ import without errors."""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture()
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_at_least_one_dag(dagbag):
    assert len(dagbag.dags) >= 1, "Expected at least one DAG"
