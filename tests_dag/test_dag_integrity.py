"""Loads the DAG the way the Airflow scheduler does. Runs in the CI `dag` job (needs Airflow)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAG_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_DIR), include_examples=False)


def test_dags_import_cleanly(dagbag):
    assert dagbag.import_errors == {}


def test_lakehouse_dag_shape(dagbag):
    dag = dagbag.dags["lakehouse_daily"]  # .dags avoids get_dag(), which needs a metadata DB
    assert [t.task_id for t in dag.topological_sort()] == [
        "land_raw_data",
        "spark_silver",
        "dbt_build",
        "dbt_docs_generate",
    ]
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_every_task_retries(dagbag):
    dag = dagbag.dags["lakehouse_daily"]  # .dags avoids get_dag(), which needs a metadata DB
    assert all(t.retries >= 1 for t in dag.tasks)
