"""Airflow is not installable on Windows, so this always-on check parses the DAG file statically.
The real import check is tests_dag/, which CI runs with Airflow installed."""

import ast
from pathlib import Path

DAG_FILE = Path(__file__).resolve().parent.parent / "dags" / "lakehouse_daily.py"


def test_dag_file_declares_expected_tasks_and_order():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    task_ids = [
        kw.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "BashOperator"
        for kw in node.keywords
        if kw.arg == "task_id"
    ]
    assert task_ids == ["land_raw_data", "spark_silver", "dbt_build", "dbt_docs_generate"]
    source = DAG_FILE.read_text(encoding="utf-8")
    assert "land_raw_data >> spark_silver >> dbt_build >> dbt_docs_generate" in source
    assert "catchup=False" in source and "max_active_runs=1" in source
