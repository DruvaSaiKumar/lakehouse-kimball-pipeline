"""Daily lakehouse refresh.

    land_raw_data -> spark_silver -> dbt_build -> dbt_docs_generate

* land_raw_data: stands in for real ingestion. Here it regenerates the synthetic landing files;
  replace it with the task that copies the day's extract into data/landing/.
* spark_silver: PySpark cleanse/validate/deduplicate/quarantine into silver Parquet.
* dbt_build: builds the Kimball star schema and marts, then runs every dbt data test. A failing test
  fails the task, so a bad load never reaches consumers as if it were good.
* dbt_docs_generate: refreshes the model documentation and lineage.

max_active_runs is 1 because the silver step rebuilds its output; two overlapping runs would clobber
each other.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ.get("LAKEHOUSE_ROOT", "/opt/lakehouse")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_daily",
    description="Landing -> Spark silver -> dbt star schema",
    start_date=datetime(2024, 6, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["lakehouse", "spark", "dbt"],
    doc_md=__doc__,
) as dag:
    land_raw_data = BashOperator(
        task_id="land_raw_data",
        bash_command=f"cd {ROOT} && python -m data_gen.generate --landing data/landing",
    )
    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=f"cd {ROOT} && python -m pipeline.run --engine spark",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {ROOT}/dbt && dbt build --profiles-dir .",
    )
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {ROOT}/dbt && dbt docs generate --profiles-dir .",
    )

    land_raw_data >> spark_silver >> dbt_build >> dbt_docs_generate
