import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from data_gen.generate import generate
from pipeline.duckdb_silver import build_silver

ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "dbt"
# dbt has no `python -m dbt`; this is the same programmatic entry point the `dbt` script uses.
DBT_ENTRYPOINT = (
    "import sys; from dbt.cli.main import dbtRunner; "
    "sys.exit(0 if dbtRunner().invoke(sys.argv[1:]).success else 1)"
)


@pytest.fixture(scope="session")
def landing(tmp_path_factory):
    path = tmp_path_factory.mktemp("data") / "landing"
    return path, generate(path)


@pytest.fixture(scope="session")
def duckdb_silver(landing, tmp_path_factory):
    base = tmp_path_factory.mktemp("duckdb_engine")
    metrics = build_silver(landing[0], base / "silver", base / "quarantine")
    return {"silver": base / "silver", "quarantine": base / "quarantine", "metrics": metrics}


def run_dbt(command: str, silver: Path, work: Path) -> Path:
    """Run a dbt command against `silver`; return the warehouse file it built.

    A subprocess, not dbtRunner: in-process, dbt keeps its DuckDB connection cached and a later
    connection to the same file from the test would conflict with it.
    """
    warehouse = work / "warehouse.duckdb"
    result = subprocess.run(
        [
            sys.executable, "-c", DBT_ENTRYPOINT, command,
            "--project-dir", str(DBT_DIR),
            "--profiles-dir", str(DBT_DIR),
            "--target-path", str(work / "target"),
            "--log-path", str(work / "logs"),
            "--vars", json.dumps({"silver_path": silver.as_posix()}),
            "--no-use-colors",
        ],
        env={**os.environ, "WAREHOUSE_PATH": str(warehouse)},
        capture_output=True,
        text=True,
    )  # fmt: skip
    assert result.returncode == 0, f"dbt {command} failed:\n{result.stdout[-3000:]}\n{result.stderr[-1000:]}"
    return warehouse


@pytest.fixture(scope="session")
def warehouse(duckdb_silver, tmp_path_factory):
    return run_dbt("build", duckdb_silver["silver"], tmp_path_factory.mktemp("dbt_duckdb"))


@pytest.fixture
def wh(warehouse):
    con = duckdb.connect(str(warehouse), read_only=True)
    yield con
    con.close()
