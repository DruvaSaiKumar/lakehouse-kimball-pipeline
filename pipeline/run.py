"""Run the landing -> silver step.

python -m pipeline.run --engine duckdb      # local, no JVM
python -m pipeline.run --engine spark       # PySpark (needs Java and pyspark)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def run(engine: str, landing: Path, silver: Path, quarantine: Path, metrics_path: Path | None) -> dict:
    if engine == "duckdb":
        from .duckdb_silver import build_silver

        metrics = build_silver(landing, silver, quarantine)
    elif engine == "spark":
        from .spark_silver import build_silver, get_spark

        spark = get_spark()
        try:
            metrics = build_silver(landing, silver, quarantine, spark)
        finally:
            spark.stop()
    else:
        raise ValueError(f"unknown engine {engine!r}")
    if metrics_path:
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["duckdb", "spark"], default="duckdb")
    parser.add_argument("--landing", default="data/landing")
    parser.add_argument("--silver", default="data/silver")
    parser.add_argument("--quarantine", default="data/quarantine")
    parser.add_argument("--metrics", default="data/metrics.json")
    args = parser.parse_args()
    metrics = run(
        args.engine, Path(args.landing), Path(args.silver), Path(args.quarantine), Path(args.metrics)
    )
    print(json.dumps(metrics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
