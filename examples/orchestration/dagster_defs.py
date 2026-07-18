"""Dagster definitions: EL with faucet, T with dbt.

The same shell-out pattern as the Airflow DAG, expressed as two Dagster assets
with a dependency edge. faucet being a single binary keeps the op bodies to a
one-line subprocess call — no runtime to embed.

Run the UI with:
    dagster dev -f examples/orchestration/dagster_defs.py

Assumes `faucet` and `dbt` are on PATH and the pipeline env vars are set.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, Definitions, asset

HERE = Path(__file__).resolve().parent
FAUCET_CONFIG = HERE / "faucet_pipeline.yaml"
DBT_DIR = HERE / "dbt"


@asset
def charges_raw(context: AssetExecutionContext) -> None:
    """Extract + Load: faucet lands raw charges in Postgres."""
    context.log.info("Running faucet EL")
    subprocess.run(["faucet", "run", str(FAUCET_CONFIG)], check=True)


@asset(deps=[charges_raw])
def stg_charges(context: AssetExecutionContext) -> None:
    """Transform + test: dbt builds the staging models over the raw load."""
    context.log.info("Running dbt build")
    subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
        ],
        check=True,
    )


defs = Definitions(assets=[charges_raw, stg_charges])
