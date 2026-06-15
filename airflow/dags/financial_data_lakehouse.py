"""
financial_data_lakehouse — daily medallion pipeline.

Per source:  landing -> bronze -> transform(silver) -> data quality
Once every DQ check passes:  dbt build (gold star schema + tests)

The Glue jobs and DQ rulesets are created by Terraform; this DAG only *triggers*
them and passes ingest_date / api dates at runtime ({{ ds }}) — which is what
makes backfills work (airflow dags backfill ...) and removes the Terraform-baked
dates. Heavy compute runs in AWS; Airflow is just the orchestrator.
"""
import os
from datetime import datetime, timedelta

import pandas_market_calendars as mcal
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.glue import (
    GlueDataQualityRuleSetEvaluationRunOperator,
    GlueJobOperator,
)
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# --- project constants ---
# Account-specific values come from the environment (set in docker-compose from
# the Terraform outputs) so the account id / ARNs stay out of version control.
PROJECT = "financial-data-lakehouse"
REGION = os.environ.get("AWS_REGION", "eu-central-1")
SILVER_DB = os.environ.get("SILVER_DB", "financial_data_lakehouse_silver")
GOLD_BUCKET = os.environ["GOLD_BUCKET"]
# DQ runs assume this role (made DQ-capable in the transform module's policy).
GLUE_ROLE_ARN = os.environ["GLUE_ROLE_ARN"]

# Rendered in the Airflow UI (the DAG's docs panel). Markdown, unlike the plain
# module docstring, so it formats with headers/tables in the browser.
DOC_MD = """
### financial_data_lakehouse — daily medallion pipeline

Per source: `landing → bronze → transform (silver) → data quality`.
Once **every** source's DQ check passes: `dbt build` (gold star schema + tests).

The Glue jobs and DQ rulesets are created by **Terraform**; this DAG only
*triggers* them and injects `ingest_date` / API dates at runtime (`{{ ds }}`),
which is what makes backfills work. Heavy compute runs in AWS — Airflow only
orchestrates.

**Where to look when something fails**
- Red task → open its **Logs** for the Glue JobRunId / DQ rule results / dbt output.
- Spark detail → CloudWatch (link is in the Glue task log).
- Full DQ breakdown → Glue Data Quality console.
"""

# source -> silver table + Glue DQ ruleset (Terraform) + market calendar.
# `calendar` gates the branch to real sessions only, so the strict landing jobs
# run only when data is expected:
#   binance    -> None   (crypto trades 24/7, never gated)
#   twelvedata -> NASDAQ (the exchange we pull equities from)
#   fred       -> SIFMAUS (US bond/federal calendar; differs from equities, e.g.
#                          Columbus/Veterans Day closed, Good Friday open)
SOURCES = {
    "binance":    {"table": "binance_klines", "ruleset": "binance_dq_ruleset",    "calendar": None},
    "twelvedata": {"table": "equity_prices",  "ruleset": "twelvedata_dq_ruleset", "calendar": "NASDAQ"},
    "fred":       {"table": "fred_macro",     "ruleset": "fred_dq_ruleset",       "calendar": "SIFMAUS"},
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Runtime args shared by landing + bronze. api_end_date is exclusive -> next day.
DATE_ARGS = {
    "--ingest_date": "{{ ds }}",
    "--api_start_date": "{{ ds }}",
    "--api_end_date": "{{ macros.ds_add(ds, 1) }}",
}


def glue_job(task_id: str, job_name: str, script_args: dict) -> GlueJobOperator:
    # Jobs already exist (Terraform) — pass only job_name + runtime args so the
    # operator runs the existing job instead of recreating it. Everything else
    # (buckets, interval, config paths) stays as the job's baked defaults.
    return GlueJobOperator(
        task_id=task_id,
        job_name=job_name,
        region_name=REGION,
        script_args=script_args,
    )


def is_trading_session(calendar_name: str, session_date: str) -> bool:
    """True if `session_date` is a session on the given market calendar.
    Returning False short-circuits the branch (weekend/holiday), so the strict
    landing jobs never run on a day with no data to fetch. NOTE: the parameter
    is NOT named `ds` — that collides with Airflow's reserved context var and
    raises 'key ds is a part of kwargs and therefore reserved'."""
    calendar = mcal.get_calendar(calendar_name)
    return not calendar.valid_days(start_date=session_date, end_date=session_date).empty


@dag(
    dag_id="financial_data_lakehouse",
    description="Daily landing -> bronze -> silver -> DQ -> dbt gold",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "glue", "dbt"],
    doc_md=DOC_MD,
)
def financial_data_lakehouse():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # dbt runs in its own container (isolates the prerelease dbt deps from
    # Airflow's). Image is built from dbt_modeling/ in the runtime step; the
    # host bind-mount paths + AWS creds are wired in docker-compose.
    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="financial-data-lakehouse-dbt",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        # Branches that short-circuit on a non-session day land as *skipped*, not
        # failed. Run the gold build as long as nothing FAILED and at least one
        # source produced data (binance runs daily, so this always holds). A real
        # DQ failure still blocks dbt — the gate is preserved.
        trigger_rule="none_failed_min_one_success",
        environment={"GOLD_BUCKET": GOLD_BUCKET, "AWS_REGION": REGION},
        # Source freshness runs first but is NON-BLOCKING: the `;` lets the build
        # run regardless of its exit code. Freshness is warn-only for the gated
        # sources (a stale equity/FRED source on a weekend shouldn't block gold),
        # and the build's own tests are the hard gate. `dbt build` then runs the
        # coverage seed + models + all data tests (recency/coverage) in one step.
        command=[
            "bash", "-c",
            "dbt source freshness "
            "--project-dir /usr/app/dbt_modeling "
            "--profiles-dir /usr/app/dbt_modeling; "
            "dbt build "
            "--project-dir /usr/app/dbt_modeling "
            "--profiles-dir /usr/app/dbt_modeling",
        ],
        mounts=[
            # Host paths come from docker-compose env (a sibling container runs on
            # the host daemon, so these must be host paths). Read via os.environ —
            # docker.types.Mount sends source straight to the Docker API with no
            # shell, so a literal "${DBT_PROJECT_DIR}" would NOT expand.
            Mount(source=os.environ["DBT_PROJECT_DIR"], target="/usr/app/dbt_modeling", type="bind"),
            Mount(source=os.environ["AWS_CREDS_DIR"], target="/root/.aws", type="bind", read_only=True),
        ],
    )

    for source, cfg in SOURCES.items():
        landing = glue_job(f"landing_{source}", f"{PROJECT}-landing-{source}", DATE_ARGS)
        bronze = glue_job(f"bronze_{source}", f"{PROJECT}-bronze-{source}", DATE_ARGS)
        transform = glue_job(
            f"transform_{source}",
            f"{PROJECT}-transform-{source}",
            {"--ingest_date": "{{ ds }}"},
        )
        dq = GlueDataQualityRuleSetEvaluationRunOperator(
            task_id=f"dq_{source}",
            role=GLUE_ROLE_ARN,
            rule_set_names=[cfg["ruleset"]],
            number_of_workers=2,
            wait_for_completion=True,
            region_name=REGION,
            retries = 0,
            datasource={
                "GlueTable": {"DatabaseName": SILVER_DB, "TableName": cfg["table"]}
            },
        )

        # Gate sources with a market calendar; Binance (24/7) wires straight in.
        # ignore_downstream_trigger_rules=False so the short-circuit skips only
        # this branch (landing..dq) while dbt_build still evaluates its own rule.
        if cfg["calendar"]:
            gate = ShortCircuitOperator(
                task_id=f"session_gate_{source}",
                python_callable=is_trading_session,
                op_args=[cfg["calendar"], "{{ ds }}"],
                ignore_downstream_trigger_rules=False,
            )
            start >> gate >> landing
        else:
            start >> landing

        # DQ gates dbt: dbt_build waits for every running source's dq to pass.
        landing >> bronze >> transform >> dq >> dbt_build

    dbt_build >> end


financial_data_lakehouse()
