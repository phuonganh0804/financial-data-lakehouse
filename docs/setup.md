# Setup & operations

How to deploy, backfill and run the pipeline, and how daily runs and backfills
fit together. Back to the [README](../README.md).

## Prerequisites
- AWS credentials with permission to create Glue, S3, Athena, Glue Data Catalog, IAM, and SSM resources.
- Terraform, Docker (for Airflow + dbt), AWS CLI.
- Config files are gitignored (account/run-specific) - copy each from its committed `.example`: `terraform/backend.hcl` (S3 state backend), `terraform/terraform.tfvars` (run dates + interval), and `airflow/.env` (Terraform outputs).

Store both API keys in SSM (read at runtime by the landing jobs - never committed):
```bash
for src in fred twelvedata; do
  read -s -p "$src API key: " K && echo
  aws ssm put-parameter --region eu-central-1 --type SecureString \
    --name "/financial-data-lakehouse/$src-api-key" --value "$K"
done; unset K
```
`read -s` keeps the keys out of your terminal and shell history. Binance needs no key.

## 1. Deploy infrastructure
```bash
cd terraform
cp backend.hcl.example backend.hcl            # Terraform state S3 bucket
cp terraform.tfvars.example terraform.tfvars  # run dates + interval
terraform init -backend-config=backend.hcl
terraform apply
```
Creates the Glue jobs, S3 buckets, Iceberg catalog, DQ rulesets, and Athena
workgroup, and uploads the job scripts. **All job IAM roles and policies are
provisioned here - no manual IAM setup.**

## 2. Backfill history (optional, one-off)
Airflow runs with `catchup=False`, so it only ingests **forward from now**. To load
history - a fresh deploy, or a newly added series/symbol - run the bulk backfill
(landing → bronze → transform per source over a wide date range, **outside**
Airflow):
```bash
chmod +x scripts/backfill.sh
./scripts/backfill.sh 2020-01-01 2026-10-02          # all sources
./scripts/backfill.sh 2020-01-01 2026-10-02 fred     # one source
```
2020 is a warm-up year: `cpi_yoy` needs the CPI print from 12 months earlier,
so the mart is complete from 2021, which still covers a full rate cycle (zero
rates → 2022–23 hikes → 2024–25 cuts).
This is far cheaper and faster than an Airflow catch-up: **one wide-range Glue job
per stage** instead of ~one DAG run per day (which would be hundreds of job
invocations paying Spark startup over and over). Skip it if you only need data
going forward.

## 3. Start orchestration
```bash
cd airflow
cp .env.example .env        # fill in Terraform outputs: bucket names, role ARN, keys
docker compose up -d
# open http://localhost:8080 and unpause the `financial_data_lakehouse` DAG
```
The containers authenticate to AWS via your host's `~/.aws` credentials
(bind-mounted in). From here the DAG runs daily, ingesting each new session forward.

## History vs. incremental

Two distinct mechanisms keep the lakehouse current, and they never conflict,
because silver `MERGE`s on natural keys (both paths converge to one row per
entity/date):

- **Incremental (ongoing)** - the Airflow DAG, daily. Each run fetches a single
  `ds` (crypto/equities) or a cadence-sized **lookback window** (FRED, so the
  latest late-released observation is always captured). This is the steady state.
- **Backfill (history)** - `scripts/backfill.sh`, on demand. Fetches a wide
  `[api_start_date, api_end_date]` range in one shot. The whole range lands under
  one `ingest_date` batch, labelled with the day the backfill ran; the real time
  axis is each row's `date` column.

Landing and bronze are organised by **when data was fetched** (`ingest_date`,
`run_id`); silver and gold by **what date it describes**. Gold models are full
rebuilds (`materialized: table`), so rolling metrics (30-day volatility, `lag()`
returns, macro forward-fill) and FRED revisions stay correct across the seam
between backfilled and daily rows. Days the DAG misses (laptop off, failed run)
are not caught up automatically - the dbt recency/coverage tests flag them, and
a short `backfill.sh` over the gap fills them; overlaps are harmless.

Trading-calendar gates (NASDAQ for equities, SIFMA for FRED) skip weekends and
market holidays automatically, so the strict landing jobs only run when there is
data to fetch.
