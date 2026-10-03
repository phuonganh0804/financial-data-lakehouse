# financial-data-lakehouse

[![ci](https://github.com/phuonganh0804/financial-data-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/phuonganh0804/financial-data-lakehouse/actions/workflows/ci.yml)

A medallion-architecture data lakehouse for financial market data on AWS. Crypto
(Binance), equities (Twelve Data), and macro (FRED) flow through
`landing → bronze → silver → data quality → dbt gold`, orchestrated by Airflow and
provisioned with Terraform.

## Purpose

Financial analysts and quantitative researchers often spend significant effort collecting and reconciling market and macroeconomic data. This platform automates ingestion, validation, and transformation into analytics-ready datasets for risk analysis and quantitative research.

## Architecture

```mermaid
flowchart LR
  B[Binance] --> L
  T[Twelve Data] --> L
  F[FRED] --> L
  L["Landing<br/>raw JSON, immutable"] --> Z["Bronze<br/>Parquet"]
  Z --> S["Silver<br/>Iceberg MERGE"]
  S --> DQ{"Glue Data Quality"}
  DQ -->|pass| G["Gold<br/>dbt star schema"]
  G --> Q[("Athena queries")]
```

*Terraform provisions every AWS resource **and** the Glue job scripts; Airflow orchestrates `landing → bronze → silver → DQ → dbt` daily, per source.*

| Layer | Tech | Purpose |
|---|---|---|
| **Landing** | Glue Python Shell | Raw API responses, byte-for-byte, immutable (append-only by `run_id`) |
| **Bronze** | Glue Spark | Parsed/structured to columnar Parquet, partitioned by `ingest_date` |
| **Silver** | Glue Spark + Iceberg | Typed, deduped, `MERGE` on natural keys `(entity, date)`; partitioned by year; row-level lineage back to landing |
| **Data quality** | Glue Data Quality (DQDL) | Row-level rules that gate the gold build |
| **Gold** | dbt-athena | Star-schema marts (e.g. returns vs. macro) |
| **Orchestration** | Airflow | Daily `landing → bronze → silver → DQ → dbt` per source |
| **IaC** | Terraform | All AWS resources **and** the Glue job scripts |

The Airflow DAG that runs it - one branch per source, with trading-calendar
**session gates** up front and a **Glue DQ → `dbt build`** gate before gold:

![Airflow DAG: per-source landing → bronze → silver → data quality → dbt build, gated by trading-calendar session checks and Glue Data Quality](docs/images/airflow_dag.png)

## Data model (gold)

dbt builds a **star schema** in Athena (`financial_data_lakehouse_gold`):

- **Dimensions** - `dim_date` (calendar), `dim_symbol` (`(exchange, symbol)` → `symbol_key`, `asset_class ∈ {crypto, equity}`, `is_benchmark` flags QQQ), `dim_series` (FRED series reference: name, frequency, unit).
- **Facts** - `fct_daily_prices` (OHLCV, one instrument/day), `fct_returns` (daily & log returns + 30-day rolling volatility), `fct_macro` (one FRED series/day). Each fact carries a surrogate key and `relationships` (foreign-key) tests back to its dimensions.
- **Report mart** - `mart_returns_vs_macro`: every asset's daily return aligned with **forward-filled** daily macro (fed funds, 10Y yield, 10Y inflation expectation, CPI, real GDP), plus a derived YoY inflation (`cpi_yoy`) and an approximate inflation-adjusted `real_daily_return`. Monthly CPI and quarterly GDP are carried forward to every trading day so low-frequency macro lines up with daily returns.

```mermaid
flowchart LR
  sb[stg_binance_klines] --> idp[int_daily_prices]
  se[stg_equity_prices] --> idp
  sf[stg_fred_macro] --> imd[int_macro_daily]
  sf --> fm[fct_macro]
  idp --> fdp[fct_daily_prices]
  fdp --> fr[fct_returns]
  fr --> mart[mart_returns_vs_macro]
  imd --> mart
```

### Example query

```sql
-- Real (inflation-adjusted) daily returns by asset class for 2025,
-- against the prevailing macro backdrop.
select
    asset_class,
    count(*)                               as obs,
    round(avg(daily_return)      * 100, 4) as avg_daily_return_pct,
    round(avg(real_daily_return) * 100, 4) as avg_real_return_pct,
    round(avg(volatility_30d)    * 100, 2) as avg_30d_vol_pct,
    round(avg(fed_funds_rate),   2)        as avg_fed_funds,
    round(avg(cpi_yoy)           * 100, 2) as avg_cpi_yoy_pct
from financial_data_lakehouse_gold.mart_returns_vs_macro
where date_day >= date '2025-01-01' and date_day < date '2026-01-01'
  and not is_benchmark                    -- QQQ is a yardstick, not part of 'equity'
group by asset_class
order by asset_class;
```

| asset_class | obs | avg_daily_return_pct | avg_real_return_pct | avg_30d_vol_pct | avg_fed_funds | avg_cpi_yoy_pct |
|---|---|---|---|---|---|---|
| crypto | 730 | 0.025 | 0.0177 | 3.0 | 4.21 | 2.69 |
| equity | 2000 | 0.0859 | 0.0786 | 2.29 | 4.21 | 2.69 |


## Setup

### Prerequisites
- AWS credentials with permission to create Glue, S3, Athena, Glue Data Catalog, IAM, and SSM resources.
- Terraform, Docker (for Airflow + dbt), AWS CLI.
- Config files are gitignored (account/run-specific) - copy each from its committed `.example`: `terraform/backend.hcl` (S3 state backend), `terraform/terraform.tfvars` (run dates + interval), and `airflow/.env` (Terraform outputs).

Store the FRED API key in SSM (read at runtime by the landing job - never committed):
```bash
aws ssm put-parameter \
  --name /financial-data-lakehouse/fred-api-key \
  --type SecureString \
  --value "<your-fred-api-key>" \
  --region eu-central-1
```

### 1. Deploy infrastructure
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

### 2. Backfill history (optional, one-off)
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

### 3. Start orchestration
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

## Quality & testing

Quality is enforced at three levels - pre-merge, runtime, and analytics:

1. **CI - static checks on every push/PR** (`.github/workflows/ci.yml`, credential-free, no AWS needed):
   - `ruff` (real-error rules) + `py_compile` across all Glue/Airflow Python;
   - **seed-drift** - regenerates the dbt coverage seed from the Terraform configs and fails if it diverges from what's committed;
   - `terraform fmt -check` + `validate` (`-backend=false`);
   - `dbt parse` against the committed profile - builds the manifest and catches model/macro/schema errors offline.
2. **Glue Data Quality - runtime row rules that gate the gold build.** Each silver table has a DQDL ruleset (`binance_dq_ruleset`, `twelvedata_dq_ruleset`, `fred_dq_ruleset`) asserting `RowCount > 0`, completeness (`IsComplete` on keys/OHLCV), and validity (`ColumnValues "close" > 0`, `volume >= 0`). The DAG runs these after silver and **only triggers `dbt build` if every source's checks pass** - bad data never reaches gold.
3. **dbt - the analytics layer.** A schema contract (`not_null`/`unique` on surrogate keys, `relationships` foreign-key integrity from every fact to its dimensions, `accepted_values` on `asset_class`) **plus source freshness and custom recency & coverage assertions** - all run in `dbt build`.

**Why four data-quality checks?** They aren't a list - they're a **closed loop**, where each one covers the blind spot of the one before it:

- **Validity** (Glue DQ) - the rows present are correct → *but maybe nothing fresh landed.*
- **Freshness** (dbt source freshness) - something landed recently → *but maybe one entity silently stalled.*
- **Recency** (custom dbt check) - each **present** entity is current → *but maybe one is entirely absent.*
- **Coverage** (custom dbt check) - each **expected** entity exists → *closing the loop back to "…and Glue DQ says those rows are valid."*

In one line: **validity → liveness → per-entity timeliness → per-entity existence**.

## Lineage & auditability

Every silver row can be traced back to the exact raw API response it came from:

| Column | Answers |
|---|---|
| `source` | Which provider? |
| `ingest_date` | Which batch? |
| `run_id` | Which fetch? (UTC timestamp + random suffix) |
| `source_file` | Which raw file in landing? |
| `first_loaded_at` | When did the row first appear? Kept across `MERGE` updates |
| `transformed_at` | When was it last written? |
| `price_adjustment` | How were equity prices adjusted? (`splits`) |

```sql
select symbol, date, close, run_id, source_file
from financial_data_lakehouse_silver.equity_prices
where symbol = 'NVDA' and date = date '2024-06-10';
-- source_file = s3://…-landing-…/equity_prices/…/symbol=NVDA/ingest_date=…/run_id=…/response.json
```

Three layers make values auditable: the **immutable, versioned landing zone**
(raw payloads, never overwritten), these **row-level lineage columns**, and
**Iceberg snapshots**, which let you query a silver table as it was at an earlier
point (`FOR TIMESTAMP AS OF …`, `"<table>$history"`). Snapshot expiry (`VACUUM`)
shortens that history, so its retention is a deliberate audit-vs-cost choice.

## Design decisions & known limitations

- **Equity prices are split-adjusted** (`adjust=splits` on Twelve Data). With raw
  prices, a 10-for-1 split shows up as a −90% "return" (NVDA 2024-06-10, AMZN and
  GOOGL 2022, AAPL and TSLA 2020) and distorts volatility and every average built
  on it. *Limitation:* daily runs fetch one day, so a future split leaves older rows
  on the old scale. Re-run the Twelve Data backfill after a split; a query for
  `|daily_return| > 30%` catches it.
- **Silver is partitioned by year, not by `(date, symbol)`.** Daily data is tiny,
  so per-day partitions meant one file per row: 27,472 files for 13.5k equity rows,
  and a `MERGE` that hit the 10-minute Glue timeout. Yearly partitions hold ~2k rows
  each (28 files in total), the same write takes ~1.5 minutes, and a daily `MERGE`
  touches only the current year. Rows arrive unsorted after the dedup shuffle, so
  the tables use Iceberg's fanout writer (`write.spark.fanout.enabled`).
- **Newest landing run wins.** Re-runs and backfills can put several runs under one
  `ingest_date`. Bronze keeps the row from the latest `run_id` per key (it starts with
  a UTC timestamp, so it sorts by time) instead of an arbitrary one, so a corrected
  re-fetch always replaces the earlier version.
- **Small files still build up from daily runs.** Each daily `MERGE` adds a few small
  files to the current-year partition. Compact them periodically in Athena:
  `OPTIMIZE <table> REWRITE DATA USING BIN_PACK` and `VACUUM <table>`.
- **Two IAM grants are broader than they need to be:** `kms:Decrypt` on `*`, and
  `glue:*DataQuality*`. The next step is to scope KMS to the SSM key's ARN and list
  the Data Quality actions explicitly.
- **Twelve Data landing is intentionally not paginated.** The `time_series`
  endpoint caps each response at `outputsize` (max **5000 rows**, not a time span).
  The 2020→2026 backfill is ~1,700 daily bars per symbol, well under the cap, so a
  single request is complete. Pagination is deliberately skipped: it would be
  untestable against the free tier's limited history depth, and `landing_binance.py`
  (a hard 1000-rows/request cap) already demonstrates the paging pattern. To go
  deeper later, either add date-windowed pagination (fetch newest-first, advance
  `end_date` until a page returns `< outputsize`) or switch to a coarser interval
  (monthly ≈ 416 years per 5000 points) if low-frequency grain is acceptable.
