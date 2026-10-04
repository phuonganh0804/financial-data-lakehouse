# Data quality & lineage

How the pipeline keeps data correct, and how any reported number can be traced
back to its source. Back to the [README](../README.md).

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
