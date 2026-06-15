# Gold layer — dbt + Athena

dbt project that models the Iceberg **silver** tables into a **gold** star schema,
served by Amazon Athena. This is the serving layer of the financial-data-lakehouse
medallion pipeline (landing → bronze → silver → **gold**).

## Layering

- `models/staging/` — thin **views** over the silver Iceberg tables (rename, type,
  tag `asset_class`). One staging model per source.
- `models/intermediate/` — `int_daily_prices` (**ephemeral**): the single place the
  crypto + equity price sources are unioned. Reused by the fact and the dimension.
- `models/marts/` — the **star schema** (Hive/Parquet tables via CTAS):
  - Dimensions: `dim_date`, `dim_symbol`, `dim_series`
  - Facts: `fct_daily_prices`, `fct_returns`, `fct_macro`
  - Report mart: `mart_returns_vs_macro` (denormalised "so what" table)

### Star schema

```
       dim_date ─┐        ┌─ dim_symbol
                 ▼        ▼
            fct_daily_prices ─► fct_returns      (grain: instrument × day)

       dim_date ─┐   ┌─ dim_series
                 ▼   ▼
            fct_macro                            (grain: series × day)
```

Facts hold surrogate keys (`dbt_utils.generate_surrogate_key`) + measures;
dimension attributes are reached via the FKs. `relationships` tests enforce the
fact → dim integrity.

## Source / target databases

A dbt "schema" is a Glue "database":

- Reads from **`financial_data_lakehouse_silver`** (`var: silver_schema`, used by the sources).
- Writes to **`financial_data_lakehouse_gold`** (the profile's `schema`).

## Running

Needs **classic dbt-core + dbt-athena** — the Fusion engine (the global `dbt`
binary) does not yet ship the Athena adapter, so run this project's venv. Python
3.14 needs the prerelease line (see `requirements.txt`).

```bash
python3 -m venv .dbt-venv
.dbt-venv/bin/pip install --pre -r requirements.txt
source .dbt-venv/bin/activate          # shadows Fusion's dbt in this shell

# gold bucket is account/region-specific — read it from Terraform
export GOLD_BUCKET=$(terraform -chdir=../terraform output -raw gold_bucket_name)

dbt deps                                # install dbt_utils
dbt build --profiles-dir .             # run models + tests in dependency order
```

`dbt build` runs the models and the data-quality tests (grain uniqueness +
fact → dim relationships) together.
