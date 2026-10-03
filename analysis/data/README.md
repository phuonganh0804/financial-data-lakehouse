# Dashboard data

CSV exports from Athena (gold layer) for the Tableau dashboard. The CSVs are
**gitignored**: they're local snapshots. Regenerate them by running the matching
query in [`../sql/`](../sql/) in Athena and downloading the result as CSV.

| File | Rows | Grain | Source query |
|---|---|---|---|
| `mart_returns_vs_macro.csv` | 20,198 | asset × day, 2020-01 → 2026-10 | `select *` from the gold mart |
| `growth_of_100.csv` | 17,176 | asset × day, from 2021-01-01 | `06_export_growth_of_100.sql` |
| `rolling_correlation_26w.csv` | 275 | week | `07_export_rolling_correlation.sql` |
| `asset_summary.csv` | 11 | asset | `01_risk_adjusted_performance.sql` |
| `stocks_vs_qqq.csv` | 8 | stock | `02_stocks_vs_benchmark.sql` |
| `rate_regimes.csv` | 15 | regime × asset group | `03_rate_regimes.sql` |

`is_benchmark = true` marks QQQ. Exclude it from "equity" averages and compare
against it instead.
