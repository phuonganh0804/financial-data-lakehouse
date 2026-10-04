# Design decisions & known limitations

Why the pipeline is built the way it is, and what it doesn't handle yet.
Back to the [README](../README.md).

- **Equity prices are split-adjusted** (`adjust=splits` on Twelve Data). With raw
  prices, a 10-for-1 split shows up as a −90% "return" (NVDA 2024-06-10, AMZN and
  GOOGL 2022, AAPL and TSLA 2020) and distorts volatility and every average built
  on it. *Limitation:* daily runs fetch one day, so a future split leaves older rows
  on the old scale. Re-run the Twelve Data backfill after a split; a query for
  `|daily_return| > 30%` catches it.
- **Silver is partitioned by year, not by `(date, symbol)`.** Daily data is tiny,
  so per-day partitions meant one file per row: 27,472 files for 13.5k equity rows,
  and a `MERGE` that hit the 10-minute Glue timeout. Yearly partitions hold ~2k rows
  each (28 files in total), the same load takes ~2 minutes, and a daily `MERGE`
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
