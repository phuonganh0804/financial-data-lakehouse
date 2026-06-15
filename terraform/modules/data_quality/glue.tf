# Glue Data Quality rulesets (DQDL) for the silver Iceberg tables.
#
# These are STANDALONE rulesets (no target_table): a ruleset only *defines* the
# checks. The table is bound at evaluation time by the EvaluateDataQuality run
# (e.g. the Airflow GlueDataQualityRuleSetEvaluationRunOperator's datasource).
# This is required because the silver tables are created at runtime by the Spark
# jobs, not by Terraform — a baked target_table would fail apply with
# EntityNotFound. CustomSql's `primary` alias resolves to the run's datasource.
#
# Ruleset -> intended silver table (bound at run time):
#   binance_dq_ruleset    -> binance_klines
#   twelvedata_dq_ruleset -> equity_prices
#   fred_dq_ruleset       -> fred_macro

resource "aws_glue_data_quality_ruleset" "binance_dq_ruleset" {
  name    = "binance_dq_ruleset"
  ruleset = <<-EOT
    Rules = [
      RowCount > 0,
      IsComplete "open_time",
      IsComplete "symbol",
      IsComplete "open",
      IsComplete "high",
      IsComplete "low",
      IsComplete "close",
      IsComplete "volume",
      ColumnValues "open" > 0,
      ColumnValues "high" > 0,
      ColumnValues "low" > 0,
      ColumnValues "close" > 0,
      ColumnValues "volume" >= 0,
      ColumnValues "trade_count" >= 0,
      CustomSql "select count(*) from primary where high < low or high < open or high < close or low > open or low > close" = 0,
      CustomSql "select count(*) - count(distinct concat(symbol, '|', cast(open_time as string))) from primary" = 0
    ]
  EOT
}

resource "aws_glue_data_quality_ruleset" "twelvedata_dq_ruleset" {
  name    = "twelvedata_dq_ruleset"
  ruleset = <<-EOT
    Rules = [
      RowCount > 0,
      IsComplete "datetime",
      IsComplete "symbol",
      IsComplete "open",
      IsComplete "high",
      IsComplete "low",
      IsComplete "close",
      IsComplete "volume",
      ColumnValues "open" > 0,
      ColumnValues "high" > 0,
      ColumnValues "low" > 0,
      ColumnValues "close" > 0,
      ColumnValues "volume" >= 0,
      CustomSql "select count(*) from primary where high < low or high < open or high < close or low > open or low > close" = 0,
      CustomSql "select count(*) - count(distinct concat(symbol, '|', cast(datetime as string))) from primary" = 0
    ]
  EOT
}

resource "aws_glue_data_quality_ruleset" "fred_dq_ruleset" {
  name    = "fred_dq_ruleset"
  ruleset = <<-EOT
    Rules = [
      RowCount > 0,
      IsComplete "date",
      IsComplete "series_id",
      IsComplete "series_name",
      IsComplete "frequency",
      IsComplete "unit",
      IsComplete "value",
      ColumnValues "frequency" in ["daily", "monthly", "quarterly"],
      CustomSql "select count(*) - count(distinct concat(series_id, '|', cast(date as string))) from primary" = 0
    ]
  EOT
}
