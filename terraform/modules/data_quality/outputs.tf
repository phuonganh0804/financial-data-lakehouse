output "binance_dq_ruleset" {
  value = aws_glue_data_quality_ruleset.binance_dq_ruleset.name
}

output "twelvedata_dq_ruleset" {
  value = aws_glue_data_quality_ruleset.twelvedata_dq_ruleset.name
}

output "fred_dq_ruleset" {
  value = aws_glue_data_quality_ruleset.fred_dq_ruleset.name
}