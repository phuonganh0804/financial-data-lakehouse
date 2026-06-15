# Glue Catalog database for the gold (serving) layer. dbt-athena materializes
# its marts here; Athena queries them via the workgroup below.
resource "aws_glue_catalog_database" "gold" {
  name        = var.gold_database
  description = "Gold serving layer — dbt-athena marts for ${var.project_name}"
}
