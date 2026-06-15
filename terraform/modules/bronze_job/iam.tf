resource "aws_iam_role" "glue_role" {
  name               = "${var.project_name}-glue-bronze-role"
  assume_role_policy = data.aws_iam_policy_document.glue_base_policy.json

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_iam_role_policy" "glue_role_policy" {
  name   = "${var.project_name}-glue-bronze-policy"
  role   = aws_iam_role.glue_role.id
  policy = data.aws_iam_policy_document.glue_access_policy.json
}
