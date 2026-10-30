resource "aws_iam_role" "this" {
  name = var.role_name

  # Sesiones OIDC de hasta 6h (jobs largos de EMR/SageMaker superan 1h)
  max_session_duration = 21600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          # Solo jobs que declaran environment (ver workflows); el sub de ramas ya no basta.
          "token.actions.githubusercontent.com:sub" = [
            "repo:${var.github_repo}:environment:aws"
          ]
        }
      }
    }]
  })
}
