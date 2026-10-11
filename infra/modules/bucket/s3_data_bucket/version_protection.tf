# Con protect_versions ninguna identidad puede borrar versiones de forma
# permanente: un DeleteObject solo agrega un marcador de borrado y la versión
# anterior sigue recuperable. Las reglas de lifecycle no pasan por esta política.
data "aws_iam_policy_document" "protect_versions" {
  count = var.protect_versions ? 1 : 0

  statement {
    sid     = "DenyPermanentVersionDeletion"
    effect  = "Deny"
    actions = ["s3:DeleteObjectVersion"]

    resources = ["${aws_s3_bucket.this.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }
}

resource "aws_s3_bucket_policy" "protect_versions" {
  count  = var.protect_versions ? 1 : 0
  bucket = aws_s3_bucket.this.id
  policy = data.aws_iam_policy_document.protect_versions[0].json

  depends_on = [aws_s3_bucket_public_access_block.this]
}
