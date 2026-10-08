############ Data Sources ############
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

############ IAM Role & Instance Profile ############
resource "aws_iam_role" "ingestion" {
  name = "sunat-ingestion-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "ingestion_s3" {
  name        = "sunat-ingestion-ec2-s3-policy"
  description = "Permisos para sincronizar datos raw hacia S3 desde la EC2 efimera"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          data.aws_s3_bucket.raw.arn,
          "${data.aws_s3_bucket.raw.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ingestion" {
  role       = aws_iam_role.ingestion.name
  policy_arn = aws_iam_policy.ingestion_s3.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_ssm" {
  role       = aws_iam_role.ingestion.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ingestion" {
  name = "sunat-ingestion-ec2-profile"
  role = aws_iam_role.ingestion.name
}

############ Security Group (Sin puertos de entrada abiertos) ############
resource "aws_security_group" "ingestion" {
  name        = "sunat-ingestion-ec2-sg"
  description = "SG efimero: solo egress HTTPS a internet, sin puertos de entrada"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description      = "Salida libre a internet para descargas y S3"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

############ Bootstrap Files in S3 (Evita exceder límite de 16KB en User Data) ############
resource "aws_s3_object" "bootstrap_urls" {
  bucket = var.raw_bucket_name
  key    = "_bootstrap/urls.json"
  source = "${path.module}/urls.json"
  etag   = filemd5("${path.module}/urls.json")
}

resource "aws_s3_object" "bootstrap_script" {
  bucket = var.raw_bucket_name
  key    = "_bootstrap/process_and_upload.py"
  source = "${path.module}/scripts/process_and_upload.py"
  etag   = filemd5("${path.module}/scripts/process_and_upload.py")
}

############ Instancia EC2 Efímera ############
resource "aws_instance" "ingestion_worker" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = tolist(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids      = [aws_security_group.ingestion.id]
  iam_instance_profile        = aws_iam_instance_profile.ingestion.name
  associate_public_ip_address = true # IP pública efímera dinámica (desaparece al destruirse la EC2)

  depends_on = [
    aws_s3_object.bootstrap_urls,
    aws_s3_object.bootstrap_script,
    aws_iam_role_policy_attachment.ingestion
  ]

  root_block_device {
    volume_size           = var.ebs_volume_size
    volume_type           = "gp3"
    delete_on_termination = true # CERO discos huérfanos
    encrypted             = true
  }

  user_data = <<-EOF
    #!/usr/bin/env bash
    set -euxo pipefail

    exec > >(tee -a /var/log/ingestion_user_data.log) 2>&1

    # Asegurar que las bitácoras se sincronicen a S3 en caso de salida o fallo
    upload_early_logs() {
      aws s3 cp /var/log/ingestion_user_data.log "s3://${var.raw_bucket_name}/_ingestion_user_data.log" || true
      if [ -f /var/log/ingestion_process.log ]; then
        aws s3 cp /var/log/ingestion_process.log "s3://${var.raw_bucket_name}/_ingestion_process.log" || true
      fi
    }
    trap upload_early_logs EXIT

    echo "=== [1/4] Preparando dependencias en la EC2 ==="
    apt-get update -y
    apt-get install -y python3-pip python3-pandas python3-openpyxl unzip curl awscli

    mkdir -p /opt/ingestion /data/tmp

    echo "=== [2/4] Descargando configuración y scripts desde S3 ==="
    aws s3 cp "s3://${var.raw_bucket_name}/_bootstrap/urls.json" /opt/ingestion/urls.json
    aws s3 cp "s3://${var.raw_bucket_name}/_bootstrap/process_and_upload.py" /opt/ingestion/process_and_upload.py
    chmod +x /opt/ingestion/process_and_upload.py

    export RAW_BUCKET="${var.raw_bucket_name}"
    export AWS_REGION="${var.aws_region}"

    echo "=== [3/4] Ejecutando procesamiento y subida ==="
    python3 /opt/ingestion/process_and_upload.py 2>&1 | tee /var/log/ingestion_process.log

    echo "=== [4/4] Subiendo bitácoras finales a S3 ==="
    aws s3 cp /var/log/ingestion_user_data.log "s3://${var.raw_bucket_name}/_ingestion_user_data.log" || true
    aws s3 cp /var/log/ingestion_process.log "s3://${var.raw_bucket_name}/_ingestion_process.log" || true

    echo "=== Proceso finalizado en la EC2 ==="
  EOF

  tags = {
    Name = "sunat-ingestion-worker"
  }
}
