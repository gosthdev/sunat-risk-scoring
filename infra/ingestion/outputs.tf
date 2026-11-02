output "instance_id" {
  value       = aws_instance.ingestion_worker.id
  description = "ID de la instancia EC2 efímera."
}

output "instance_public_ip" {
  value       = aws_instance.ingestion_worker.public_ip
  description = "IP pública efímera dinámica asignada a la instancia."
}

output "raw_bucket_name" {
  value       = var.raw_bucket_name
  description = "Nombre del bucket S3 de destino."
}
