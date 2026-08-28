output "node_private_ips" {
  description = "Private IPs of the cluster nodes."
  value       = local.node_private_ips
}

output "read_ip" {
  description = "Read replica private IP."
  value       = local.read_ip
}

output "gateway_ip" {
  description = "Gateway private IP."
  value       = local.gateway_ip
}

output "load_ip" {
  description = "Load generator private IP."
  value       = local.load_ip
}

output "verify_ip" {
  description = "Read verifier private IP."
  value       = local.verify_ip
}

output "gateway_url" {
  description = "Gateway HTTP endpoint (reachable in-VPC)."
  value       = "http://${local.gateway_ip}:8080"
}

output "cluster_members" {
  description = "Aeron cluster members string (for debugging)."
  value       = local.cluster_members
}

output "artifact_s3_url" {
  description = "S3 URL the runtime tarball is expected at."
  value       = "s3://${var.s3_bucket}/${var.s3_key}"
}
