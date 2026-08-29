output "node_private_ips" {
  description = "Private IPs of the cluster nodes."
  value       = local.node_private_ips
}

output "node_public_ips" {
  description = "Public IPs of the cluster nodes (Ansible SSH targets)."
  value       = aws_instance.node[*].public_ip
}

output "read_ip" {
  description = "Read replica private IP."
  value       = local.read_ip
}

output "read_public_ip" {
  description = "Read replica public IP (Ansible SSH target)."
  value       = aws_instance.read.public_ip
}

output "gateway_ip" {
  description = "Gateway private IP."
  value       = local.gateway_ip
}

output "gateway_public_ip" {
  description = "Gateway public IP (Ansible SSH target)."
  value       = aws_instance.gateway.public_ip
}

output "load_ip" {
  description = "Load generator private IP."
  value       = local.load_ip
}

output "load_public_ip" {
  description = "Load generator public IP (Ansible SSH target)."
  value       = aws_instance.load.public_ip
}

output "verify_ip" {
  description = "Read verifier private IP."
  value       = local.verify_ip
}

output "verify_public_ip" {
  description = "Read verifier public IP (Ansible SSH target)."
  value       = aws_instance.verify.public_ip
}

output "gateway_url" {
  description = "Gateway HTTP endpoint (reachable in-VPC)."
  value       = "http://${local.gateway_ip}:8080"
}

output "cluster_members" {
  description = "Aeron cluster members string (fed to Ansible)."
  value       = local.cluster_members
}

output "ingress_endpoints" {
  description = "Write client ingress endpoints (id=host:port,...) for the load generator and gateway."
  value       = local.ingress_endpoints
}

output "archive_channels" {
  description = "Comma-separated archive control channels followed by the read replica."
  value       = local.archive_channels
}

output "query_channel" {
  description = "Read-side query channel (read replica listen endpoint)."
  value       = local.query_channel
}
