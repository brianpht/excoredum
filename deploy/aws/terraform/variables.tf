variable "region" {
  description = "AWS region for the benchmark environment."
  type        = string
  default     = "ap-southeast-1"
}

variable "az" {
  description = "Availability zone. All instances are placed in a single AZ for stable inter-node latency."
  type        = string
  default     = "ap-southeast-1a"
}

variable "subnet_cidr" {
  description = "CIDR of the single benchmark subnet. Static private IPs are derived from it."
  type        = string
  default     = "10.0.0.0/24"
}

variable "cluster_node_count" {
  description = "Number of Aeron Raft cluster nodes (odd: 3 or 5)."
  type        = number
  default     = 3

  validation {
    condition     = var.cluster_node_count == 3 || var.cluster_node_count == 5
    error_message = "cluster_node_count must be 3 or 5 (odd, for Raft quorum)."
  }
}

variable "node_instance_type" {
  description = "EC2 instance type for cluster nodes."
  type        = string
  default     = "c6i.xlarge"
}

variable "app_instance_type" {
  description = "EC2 instance type for the read replica, gateway, load generator, and verifier."
  type        = string
  default     = "c6i.large"
}

variable "key_name" {
  description = "EC2 key pair name for SSH access (used by Ansible)."
  type        = string
  default     = ""
}

variable "ssh_cidr" {
  description = "CIDR allowed to reach SSH (and the gateway HTTP port) from outside the VPC."
  type        = string
  default     = "0.0.0.0/0"
}
