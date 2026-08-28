variable "region" {
  description = "AWS region for the benchmark environment."
  type        = string
  default     = "us-east-1"
}

variable "az" {
  description = "Availability zone. All instances are placed in a single AZ for stable inter-node latency."
  type        = string
  default     = "us-east-1a"
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
  description = "EC2 key pair name for SSH access. Empty disables SSH (use SSM Session Manager instead)."
  type        = string
  default     = ""
}

variable "ssh_cidr" {
  description = "CIDR allowed to reach SSH (and the gateway HTTP port) from outside the VPC."
  type        = string
  default     = "0.0.0.0/0"
}

variable "s3_bucket" {
  description = "Name of the private S3 bucket holding the runtime tarball. May be pre-created."
  type        = string
}

variable "s3_key" {
  description = "Object key of the runtime tarball inside the bucket."
  type        = string
  default     = "excoredum-runtime.tgz"
}

variable "workload_ops" {
  description = "Number of main-loop commands per load run (ExternalLoadRunner / ReadVerifyRunner)."
  type        = number
  default     = 100000
}

variable "workload_users" {
  description = "Number of users in the deterministic workload."
  type        = number
  default     = 100
}

variable "load_client_id" {
  description = "Write client id for the load generator (unique per concurrent runner)."
  type        = number
  default     = 1
}

variable "metrics_interval_ms" {
  description = "Cluster node CoreMetrics dump interval in ms (0 disables)."
  type        = number
  default     = 5000
}

variable "gateway_symbols" {
  description = "gateway.symbols value; must list symbol 1 with currencies 10/20 for the workload."
  type        = string
  default     = "1|BTC/USDT|10|20|1|1|0|0"
}

variable "gateway_currencies" {
  description = "gateway.currencies value; must list currencies 10 and 20 for the workload."
  type        = string
  default     = "10|BTC|1,20|USDT|1"
}
