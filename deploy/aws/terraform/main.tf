# ---------------------------------------------------------------------------
# Infrastructure only. Provisioning (installing the runtime, writing config,
# and starting services) is delegated to Ansible, which pushes the runtime
# tarball over SSH. No IAM role / instance profile / S3 artifact store is
# needed here, because the instances do not pull from S3 at boot.
# ---------------------------------------------------------------------------

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

# ---------------------------------------------------------------------------
# Networking: one VPC, one public subnet, one AZ. Benchmark traffic is
# private-IP based; public IPs are only for SSH access from the Ansible
# controller.
# ---------------------------------------------------------------------------

resource "aws_vpc" "bench" {
  cidr_block           = var.subnet_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = { Name = "excoredum-bench" }
}

resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = var.subnet_cidr
  availability_zone       = var.az
  map_public_ip_on_launch = true

  tags = { Name = "excoredum-bench" }
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

# ---------------------------------------------------------------------------
# Cluster placement group: only the Raft nodes (same instance type) live here,
# for low, consistent inter-node commit latency.
# ---------------------------------------------------------------------------

resource "aws_placement_group" "bench" {
  name     = "excoredum-bench"
  strategy = "cluster"
}

# ---------------------------------------------------------------------------
# Security group: open UDP + Aeron TCP among members, SSH and gateway HTTP
# from the operator CIDR.
# ---------------------------------------------------------------------------

locals {
  tcp_max_port = 20100 + var.cluster_node_count * 100 - 97
}

resource "aws_security_group" "bench" {
  name        = "excoredum-bench"
  description = "excoredum benchmark environment"
  vpc_id      = aws_vpc.bench.id
}

# UDP all: Aeron ingress/consensus/archive use fixed ports, but replication and
# client egress bind ephemeral ports (host:0), so the whole range must be open.
resource "aws_security_group_rule" "udp_self" {
  type              = "ingress"
  from_port         = 0
  to_port           = 65535
  protocol          = "udp"
  self              = true
  security_group_id = aws_security_group.bench.id
}

# Aeron log/catchup TCP channels: node n uses 20102 + n*100 and 20103 + n*100.
resource "aws_security_group_rule" "tcp_log_catchup_self" {
  type              = "ingress"
  from_port         = 20102
  to_port           = local.tcp_max_port
  protocol          = "tcp"
  self              = true
  security_group_id = aws_security_group.bench.id
}

# Gateway HTTP reachable from any instance (GatewayBenchRunner runs in-VPC).
resource "aws_security_group_rule" "http_self" {
  type              = "ingress"
  from_port         = 8080
  to_port           = 8080
  protocol          = "tcp"
  self              = true
  security_group_id = aws_security_group.bench.id
}

resource "aws_security_group_rule" "ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = [var.ssh_cidr]
  security_group_id = aws_security_group.bench.id
}

resource "aws_security_group_rule" "http_public" {
  type              = "ingress"
  from_port         = 8080
  to_port           = 8080
  protocol          = "tcp"
  cidr_blocks       = [var.ssh_cidr]
  security_group_id = aws_security_group.bench.id
}

resource "aws_security_group_rule" "egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.bench.id
}

# ---------------------------------------------------------------------------
# Derived addressing (static private IPs so the cluster members string is
# deterministic before instances exist). These values feed the Ansible
# inventory via `terraform output -json`.
# ---------------------------------------------------------------------------

locals {
  node_private_ips = [for i in range(var.cluster_node_count) : cidrhost(var.subnet_cidr, 10 + i)]
  read_ip          = cidrhost(var.subnet_cidr, 20)
  gateway_ip       = cidrhost(var.subnet_cidr, 21)
  load_ip          = cidrhost(var.subnet_cidr, 30)
  verify_ip        = cidrhost(var.subnet_cidr, 31)

  member_entries = [for i in range(var.cluster_node_count) : format(
    "%d,%s:%d,%s:%d,%s:%d,%s:%d,%s:%d",
    i,
    local.node_private_ips[i], 20100 + i * 100,
    local.node_private_ips[i], 20100 + i * 100 + 1,
    local.node_private_ips[i], 20100 + i * 100 + 2,
    local.node_private_ips[i], 20100 + i * 100 + 3,
    local.node_private_ips[i], 20100 + i * 100 + 4,
  )]

  cluster_members = join("|", local.member_entries)

  ingress_endpoints = join(",", [for i in range(var.cluster_node_count) : format(
    "%d=%s:%d", i, local.node_private_ips[i], 20100 + i * 100,
  )])

  archive_channels = join(",", [for i in range(var.cluster_node_count) : format(
    "aeron:udp?endpoint=%s:%d", local.node_private_ips[i], 20100 + i * 100 + 4,
  )])

  query_channel = format("aeron:udp?endpoint=%s:44000", local.read_ip)
}

# ---------------------------------------------------------------------------
# Instances. Provisioning is done by Ansible after `terraform apply`.
# ---------------------------------------------------------------------------

resource "aws_instance" "node" {
  count = var.cluster_node_count

  ami                         = data.aws_ami.al2023.id
  instance_type               = var.node_instance_type
  subnet_id                   = aws_subnet.bench.id
  private_ip                  = local.node_private_ips[count.index]
  key_name                    = var.key_name == "" ? null : var.key_name
  vpc_security_group_ids      = [aws_security_group.bench.id]
  associate_public_ip_address = true
  placement_group             = aws_placement_group.bench.name

  tags = { Name = "excoredum-node-${count.index}" }
}

resource "aws_instance" "read" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.app_instance_type
  subnet_id                   = aws_subnet.bench.id
  private_ip                  = local.read_ip
  key_name                    = var.key_name == "" ? null : var.key_name
  vpc_security_group_ids      = [aws_security_group.bench.id]
  associate_public_ip_address = true

  tags = { Name = "excoredum-read" }
}

resource "aws_instance" "gateway" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.app_instance_type
  subnet_id                   = aws_subnet.bench.id
  private_ip                  = local.gateway_ip
  key_name                    = var.key_name == "" ? null : var.key_name
  vpc_security_group_ids      = [aws_security_group.bench.id]
  associate_public_ip_address = true

  tags = { Name = "excoredum-gateway" }
}

resource "aws_instance" "load" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.app_instance_type
  subnet_id                   = aws_subnet.bench.id
  private_ip                  = local.load_ip
  key_name                    = var.key_name == "" ? null : var.key_name
  vpc_security_group_ids      = [aws_security_group.bench.id]
  associate_public_ip_address = true

  tags = { Name = "excoredum-load" }
}

resource "aws_instance" "verify" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.app_instance_type
  subnet_id                   = aws_subnet.bench.id
  private_ip                  = local.verify_ip
  key_name                    = var.key_name == "" ? null : var.key_name
  vpc_security_group_ids      = [aws_security_group.bench.id]
  associate_public_ip_address = true

  tags = { Name = "excoredum-verify" }
}
