# AWS deployment for load testing and benchmarking

This directory provisions a throwaway AWS environment that runs the deployed
benchmark modes against a real multi-node cluster:

- **Distributed cluster load test** - `ExternalLoadRunner` drives the
  deterministic `LoadWorkload` through the write client SDK and reports
  throughput, latency tails, and session stats.
- **Gateway HTTP/WS end-to-end** - `GatewayBenchRunner` drives the same
  workload through the REST/WebSocket gateway and cross-checks the replicated
  state on every read endpoint.

Infrastructure (network, instances, security group) is created by Terraform.
Provisioning (installing the runtime, writing config, starting services) is
done by Ansible over SSH, which pushes the runtime tarball directly - no S3
bucket and no IAM role / instance profile are required.

Topology (single AZ, `ap-southeast-1a` by default):

| Role | Count | Instance (default) | Placement group |
|------|-------|--------------------|-----------------|
| Aeron Raft cluster node | 3 | `c6i.xlarge` | cluster |
| CQRS read replica | 1 | `c6i.large` | - |
| HTTP/WS gateway | 1 | `c6i.large` | - |
| Load generator (`ExternalLoadRunner`) | 1 | `c6i.large` | - |
| Read verifier (`ReadVerifyRunner`) | 1 | `c6i.large` | - |

The cluster nodes are in a cluster placement group for low, consistent
inter-node commit latency. Clients (load, verify, gateway, read) are outside
it because a cluster placement group accepts a single instance type.

See [SCALING.md](SCALING.md) for an analysis of how to scale commands, users,
symbols, and instance sizes for a larger benchmark.

## Prerequisites

- Terraform `>= 1.5`, AWS CLI with credentials, JDK 21 (to build), `curl`,
  `tar`, `python3`.
- Ansible (install with `pip install ansible` or `pipx install ansible`; if it
  lands in `~/.local/bin`, add that directory to `PATH`).
- An EC2 key pair for SSH (Ansible uses `~/.ssh/excoredum-bench` by default).

## 1. Build the runtime tarball

```bash
./deploy/aws/build-artifacts.sh
```

This builds `deploy/aws/excoredum-runtime.tgz` (Temurin 21 JRE + the four
distributions + `bin/` entrypoints). Pin the JRE for reproducibility with
`JRE_URL` (default is the Adoptium "latest" 21 JRE for linux/x64):

```bash
JRE_URL=https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.5%2B11/OpenJDK21U-jre_x64_linux_hotspot_21.0.5_11.tar.gz \
  ./deploy/aws/build-artifacts.sh
```

## 2. Create a key pair

```bash
ssh-keygen -t rsa -b 2048 -N "" -f ~/.ssh/excoredum-bench -C "excoredum-bench"
aws ec2 import-key-pair --key-name excoredum-bench \
  --public-key-material fileb://~/.ssh/excoredum-bench.pub \
  --region ap-southeast-1
```

> The AWS CLI defaults to your configured region (possibly not
> `ap-southeast-1`), while Terraform deploys into `ap-southeast-1`. Pass
> `--region ap-southeast-1` to any `aws ec2` command that must match the
> deployment.

## 3. Deploy infrastructure

```bash
terraform -chdir=deploy/aws/terraform init
terraform -chdir=deploy/aws/terraform plan
terraform -chdir=deploy/aws/terraform apply
```

```hcl
# terraform.tfvars
key_name = "excoredum-bench"      # EC2 key pair name (Ansible SSH)
ssh_cidr = "203.0.113.0/24"       # restrict SSH + gateway HTTP to your IP
```

Terraform only creates infrastructure. No IAM role, instance profile, or S3
bucket is involved, so the credential needs only EC2 permissions.

## 4. Provision and start services

```bash
cd deploy/aws/ansible
./inventory.sh                    # writes hosts.ini from terraform outputs
ansible-playbook playbooks/deploy.yml
```

The nodes, read replica, and gateway start automatically via systemd. The load
and verify units are installed but not started (they are one-shots run by the
operator). Tune the benchmark in `deploy/aws/ansible/group_vars/all.yml`:

- `workload_ops` / `workload_users` / `workload_symbols` - command count, user
  count, and symbol count for the `load` and `verify` runners (the write and
  read sides must use the same values).
- `workload_trade_limit` - the verifier's per-user trade-query limit (raise it
  together with the read replica's ledger caps when fills per user exceed 4096).
- `node_java_opts` / `read_java_opts` / `gateway_java_opts` / `load_java_opts`
  / `verify_java_opts` - per-role JVM flags (heap + GC; the bin scripts default
  to ZGC with a pinned heap when left empty).
- `aeron_term_length` - the cluster ingress term length (default `64k`; raise it
  to `1m` / `8m` for high-rate runs).
- `ledger_max_orders_per_user` / `ledger_max_market_trades` - the read replica's
  read-side ledger caps (defaults 4096 / 65536).

The `EXC_CORE_*` engine capacities (symbol / account / order-pool / journal,
etc.) can also be added to `nodes.yml` / `read.yml` `service_env` when a run
needs to raise them. See [SCALING.md](SCALING.md) for sizing guidance.

## 5. Run the load test and verify

```bash
cd deploy/aws/ansible
ansible-playbook playbooks/run-load.yml
ansible-playbook playbooks/run-verify.yml
ansible-playbook playbooks/run-gateway-bench.yml
```

Each runner prints throughput, latency `p50/p99/p99.9/max`, and a `PASS`/`FAIL`
line.

## 6. Collect metrics

- **Application** (runners, `journalctl -u excoredum` on each instance):
  throughput, latency tails, `success/nonSuccess/expired`, `leaderChanges`,
  `reconnects`, `backpressure`, `retransmits`, `fills observed vs expected`.
- **Cluster internal counters**: each node logs a `metrics ...` line every
  `metrics_interval_ms` (default 5000) via the `-Dexc.metricsIntervalMs` flag.
  The line carries `commands`, `duplicates`, `backpressure`, pool exhaustions,
  `journalBackpressure`, `dedupEvictions`, snapshot write/read ms, and
  `journalPublished`.
- **OS/host** (optional): `sar`/`vmstat` on the nodes for CPU,
  soft-interrupts, and network pps/bytes.
- **JVM** (optional): add `-Xlog:gc*` to the entrypoint for GC pause logs.

## 7. Teardown

```bash
terraform -chdir=deploy/aws/terraform destroy
```

## Notes

- **Saturation load**: `ExternalLoadRunner` is closed-loop (one client), so it
  measures latency and correctness. For a throughput ceiling, run several load
  instances concurrently, each with a distinct `load_client_id` (set in
  `group_vars/all.yml`).
- **Gateway throughput vs latency**: `GatewayBenchRunner` is closed-loop
  (concurrency 1). Use `k6`/`wrk` against the gateway for raw HTTP throughput.
- **Determinism**: keep `clean_start = true` for a fresh cluster; a warm
  restart (`clean_start = false`) reuses state and changes the measured window.
- **Fresh cluster per benchmark**: the load and gateway runners each run their
  own setup (add symbol + users + balances) and are not idempotent - re-running
  one after the other against the same cluster fails with `DUPLICATE`. Reset
  the cluster between runs with
  `ansible-playbook playbooks/fresh-cluster.yml` (stops every service, wipes
  the node data directory, and restarts the nodes with `clean_start = true`
  before bringing the read replica and gateway back up) or run each benchmark
  against a fresh environment.
- **Node public IPs (100.x)**: AWS can assign `100.x` public IPs to the
  cluster nodes that some networks cannot route (SSH times out during the
  banner exchange, while `3.x`/`44.x`/`54.x` work). If node SSH is flaky,
  attach Elastic IPs to the nodes, then `terraform refresh` before regenerating
  the inventory.
- **Incremental redeploy**: after the first deploy,
  `ansible-playbook playbooks/deploy.yml --tags configure` re-applies
  config/systemd without re-uploading the tarball; `--tags distribute`
  re-distributes only the tarball.
- **Placement group**: cluster placement groups accept one instance type and
  require available capacity in the AZ; if launch fails, retry or drop the
  placement group for a smoke run.
