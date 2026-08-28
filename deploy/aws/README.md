# AWS deployment for load testing and benchmarking

This directory provisions a throwaway AWS environment that runs the two
deployed benchmark modes against a real multi-node cluster:

- **Distributed cluster load test** - `ExternalLoadRunner` drives the
  deterministic `LoadWorkload` through the write client SDK and reports
  throughput, latency tails, and session stats.
- **Gateway HTTP/WS end-to-end** - `GatewayBenchRunner` drives the same
  workload through the REST/WebSocket gateway and cross-checks the replicated
  state on every read endpoint.

Topology (single AZ, `us-east-1a` by default):

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

## Prerequisites

- Terraform `>= 1.5`, AWS CLI with credentials, JDK 21 (to build), `curl`, `tar`.
- A non-existing S3 bucket name for the runtime artifact.

## 1. Prepare the artifact

```bash
# Create only the bucket first (instances download from it at boot).
terraform -chdir=deploy/aws/terraform init
terraform -chdir=deploy/aws/terraform apply -target=aws_s3_bucket.artifacts

# Build the self-contained runtime tarball (Temurin 21 JRE + 4 distributions
# + bin/ entrypoints) and upload it.
./deploy/aws/build-artifacts.sh --s3=s3://<bucket>/excoredum-runtime.tgz
```

Pin the JRE for reproducibility with `JRE_URL` (default is the Adoptium
"latest" 21 JRE for linux/x64):

```bash
JRE_URL=https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.5%2B11/OpenJDK21U-jre_x64_linux_hotspot_21.0.5_11.tar.gz \
  ./deploy/aws/build-artifacts.sh --s3=s3://<bucket>/excoredum-runtime.tgz
```

## 2. Deploy

```bash
terraform -chdir=deploy/aws/terraform plan
terraform -chdir=deploy/aws/terraform apply
```

Set the bucket and key to match step 1:

```hcl
# terraform.tfvars
s3_bucket = "<bucket>"
key_name  = "<your-keypair>"     # empty disables SSH (use SSM Session Manager)
ssh_cidr  = "203.0.113.0/24"     # restrict SSH + gateway HTTP
```

The nodes, read replica, and gateway start automatically via systemd. The load
and verify units are installed but not started (they are one-shots run by the
operator).

## 3. Run the load test and verify

```bash
# SSH into the load instance (private IP from outputs), then:
sudo systemctl start excoredum          # blocks until ExternalLoadRunner exits
sudo journalctl -u excoredum            # throughput + p50/p99/p99.9/max + PASS/FAIL
```

After the load run completes, run the read-side verifier:

```bash
# SSH into the verify instance, then:
sudo systemctl start excoredum          # ReadVerifyRunner: read-side checks PASS/FAIL
```

Run the gateway end-to-end benchmark from any instance against the gateway
URL (the `bench` distribution is on every instance):

```bash
/opt/excoredum/jre/bin/java \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  -cp "/opt/excoredum/bench/lib/*" \
  com.exadbe.bench.GatewayBenchRunner \
  --base-url=http://<gateway-ip>:8080 --ops=10000 --users=100 --admin-uid=811
```

## 4. Collect metrics

- **Application** (runners, `journalctl -u excoredum`): throughput, latency
  `p50/p99/p99.9/max`, `success/nonSuccess/expired`, `leaderChanges`,
  `reconnects`, `backpressure`, `retransmits`, `fills observed vs expected`.
- **Cluster internal counters**: each node logs a `metrics ...` line every
  `metrics_interval_ms` (default 5000) via the `-Dexc.metricsIntervalMs`
  flag added to `ClusterLauncher`. The line carries `commands`, `duplicates`,
  `backpressure`, pool exhaustions, `journalBackpressure`, `dedupEvictions`,
  snapshot write/read ms, and `journalPublished`.
- **OS/host** (optional): install the CloudWatch agent, or run `sar`/`vmstat`
  on the nodes for CPU, soft-interrupts, and network pps/bytes.
- **JVM** (optional): add `-Xlog:gc*` to the entrypoint for GC pause logs.
- **Aeron driver** (optional): run `aeron-stat` against each node's
  `aeron-dir` (under `/data/driver`) for driver counters and error stats.

## 5. Teardown

```bash
terraform -chdir=deploy/aws/terraform destroy
```

`force_destroy = true` on the bucket removes the uploaded tarball too.

## Notes

- **Saturation load**: `ExternalLoadRunner` is closed-loop (one client), so it
  measures latency and correctness. For a throughput ceiling, run several load
  instances concurrently, each with a distinct `load_client_id` (the engine
  dedups on `(clientId, clientSeq)`; the runner gained a `--client-id` flag for
  this).
- **Gateway throughput vs latency**: `GatewayBenchRunner` is closed-loop
  (concurrency 1). Use `k6`/`wrk` against the gateway for raw HTTP throughput.
- **Determinism**: keep `clean_start = true` for a fresh cluster; a warm
  restart (`clean_start = false`) reuses state and changes the measured window.
- **Placement group**: cluster placement groups accept one instance type and
  require available capacity in the AZ; if launch fails, retry or drop the
  placement group for a smoke run.
