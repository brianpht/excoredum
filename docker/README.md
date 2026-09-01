# Docker end-to-end system test

This directory packages the whole justrade system into containers and runs the
same deterministic pipeline as `SystemLoadIntegrationTest`, but across the
network: a 3-node Aeron Cluster (Raft), a CQRS read replica, a write-side load
runner, and a read-side verifier. Exit code `0` means every write-side and
read-side check passed.

For the full design (data flow, deterministic workload, verification, and the
defects this test has caught) see the "Containerized System Test" section of
[../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md). This file is the operational
quick reference.

## Layout

| File                   | Role                                                             |
|------------------------|-----------------------------------------------------------------|
| `Dockerfile`           | Multi-stage build, JDK 21, `installDist` for launcher/read/bench |
| `docker-compose.yml`   | 6 services on the `justrade-net` bridge; one shared `justrade:test` image |
| `entrypoint-node.sh`   | Cluster member (`ClusterLauncher`)                              |
| `entrypoint-read.sh`   | Read replica (`ReadServiceLauncher`)                            |
| `entrypoint-load.sh`   | Write-side load runner (`ExternalLoadRunner`)                   |
| `entrypoint-verify.sh` | Read-side verifier (`ReadVerifyRunner`)                         |

## Services and ports

| Container    | Role                                                                    |
|--------------|-------------------------------------------------------------------------|
| `node-0/1/2` | Raft members; node `n` uses `20100 + n*100 .. +4` (ingress, consensus, log, catchup, archive) |
| `read`       | CQRS replica following node-0's archive (node-1/node-2 as failover), answers queries on `0.0.0.0:44000` |
| `load`       | Submits the 100k-command workload through `WriteClient`, checks the write side |
| `verify`     | Replays the simulation, asserts the read side matches it exactly         |

Ingress is UDP `20100 / 20200 / 20300`; queries use stream `300` (request) /
`301` (response) on port `44000`. Every container runs its own Aeron media
driver and advertises its own address, so no `localhost` assumptions cross
container boundaries.

## Run

```bash
docker compose -f docker/docker-compose.yml up --build   # exit 0 = all checks passed
docker compose -f docker/docker-compose.yml ps
docker compose -f docker/docker-compose.yml logs load verify
docker compose -f docker/docker-compose.yml down -v      # teardown, remove volumes
```

Scale the workload with `JUSTRADE_OPS`, `JUSTRADE_USERS`, and `JUSTRADE_SYMBOLS`
on the `load` and `verify` services (they must match). The `read` service reads
its sources from `JUSTRADE_ARCHIVE` and binds queries via `JUSTRADE_QUERY`.
