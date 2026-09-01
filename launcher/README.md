# launcher

Bootstraps a single justrade cluster node. It stands up the Aeron stack (media
driver, Archive, consensus) and hosts the matching `ClusteredService`, plus the
agent that drains the event journal to the Archive.

## Responsibility

- Launch and wire an Aeron media driver, Archive, and Cluster (consensus) for one
  node.
- Host the `core` matching service inside the cluster container.
- Run the journaler agent that records committed domain events to the Archive,
  off the consensus thread.

## Key classes

- [ClusterLauncher.java](src/main/java/io/justrade/launcher/ClusterLauncher.java) -
  entry point; assembles and starts a node.
- `ClusterNode` - the composed node lifecycle.
- `EventJournalRecorder` - drains the off-heap journal ring to the Archive.

## Run

```bash
# Single-node localhost cluster
./gradlew :launcher:run

# With a config file overriding CoreConfig capacities
./gradlew :launcher:run --args="--config=production.properties"
```

The launch configuration sets the required `--add-opens` JVM flags
automatically.

## Configuration

- `CoreConfig` capacities via `justrade.core.*` (`--config=<file>` or
  `-Djustrade.core.*`).
- Aeron ingress term length via `justrade.aeron.termLength` (default `64k`).

See [../docs/getting-started.md](../docs/getting-started.md#configuration) and the
capacity table in [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md#configuration).

## Related

- Consensus and determinism: [../docs/concepts/determinism-and-consensus.md](../docs/concepts/determinism-and-consensus.md).
- Deployment: [../deploy/aws/README.md](../deploy/aws/README.md) or [../docker/docker-compose.yml](../docker/docker-compose.yml).
