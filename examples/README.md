# examples

Runnable examples that drive justrade through the client SDK. The quickest way to
see the whole system work end to end.

## Contents

- [QuickStartExample.java](src/main/java/io/justrade/examples/QuickStartExample.java) -
  boots an in-process single-node cluster, submits commands through the write
  client, and prints every egress event (trades, reduces, rejects).

## Run

```bash
./gradlew :examples:run
```

The example configuration sets the required `--add-opens` JVM flags
automatically.

## Related

- Step-by-step setup: [../docs/getting-started.md](../docs/getting-started.md).
- What the events mean: [../docs/concepts/order-types.md](../docs/concepts/order-types.md).
- Write SDK: [../write-client/README.md](../write-client/README.md).
