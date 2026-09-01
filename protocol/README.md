# protocol

The wire contract for justrade: the SBE (Simple Binary Encoding) schema and the
generated flyweight codecs that every other module uses to talk to the cluster
and the read replica. This module is the contract only; it holds no business
logic.

## Responsibility

- Define all on-wire and IPC messages in an SBE schema.
- Generate zero-copy flyweight encoders and decoders (no reflection, little
  endian, fixed binary layout with backward-compatible optional fields).
- Provide stream identifiers and shared wire constants.

## Key contents

- [src/main/resources/messages.xml](src/main/resources/messages.xml) - the SBE
  schema (source of truth for the wire format).
- Generated codecs under the module build output (encoders/decoders for
  `CommandEnvelope`, `CommandResult`, trade / reduce / reject events,
  `QueryRequest` / `QueryResponse`, journal events, and snapshot records).
- `QueryStreams` and related constants - stream ids for the read path.

## Message families

- Ingress: `CommandEnvelope` (command plus correlation: clientId, clientSeq,
  commandId).
- Egress: `CommandResult`, `TradeEvent`, `ReduceEvent`, `RejectEvent`.
- Read path: `QueryRequest`, `QueryResponse`.
- Persistence: journal events and snapshot records.

## Design notes

- Flyweights hold no state: `wrap(buffer, offset, blockLength, version)` per
  message, decode fields in place, never build an intermediate POJO.
- Little endian only, matching SBE default and native x86/ARM order.
- Schema evolution is via optional fields, so newer and older peers interoperate.

## Related

- Wire and snapshot format details: [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).
- Concepts: [../docs/GLOSSARY.md](../docs/GLOSSARY.md) (SBE, flyweight).
