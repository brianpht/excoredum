package io.justrade.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.justrade.config.CoreConfig;
import io.justrade.core.CommandOutcome;
import io.justrade.engine.MatchingEngine;
import io.justrade.protocol.BalanceRecordDecoder;
import io.justrade.protocol.CommandEnvelopeDecoder;
import io.justrade.protocol.CommandEnvelopeEncoder;
import io.justrade.protocol.MessageHeaderDecoder;
import io.justrade.protocol.MessageHeaderEncoder;
import io.justrade.protocol.OrderAction;
import io.justrade.protocol.OrderCommandType;
import io.justrade.protocol.OrderType;
import io.justrade.read.config.ReadReplicaConfig;
import io.justrade.read.order.OrderLedger;
import io.justrade.snapshot.SnapshotManager;
import io.justrade.telemetry.CoreMetrics;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A corrupt local checkpoint must never make the replica unconstructable and
 * must never leave a partially loaded engine behind: every corruption flavor
 * falls back to a cold start (empty engine, position 0) with the failure
 * surfaced through the health counters.
 */
final class ReplicaCheckpointCorruptionTest {

    private static final int SYMBOL = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long UID = 7L;
    private static final int MAGIC = 0x45584352;
    private static final int VERSION = 1;

    private static final UnsafeBuffer ENCODE_BUFFER = new UnsafeBuffer(new byte[256]);
    private static final MessageHeaderEncoder HEADER_ENCODER = new MessageHeaderEncoder();
    private static final CommandEnvelopeEncoder ENVELOPE_ENCODER = new CommandEnvelopeEncoder();
    private static final MessageHeaderDecoder HEADER_DECODER = new MessageHeaderDecoder();
    private static final CommandEnvelopeDecoder ENVELOPE_DECODER = new CommandEnvelopeDecoder();

    @Test
    void badMagicFallsBackToColdStart(@TempDir final Path baseDir) throws IOException {
        final Path checkpoint = baseDir.resolve("replica.checkpoint");
        Files.write(checkpoint, new byte[] {0, 0, 0, 0, 1, 2, 3, 4});

        assertColdStartFallback(baseDir, checkpoint);
    }

    @Test
    void truncatedCheckpointClearsPartiallyLoadedEngine(@TempDir final Path baseDir) throws IOException {
        final List<byte[]> records = snapshotRecords();
        final Path checkpoint = baseDir.resolve("replica.checkpoint");
        // Write the header, symbol, user, and balance records fully (the engine
        // has real state by then), then cut the next record mid-body so the load
        // fails after the engine was mutated.
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(checkpoint))) {
            writeFileHeader(out);
            final int complete = 4;
            for (int i = 0; i < complete; i++) {
                out.writeInt(records.get(i).length);
                out.write(records.get(i));
            }
            final byte[] next = records.get(complete);
            out.writeInt(next.length);
            out.write(next, 0, next.length / 2);
        }

        assertColdStartFallback(baseDir, checkpoint);
    }

    @Test
    void negativeLedgerLengthFallsBackToColdStart(@TempDir final Path baseDir) throws IOException {
        final List<byte[]> records = snapshotRecords();
        final Path checkpoint = baseDir.resolve("replica.checkpoint");
        // All engine records load and pass the invariant; the ledger length
        // field is negative, which escapes IOException entirely.
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(checkpoint))) {
            writeFileHeader(out);
            for (final byte[] record : records) {
                out.writeInt(record.length);
                out.write(record);
            }
            out.writeInt(0);
            out.writeInt(-5);
        }

        assertColdStartFallback(baseDir, checkpoint);
    }

    @Test
    void invariantViolationClearsLoadedEngine(@TempDir final Path baseDir) throws IOException {
        final List<byte[]> records = snapshotRecords();
        corruptFirstBalanceValue(records);
        final Path checkpoint = baseDir.resolve("replica.checkpoint");
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(checkpoint))) {
            writeFileHeader(out);
            for (final byte[] record : records) {
                out.writeInt(record.length);
                out.write(record);
            }
            out.writeInt(0);
            out.write(ledgerBytes());
        }

        assertColdStartFallback(baseDir, checkpoint);
    }

    private static void assertColdStartFallback(final Path baseDir, final Path checkpoint) {
        final ReadReplicaConfig config = ReadReplicaConfig.builder(
                        baseDir.resolve("driver").toString())
                .channels("aeron:udp?endpoint=localhost:20999")
                .localHost("localhost")
                .checkpointFile(checkpoint)
                .build();
        try (ExcReadReplica replica = new ExcReadReplica(config, CoreConfig.defaults())) {
            assertEquals(1L, replica.health().checkpointFailures(), "the corruption must be surfaced");
            assertFalse(replica.userExists(UID), "a corrupt checkpoint must not leave engine state behind");
        }
    }

    // Builds an engine with a symbol, one user, and a balance, then returns its
    // deterministic snapshot records in write order.
    private static List<byte[]> snapshotRecords() {
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final CommandOutcome out = new CommandOutcome();
        engine.process(symbolCommand(1L), 0L, out);
        engine.process(accountCommand(OrderCommandType.ADD_USER, UID, 2L), 0L, out);
        engine.process(accountCommand(OrderCommandType.BALANCE_ADJUSTMENT, UID, 3L), 0L, out);

        final List<byte[]> records = new ArrayList<>();
        engine.writeSnapshot(
                new SnapshotManager(),
                (buffer, offset, length) -> {
                    final byte[] record = new byte[length];
                    buffer.getBytes(offset, record, 0, length);
                    records.add(record);
                },
                () -> {},
                1234L);
        return records;
    }

    private static void writeFileHeader(final DataOutputStream out) throws IOException {
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        out.writeLong(1234L);
        out.writeInt(0);
    }

    private static byte[] ledgerBytes() {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            new OrderLedger().writeTo(new DataOutputStream(stream));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return stream.toByteArray();
    }

    private static void corruptFirstBalanceValue(final List<byte[]> records) {
        for (final byte[] record : records) {
            HEADER_DECODER.wrap(new UnsafeBuffer(record), 0);
            if (HEADER_DECODER.templateId() != BalanceRecordDecoder.TEMPLATE_ID) {
                continue;
            }
            // Flip one byte of the balance value (after uid and currency).
            final int balanceIndex = MessageHeaderDecoder.ENCODED_LENGTH + Long.BYTES + Integer.BYTES;
            record[balanceIndex] ^= 0x01;
            return;
        }
        throw new IllegalStateException("no BalanceRecord found to corrupt");
    }

    private static CommandEnvelopeDecoder symbolCommand(final long seq) {
        ENVELOPE_ENCODER
                .wrapAndApplyHeader(ENCODE_BUFFER, 0, HEADER_ENCODER)
                .clientId(1L)
                .clientSeq(seq)
                .commandIdHi(1L)
                .commandIdLo(seq)
                .commandType(OrderCommandType.ADD_SYMBOL)
                .uid(0L)
                .symbolId(SYMBOL)
                .orderId(CommandEnvelopeEncoder.orderIdNullValue())
                .price(CommandEnvelopeEncoder.priceNullValue())
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(CommandEnvelopeEncoder.sizeNullValue())
                .action(OrderAction.NULL_VAL)
                .orderType(OrderType.NULL_VAL)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(BASE)
                .quoteCurrency(QUOTE)
                .baseScaleK(1L)
                .quoteScaleK(1L)
                .takerFee(0L)
                .makerFee(0L);
        return wrapEncoded();
    }

    private static CommandEnvelopeDecoder accountCommand(final OrderCommandType type, final long uid, final long seq) {
        ENVELOPE_ENCODER
                .wrapAndApplyHeader(ENCODE_BUFFER, 0, HEADER_ENCODER)
                .clientId(1L)
                .clientSeq(seq)
                .commandIdHi(1L)
                .commandIdLo(seq)
                .commandType(type)
                .uid(uid)
                .symbolId(CommandEnvelopeEncoder.symbolIdNullValue())
                .orderId(CommandEnvelopeEncoder.orderIdNullValue())
                .price(CommandEnvelopeEncoder.priceNullValue())
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(CommandEnvelopeEncoder.sizeNullValue())
                .action(OrderAction.NULL_VAL)
                .orderType(OrderType.NULL_VAL)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(
                        type == OrderCommandType.BALANCE_ADJUSTMENT ? BASE : CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(
                        type == OrderCommandType.BALANCE_ADJUSTMENT
                                ? 1000L
                                : CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue())
                .takerFee(CommandEnvelopeEncoder.takerFeeNullValue())
                .makerFee(CommandEnvelopeEncoder.makerFeeNullValue());
        return wrapEncoded();
    }

    private static CommandEnvelopeDecoder wrapEncoded() {
        HEADER_DECODER.wrap(ENCODE_BUFFER, 0);
        ENVELOPE_DECODER.wrap(
                ENCODE_BUFFER,
                MessageHeaderDecoder.ENCODED_LENGTH,
                HEADER_DECODER.blockLength(),
                HEADER_DECODER.version());
        return ENVELOPE_DECODER;
    }
}
