package io.justrade.read;

import io.justrade.engine.MatchingEngine;
import io.justrade.read.order.OrderLedger;
import io.justrade.snapshot.SnapshotManager;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Local checkpoint of the read replica's replicated state: the engine (via the
 * same deterministic {@link SnapshotManager} records the cluster uses), the
 * {@link OrderLedger} (read-side-only, so it cannot come from a cluster
 * snapshot), and the cluster-global applied position. Written atomically (temp
 * file + rename) so a crash mid-write never corrupts the last good checkpoint.
 *
 * <p>On a warm start the replica loads the checkpoint and resumes the consensus
 * log from the applied position, skipping the replay of everything before it.
 * The checkpoint path is read-side-only and never touches the cluster.
 */
public final class ReplicaCheckpoint {

    private static final int MAGIC = 0x45584352; // "EXCR"
    private static final int VERSION = 1;

    /** Checkpoint contents for a warm start. */
    public record Data(long logPosition, int currentSource, OrderLedger ledger) {}

    /** The resume position stored in a checkpoint, without loading the state. */
    public record Position(long logPosition, int currentSource) {}

    private ReplicaCheckpoint() {}

    /** Reads only the header (resume position + source) of a checkpoint file. */
    public static Position peek(final Path file) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
            if (in.readInt() != MAGIC) {
                throw new IOException("not an justrade replica checkpoint: " + file);
            }
            if (in.readInt() != VERSION) {
                throw new IOException("unsupported replica checkpoint version: " + file);
            }
            return new Position(in.readLong(), in.readInt());
        }
    }

    /**
     * Writes the engine + ledger + position atomically to {@code file}.
     *
     * @param engine the replica's engine, written via {@link SnapshotManager} in
     *     deterministic order
     * @param ledger the read-side order ledger
     * @param logPosition the cluster-global position the state covers
     * @param currentSource the source index to resume from
     */
    public static void save(
            final Path file,
            final MatchingEngine engine,
            final OrderLedger ledger,
            final long logPosition,
            final int currentSource)
            throws IOException {
        final Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        final Path tmp = file.resolveSibling(file.getFileName().toString() + ".tmp");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)))) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(logPosition);
            out.writeInt(currentSource);
            engine.writeSnapshot(
                    new SnapshotManager(),
                    (buffer, offset, length) -> {
                        final byte[] record = new byte[length];
                        buffer.getBytes(offset, record, 0, length);
                        try {
                            out.writeInt(length);
                            out.write(record);
                        } catch (final IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    },
                    () -> {},
                    logPosition);
            out.writeInt(0); // snapshot-record terminator
            final ByteArrayOutputStream ledgerStream = new ByteArrayOutputStream();
            ledger.writeTo(new DataOutputStream(ledgerStream));
            final byte[] ledgerBytes = ledgerStream.toByteArray();
            out.writeInt(ledgerBytes.length);
            out.write(ledgerBytes);
        }
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Loads a checkpoint into {@code engine} (whose stores are replaced with the
     * checkpointed state) and returns the ledger and resume position.
     *
     * @param maxOrdersPerUser per-user order-history cap for the rebuilt ledger
     * @param maxMarketTrades market trade-tape cap for the rebuilt ledger
     * @throws IOException when the file is missing, corrupt, or fails the engine
     *     integrity check
     */
    public static Data load(
            final Path file, final MatchingEngine engine, final int maxOrdersPerUser, final int maxMarketTrades)
            throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
            if (in.readInt() != MAGIC) {
                throw new IOException("not an justrade replica checkpoint: " + file);
            }
            if (in.readInt() != VERSION) {
                throw new IOException("unsupported replica checkpoint version: " + file);
            }
            final long logPosition = in.readLong();
            final int currentSource = in.readInt();

            final SnapshotManager manager = new SnapshotManager();
            engine.beginSnapshotLoad(manager);
            for (; ; ) {
                final int length = in.readInt();
                if (length == 0) {
                    break;
                }
                final byte[] record = new byte[length];
                in.readFully(record);
                manager.onRecord(new UnsafeBuffer(record), 0);
            }
            if (!manager.verifyInvariant()) {
                throw new IOException("replica checkpoint integrity check failed: " + file);
            }

            final int ledgerLength = in.readInt();
            final byte[] ledgerBytes = new byte[ledgerLength];
            in.readFully(ledgerBytes);
            final OrderLedger ledger = new OrderLedger(maxOrdersPerUser, maxMarketTrades);
            ledger.readFrom(new DataInputStream(new ByteArrayInputStream(ledgerBytes)));
            return new Data(logPosition, currentSource, ledger);
        }
    }
}
