package com.exadbe.testkit;

import com.exadbe.engine.MatchingEngine;
import com.exadbe.protocol.BalanceRecordDecoder;
import com.exadbe.protocol.DedupRecordDecoder;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.snapshot.SnapshotManager;
import java.util.Arrays;
import org.agrona.ExpandableArrayBuffer;

/**
 * Serialises engine state into an in-memory, length-prefixed record stream and
 * restores it, standing in for an Aeron snapshot publication/image in unit tests.
 */
public final class InMemorySnapshot {

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private int length;

    /** Writes a full snapshot of {@code engine} into the in-memory buffer. */
    public void writeFrom(final SnapshotManager manager, final MatchingEngine engine, final long logPosition) {
        length = 0;
        engine.writeSnapshot(
                manager,
                (recordBuffer, recordOffset, recordLength) -> {
                    buffer.putInt(length, recordLength);
                    length += Integer.BYTES;
                    buffer.putBytes(length, recordBuffer, recordOffset, recordLength);
                    length += recordLength;
                },
                () -> {},
                logPosition);
    }

    /** Restores a previously written snapshot into {@code engine}. */
    public void readInto(final SnapshotManager manager, final MatchingEngine engine) {
        engine.beginSnapshotLoad(manager);
        int offset = 0;
        while (offset < length && !manager.loadComplete()) {
            final int recordLength = buffer.getInt(offset);
            offset += Integer.BYTES;
            manager.onRecord(buffer, offset);
            offset += recordLength;
        }
    }

    /**
     * Restores every record except the trailing footer, standing in for a
     * truncated snapshot whose terminating footer never arrived.
     */
    public void readIntoDroppingFooter(final SnapshotManager manager, final MatchingEngine engine) {
        int lastRecordStart = 0;
        int scan = 0;
        while (scan < length) {
            lastRecordStart = scan;
            scan += Integer.BYTES + buffer.getInt(scan);
        }
        engine.beginSnapshotLoad(manager);
        int offset = 0;
        while (offset < lastRecordStart && !manager.loadComplete()) {
            final int recordLength = buffer.getInt(offset);
            offset += Integer.BYTES;
            manager.onRecord(buffer, offset);
            offset += recordLength;
        }
    }

    /**
     * Flips a byte of the first balance entry's value, simulating on-the-wire
     * corruption so the loaded state no longer reconciles with the footer checksum.
     */
    public void corruptFirstBalanceValue() {
        int offset = 0;
        while (offset < length) {
            final int recordLength = buffer.getInt(offset);
            final int bodyOffset = offset + Integer.BYTES;
            headerDecoder.wrap(buffer, bodyOffset);
            if (headerDecoder.templateId() == BalanceRecordDecoder.TEMPLATE_ID) {
                final int balanceValueIndex =
                        bodyOffset + MessageHeaderDecoder.ENCODED_LENGTH + Long.BYTES + Integer.BYTES;
                buffer.putByte(balanceValueIndex, (byte) (buffer.getByte(balanceValueIndex) ^ 0x01));
                return;
            }
            offset = bodyOffset + recordLength;
        }
        throw new IllegalStateException("no BalanceRecord found to corrupt");
    }

    /**
     * Flips a byte of the first dedup record's uid, simulating on-the-wire
     * corruption confined to a dedup field so the loaded state no longer
     * reconciles with the footer checksum (the checksum must cover dedup fields).
     */
    public void corruptFirstDedupUid() {
        int offset = 0;
        while (offset < length) {
            final int recordLength = buffer.getInt(offset);
            final int bodyOffset = offset + Integer.BYTES;
            headerDecoder.wrap(buffer, bodyOffset);
            if (headerDecoder.templateId() == DedupRecordDecoder.TEMPLATE_ID) {
                final int uidIndex = bodyOffset + DedupRecordDecoder.uidEncodingOffset();
                buffer.putByte(uidIndex, (byte) (buffer.getByte(uidIndex) ^ 0x01));
                return;
            }
            offset = bodyOffset + recordLength;
        }
        throw new IllegalStateException("no DedupRecord found to corrupt");
    }

    /** Returns a copy of the raw serialized bytes for byte-identical comparisons. */
    public byte[] toByteArray() {
        final byte[] out = new byte[length];
        buffer.getBytes(0, out, 0, length);
        return out;
    }

    public int length() {
        return length;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InMemorySnapshot other)) {
            return false;
        }
        return length == other.length && Arrays.equals(toByteArray(), other.toByteArray());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(toByteArray());
    }
}
