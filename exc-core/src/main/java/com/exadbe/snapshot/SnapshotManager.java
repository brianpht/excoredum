package com.exadbe.snapshot;

import com.exadbe.collections.AccountStore;
import com.exadbe.collections.DedupTable;
import com.exadbe.engine.risk.SymbolSpec;
import com.exadbe.engine.risk.SymbolSpecStore;
import com.exadbe.protocol.BalanceRecordDecoder;
import com.exadbe.protocol.BalanceRecordEncoder;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.protocol.DedupRecordDecoder;
import com.exadbe.protocol.DedupRecordEncoder;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderRecordDecoder;
import com.exadbe.protocol.OrderRecordEncoder;
import com.exadbe.protocol.SnapshotFooterDecoder;
import com.exadbe.protocol.SnapshotFooterEncoder;
import com.exadbe.protocol.SnapshotHeaderDecoder;
import com.exadbe.protocol.SnapshotHeaderEncoder;
import com.exadbe.protocol.SymbolSpecRecordDecoder;
import com.exadbe.protocol.SymbolSpecRecordEncoder;
import com.exadbe.protocol.UserRecordDecoder;
import com.exadbe.protocol.UserRecordEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Serialises and restores engine state as a sequence of self-describing SBE
 * records: a header, then symbols, users, balances, resting orders, and dedup
 * records in deterministic key order, terminated by a footer carrying a
 * checksum.
 *
 * <p>Records are emitted one at a time into a small reusable buffer and handed
 * to a {@link SnapshotSink}, so the writer never allocates a dataset-sized
 * buffer. On load, records are fed one at a time to {@link #onRecord} in the
 * same order they were written.
 *
 * <p>Deterministic ordering is guaranteed by the stores' {@code forEachSorted}
 * methods and the book's {@code forEachOrderSorted}, which is mandatory for
 * byte-identical snapshots across nodes.
 */
public final class SnapshotManager {

    private static final int MAX_RECORD_LENGTH = 128;
    private static final long HASH_PRIME = 1000003L;

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SnapshotHeaderEncoder snapshotHeaderEncoder = new SnapshotHeaderEncoder();
    private final SymbolSpecRecordEncoder symbolEncoder = new SymbolSpecRecordEncoder();
    private final UserRecordEncoder userEncoder = new UserRecordEncoder();
    private final BalanceRecordEncoder balanceEncoder = new BalanceRecordEncoder();
    private final OrderRecordEncoder orderEncoder = new OrderRecordEncoder();
    private final DedupRecordEncoder dedupEncoder = new DedupRecordEncoder();
    private final SnapshotFooterEncoder footerEncoder = new SnapshotFooterEncoder();
    private final UnsafeBuffer recordBuffer = new UnsafeBuffer(new byte[MAX_RECORD_LENGTH]);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final SnapshotHeaderDecoder snapshotHeaderDecoder = new SnapshotHeaderDecoder();
    private final SymbolSpecRecordDecoder symbolDecoder = new SymbolSpecRecordDecoder();
    private final UserRecordDecoder userDecoder = new UserRecordDecoder();
    private final BalanceRecordDecoder balanceDecoder = new BalanceRecordDecoder();
    private final OrderRecordDecoder orderDecoder = new OrderRecordDecoder();
    private final DedupRecordDecoder dedupDecoder = new DedupRecordDecoder();
    private final SnapshotFooterDecoder footerDecoder = new SnapshotFooterDecoder();

    // Load-side targets, set once before load begins.
    private SymbolSpecStore loadSymbols;
    private AccountStore loadAccounts;
    private OrderRestorer loadOrders;
    private OrderSource loadOrderSource;
    private DedupTable loadDedup;
    private long expectedChecksum;
    private boolean footerSeen;
    private long loadedLogPosition;

    /** Receives one encoded snapshot record. */
    @FunctionalInterface
    public interface SnapshotSink {
        void accept(DirectBuffer buffer, int offset, int length);
    }

    /** Emits every resting order across all books in deterministic order. */
    @FunctionalInterface
    public interface OrderSource {
        void forEach(OrderSink sink);
    }

    /** Receives one resting order for either serialisation or checksumming. */
    @FunctionalInterface
    public interface OrderSink {
        void accept(
                int symbolId,
                long orderId,
                boolean ask,
                long price,
                long size,
                long filled,
                long reserveBidPrice,
                long uid,
                long timestamp);
    }

    /** Reinstates one resting order into its book during a load. */
    @FunctionalInterface
    public interface OrderRestorer {
        void restore(
                int symbolId,
                long orderId,
                boolean ask,
                long price,
                long size,
                long filled,
                long reserveBidPrice,
                long uid,
                long timestamp);
    }

    /**
     * Writes a full snapshot to the sink in deterministic order.
     *
     * @param idler invoked once per record so a live publication can honour
     *     back-pressure via {@code Cluster.idle()}; may be a no-op in tests.
     */
    public void write(
            final SnapshotSink sink,
            final Runnable idler,
            final SymbolSpecStore symbols,
            final AccountStore accounts,
            final OrderSource orderSource,
            final DedupTable dedup,
            final long logPosition) {
        final long checksum = computeChecksum(symbols, accounts, orderSource, dedup);

        int len = snapshotHeaderEncoder
                .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                .logPosition(logPosition)
                .snapshotSchemaVer(SnapshotHeaderEncoder.SCHEMA_VERSION)
                .symbolCount(symbols.size())
                .userCount(accounts.userCount())
                .orderCount(countOrders(orderSource))
                .dedupClientCount(dedup.clientCount())
                .encodedLength();
        emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + len);

        symbols.forEachSorted(spec -> {
            final int l = symbolEncoder
                    .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                    .symbolId(spec.symbolId())
                    .baseCurrency(spec.baseCurrency())
                    .quoteCurrency(spec.quoteCurrency())
                    .baseScaleK(spec.baseScaleK())
                    .quoteScaleK(spec.quoteScaleK())
                    .encodedLength();
            emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + l);
        });

        accounts.forEachUserSorted(uid -> {
            final int l = userEncoder
                    .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                    .uid(uid)
                    .encodedLength();
            emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + l);
        });

        accounts.forEachSorted((uid, currency, balance) -> {
            final int l = balanceEncoder
                    .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                    .uid(uid)
                    .currency(currency)
                    .balance(balance)
                    .encodedLength();
            emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + l);
        });

        orderSource.forEach((symbolId, orderId, ask, price, size, filled, reserveBidPrice, uid, timestamp) -> {
            final int l = orderEncoder
                    .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                    .symbolId(symbolId)
                    .orderId(orderId)
                    .ask((short) (ask ? 1 : 0))
                    .price(price)
                    .size(size)
                    .filled(filled)
                    .reserveBidPrice(reserveBidPrice)
                    .uid(uid)
                    .timestamp(timestamp)
                    .encodedLength();
            emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + l);
        });

        dedup.forEachSorted(
                (clientId, seq, cmdHi, cmdLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    dedupEncoder
                            .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                            .clientId(clientId)
                            .clientSeq(seq)
                            .commandIdHi(cmdHi)
                            .commandIdLo(cmdLo)
                            .resultCode(CommandResultCode.get(code))
                            .uid(hasUid ? uid : DedupRecordEncoder.uidNullValue())
                            .orderId(hasOrderId ? orderId : DedupRecordEncoder.orderIdNullValue())
                            .filledSize(hasFilledSize ? filledSize : DedupRecordEncoder.filledSizeNullValue());
                    emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + dedupEncoder.encodedLength());
                });

        len = footerEncoder
                .wrapAndApplyHeader(recordBuffer, 0, headerEncoder)
                .checksum(checksum)
                .encodedLength();
        emit(sink, idler, MessageHeaderEncoder.ENCODED_LENGTH + len);
    }

    private void emit(final SnapshotSink sink, final Runnable idler, final int length) {
        sink.accept(recordBuffer, 0, length);
        idler.run();
    }

    /** Prepares the manager to receive records for the given (cleared) targets. */
    public void beginLoad(
            final SymbolSpecStore symbols,
            final AccountStore accounts,
            final OrderSource orderSource,
            final OrderRestorer orderRestorer,
            final DedupTable dedup) {
        symbols.clear();
        accounts.clear();
        dedup.clear();
        this.loadSymbols = symbols;
        this.loadAccounts = accounts;
        this.loadOrderSource = orderSource;
        this.loadOrders = orderRestorer;
        this.loadDedup = dedup;
        this.expectedChecksum = 0L;
        this.footerSeen = false;
        this.loadedLogPosition = 0L;
    }

    /** Decodes and applies a single snapshot record. */
    public void onRecord(final DirectBuffer buffer, final int offset) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();
        final int blockLength = headerDecoder.blockLength();
        final int version = headerDecoder.version();
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        switch (templateId) {
            case SnapshotHeaderDecoder.TEMPLATE_ID -> {
                snapshotHeaderDecoder.wrap(buffer, bodyOffset, blockLength, version);
                loadedLogPosition = snapshotHeaderDecoder.logPosition();
            }
            case SymbolSpecRecordDecoder.TEMPLATE_ID -> {
                symbolDecoder.wrap(buffer, bodyOffset, blockLength, version);
                loadSymbols.add(new SymbolSpec(
                        symbolDecoder.symbolId(),
                        symbolDecoder.baseCurrency(),
                        symbolDecoder.quoteCurrency(),
                        symbolDecoder.baseScaleK(),
                        symbolDecoder.quoteScaleK()));
            }
            case UserRecordDecoder.TEMPLATE_ID -> {
                userDecoder.wrap(buffer, bodyOffset, blockLength, version);
                loadAccounts.addUser(userDecoder.uid());
            }
            case BalanceRecordDecoder.TEMPLATE_ID -> {
                balanceDecoder.wrap(buffer, bodyOffset, blockLength, version);
                loadAccounts.set(balanceDecoder.uid(), balanceDecoder.currency(), balanceDecoder.balance());
            }
            case OrderRecordDecoder.TEMPLATE_ID -> {
                orderDecoder.wrap(buffer, bodyOffset, blockLength, version);
                loadOrders.restore(
                        orderDecoder.symbolId(),
                        orderDecoder.orderId(),
                        orderDecoder.ask() != 0,
                        orderDecoder.price(),
                        orderDecoder.size(),
                        orderDecoder.filled(),
                        orderDecoder.reserveBidPrice(),
                        orderDecoder.uid(),
                        orderDecoder.timestamp());
            }
            case DedupRecordDecoder.TEMPLATE_ID -> {
                dedupDecoder.wrap(buffer, bodyOffset, blockLength, version);
                final long uid = dedupDecoder.uid();
                final long orderId = dedupDecoder.orderId();
                final long filledSize = dedupDecoder.filledSize();
                final boolean hasUid = uid != DedupRecordDecoder.uidNullValue();
                final boolean hasOrderId = orderId != DedupRecordDecoder.orderIdNullValue();
                final boolean hasFilledSize = filledSize != DedupRecordDecoder.filledSizeNullValue();
                loadDedup.store(
                        dedupDecoder.clientId(),
                        dedupDecoder.clientSeq(),
                        dedupDecoder.commandIdHi(),
                        dedupDecoder.commandIdLo(),
                        dedupDecoder.resultCode().value(),
                        hasUid ? uid : 0L,
                        hasUid,
                        hasOrderId ? orderId : 0L,
                        hasOrderId,
                        hasFilledSize ? filledSize : 0L,
                        hasFilledSize);
            }
            case SnapshotFooterDecoder.TEMPLATE_ID -> {
                footerDecoder.wrap(buffer, bodyOffset, blockLength, version);
                expectedChecksum = footerDecoder.checksum();
                footerSeen = true;
            }
            default -> throw new IllegalStateException("Unknown snapshot template id: " + templateId);
        }
    }

    /** Returns {@code true} once the terminating footer has been applied. */
    public boolean loadComplete() {
        return footerSeen;
    }

    /** The log position decoded from the snapshot header, or 0 if not yet loaded. */
    public long loadedLogPosition() {
        return loadedLogPosition;
    }

    /** Verifies that the restored state reproduces the checksum carried by the footer. */
    public boolean verifyInvariant() {
        return footerSeen && computeChecksum(loadSymbols, loadAccounts, loadOrderSource, loadDedup) == expectedChecksum;
    }

    private static int countOrders(final OrderSource orderSource) {
        final int[] count = {0};
        orderSource.forEach(
                (symbolId, orderId, ask, price, size, filled, reserveBidPrice, uid, timestamp) -> count[0]++);
        return count[0];
    }

    private static long computeChecksum(
            final SymbolSpecStore symbols,
            final AccountStore accounts,
            final OrderSource orderSource,
            final DedupTable dedup) {
        final long[] h = {1L};
        symbols.forEachSorted(spec -> {
            h[0] = combine(h[0], spec.symbolId());
            h[0] = combine(h[0], spec.baseCurrency());
            h[0] = combine(h[0], spec.quoteCurrency());
            h[0] = combine(h[0], spec.baseScaleK());
            h[0] = combine(h[0], spec.quoteScaleK());
        });
        accounts.forEachUserSorted(uid -> h[0] = combine(h[0], uid));
        accounts.forEachSorted((uid, currency, balance) -> {
            h[0] = combine(h[0], uid);
            h[0] = combine(h[0], currency);
            h[0] = combine(h[0], balance);
        });
        orderSource.forEach((symbolId, orderId, ask, price, size, filled, reserveBidPrice, uid, timestamp) -> {
            h[0] = combine(h[0], symbolId);
            h[0] = combine(h[0], orderId);
            h[0] = combine(h[0], ask ? 1L : 0L);
            h[0] = combine(h[0], price);
            h[0] = combine(h[0], size);
            h[0] = combine(h[0], filled);
            h[0] = combine(h[0], reserveBidPrice);
            h[0] = combine(h[0], uid);
            h[0] = combine(h[0], timestamp);
        });
        dedup.forEachSorted(
                (clientId, seq, cmdHi, cmdLo, code, uid, hasUid, orderId, hasOrderId, filledSize, hasFilledSize) -> {
                    h[0] = combine(h[0], clientId);
                    h[0] = combine(h[0], seq);
                    h[0] = combine(h[0], cmdHi);
                    h[0] = combine(h[0], cmdLo);
                    h[0] = combine(h[0], code);
                });
        return h[0];
    }

    private static long combine(final long h, final long v) {
        return h * HASH_PRIME + v;
    }

    /** Length of the small reusable per-record buffer. */
    public static int maxRecordLength() {
        return MAX_RECORD_LENGTH;
    }

    /** Exposes the reusable record buffer for callers that copy before offering. */
    public MutableDirectBuffer recordBuffer() {
        return recordBuffer;
    }
}
