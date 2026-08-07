package com.exadbe.core;

import com.exadbe.config.CoreConfig;
import com.exadbe.engine.MatchingEngine;
import com.exadbe.engine.orderbook.L2View;
import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandResultEncoder;
import com.exadbe.protocol.L2MarketDataEncoder;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderCommandType;
import com.exadbe.protocol.ReduceEventEncoder;
import com.exadbe.protocol.RejectEventEncoder;
import com.exadbe.protocol.TradeEventEncoder;
import com.exadbe.snapshot.SnapshotManager;
import com.exadbe.telemetry.CoreMetrics;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The excoredum clustered service: a deterministic, single-writer state machine
 * that applies commands, replies with a {@link CommandResultEncoder}, and (from
 * phase 4) supports native snapshotting.
 *
 * <p>All mutation happens on the single {@code ClusteredServiceAgent} thread, so
 * no locks are used. The only permitted time source is the leader-assigned
 * {@code timestamp} passed to {@link #onSessionMessage}.
 */
public final class MatchingService implements ClusteredService {

    private static final int EGRESS_BUFFER_LENGTH = 128;
    private static final int EVENT_BUFFER_LENGTH = 128;

    private final MatchingEngine engine;
    private final CoreMetrics metrics;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final MessageHeaderEncoder resultHeaderEncoder = new MessageHeaderEncoder();
    private final CommandResultEncoder resultEncoder = new CommandResultEncoder();
    private final CommandOutcome outcome;
    private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[EGRESS_BUFFER_LENGTH]);

    private final MessageHeaderEncoder eventHeaderEncoder = new MessageHeaderEncoder();
    private final TradeEventEncoder tradeEncoder = new TradeEventEncoder();
    private final ReduceEventEncoder reduceEncoder = new ReduceEventEncoder();
    private final RejectEventEncoder rejectEncoder = new RejectEventEncoder();
    private final UnsafeBuffer eventBuffer = new UnsafeBuffer(new byte[EVENT_BUFFER_LENGTH]);

    private final MessageHeaderEncoder l2HeaderEncoder = new MessageHeaderEncoder();
    private final L2MarketDataEncoder l2Encoder = new L2MarketDataEncoder();
    private final UnsafeBuffer l2Buffer;

    private final SnapshotManager snapshotManager = new SnapshotManager();

    private Cluster cluster;
    private IdleStrategy idleStrategy;

    public MatchingService(final CoreConfig config, final CoreMetrics metrics) {
        this.metrics = metrics;
        this.engine = new MatchingEngine(config, metrics);
        this.outcome = new CommandOutcome(config.eventBufferCapacity());
        this.l2Buffer = new UnsafeBuffer(new byte[128 + config.l2MaxLevels() * 48]);
    }

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        // No per-session state in phase 0.
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        // No per-session state in phase 0.
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        messageHeaderDecoder.wrap(buffer, offset);
        if (messageHeaderDecoder.templateId() != CommandEnvelopeDecoder.TEMPLATE_ID) {
            // Not a command we recognise; ignore rather than corrupt state.
            return;
        }

        envelopeDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

        final OrderCommandType commandType = envelopeDecoder.commandType();
        final boolean duplicate = engine.process(envelopeDecoder, timestamp, outcome);
        sendResult(session);
        if (!duplicate) {
            emitEvents(session, timestamp);
            if (commandType == OrderCommandType.ORDER_BOOK_REQUEST) {
                sendL2(session);
            }
        }
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        // No timers in phase 0.
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        final long start = cluster.time();
        engine.writeSnapshot(
                snapshotManager,
                (recordBuffer, recordOffset, recordLength) ->
                        offerToPublication(snapshotPublication, recordBuffer, recordOffset, recordLength),
                idleStrategy::idle,
                cluster.logPosition());
        metrics.snapshotWriteMillis(cluster.time() - start);
        metrics.onSnapshotTaken();
    }

    private void loadSnapshot(final Image snapshotImage) {
        final long start = cluster.time();
        engine.beginSnapshotLoad(snapshotManager);
        while (!snapshotManager.loadComplete()) {
            final int fragments = snapshotImage.poll(this::onSnapshotFragment, 32);
            if (fragments == 0) {
                if (snapshotImage.isEndOfStream()) {
                    break;
                }
                idleStrategy.idle();
            }
        }
        // A corrupt or truncated snapshot must never become committed state; fail
        // fast so the node aborts recovery rather than serving broken state.
        if (!snapshotManager.verifyInvariant()) {
            throw new IllegalStateException("Snapshot integrity check failed: checksum mismatch or footer missing");
        }
        metrics.snapshotReadMillis(cluster.time() - start);
        metrics.onSnapshotLoaded();
    }

    private void onSnapshotFragment(
            final DirectBuffer buffer, final int offset, final int length, final Header header) {
        snapshotManager.onRecord(buffer, offset);
    }

    private void offerToPublication(
            final ExclusivePublication publication, final DirectBuffer buffer, final int offset, final int length) {
        idleStrategy.reset();
        while (true) {
            final long result = publication.offer(buffer, offset, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("Snapshot publication unavailable: " + result);
            }
            idleStrategy.idle();
        }
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        // No role-specific behaviour in phase 0.
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        // No resources to release in phase 0.
    }

    private void sendResult(final ClientSession session) {
        resultEncoder
                .wrapAndApplyHeader(egressBuffer, 0, resultHeaderEncoder)
                .commandIdHi(outcome.commandIdHi())
                .commandIdLo(outcome.commandIdLo())
                .resultCode(outcome.resultCode())
                .uid(outcome.hasUid() ? outcome.uid() : CommandResultEncoder.uidNullValue())
                .orderId(outcome.hasOrderId() ? outcome.orderId() : CommandResultEncoder.orderIdNullValue())
                .filledSize(
                        outcome.hasFilledSize() ? outcome.filledSize() : CommandResultEncoder.filledSizeNullValue());

        final int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + resultEncoder.encodedLength();
        offerToSession(session, egressBuffer, msgLength);
    }

    private void emitEvents(final ClientSession session, final long timestamp) {
        final int count = outcome.eventCount();
        for (int i = 0; i < count; i++) {
            final CommandOutcome.EventRecord e = outcome.event(i);
            final int length;
            switch (e.kind()) {
                case TRADE -> {
                    tradeEncoder
                            .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                            .commandIdHi(outcome.commandIdHi())
                            .commandIdLo(outcome.commandIdLo())
                            .eventIndex(i)
                            .timestamp(timestamp)
                            .symbolId(e.symbolId())
                            .makerOrderId(e.makerOrderId())
                            .makerUid(e.makerUid())
                            .takerUid(e.takerUid())
                            .price(e.price())
                            .size(e.size())
                            .makerOrderCompleted((short) (e.makerCompleted() ? 1 : 0));
                    length = MessageHeaderEncoder.ENCODED_LENGTH + tradeEncoder.encodedLength();
                }
                case REDUCE -> {
                    reduceEncoder
                            .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                            .commandIdHi(outcome.commandIdHi())
                            .commandIdLo(outcome.commandIdLo())
                            .eventIndex(i)
                            .timestamp(timestamp)
                            .symbolId(e.symbolId())
                            .orderId(e.makerOrderId())
                            .uid(e.makerUid())
                            .reducedBy(e.size());
                    length = MessageHeaderEncoder.ENCODED_LENGTH + reduceEncoder.encodedLength();
                }
                default -> {
                    rejectEncoder
                            .wrapAndApplyHeader(eventBuffer, 0, eventHeaderEncoder)
                            .commandIdHi(outcome.commandIdHi())
                            .commandIdLo(outcome.commandIdLo())
                            .eventIndex(i)
                            .timestamp(timestamp)
                            .symbolId(e.symbolId())
                            .orderId(e.makerOrderId())
                            .uid(e.makerUid())
                            .rejectedSize(e.size());
                    length = MessageHeaderEncoder.ENCODED_LENGTH + rejectEncoder.encodedLength();
                }
            }
            offerToSession(session, eventBuffer, length);
        }
    }

    private void sendL2(final ClientSession session) {
        final L2View view = engine.l2();
        final int askDepth = view.askDepth();
        final int bidDepth = view.bidDepth();

        l2Encoder
                .wrapAndApplyHeader(l2Buffer, 0, l2HeaderEncoder)
                .commandIdHi(outcome.commandIdHi())
                .commandIdLo(outcome.commandIdLo())
                .symbolId(envelopeDecoder.symbolId());

        final L2MarketDataEncoder.AsksEncoder asks = l2Encoder.asksCount(askDepth);
        for (int i = 0; i < askDepth; i++) {
            asks.next().price(view.askPrice(i)).size(view.askVolume(i)).orders(view.askOrders(i));
        }
        final L2MarketDataEncoder.BidsEncoder bids = l2Encoder.bidsCount(bidDepth);
        for (int i = 0; i < bidDepth; i++) {
            bids.next().price(view.bidPrice(i)).size(view.bidVolume(i)).orders(view.bidOrders(i));
        }

        offerToSession(session, l2Buffer, MessageHeaderEncoder.ENCODED_LENGTH + l2Encoder.encodedLength());
    }

    private void offerToSession(final ClientSession session, final UnsafeBuffer buffer, final int length) {
        idleStrategy.reset();
        while (true) {
            final long result = session.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED
                    || result == Publication.MAX_POSITION_EXCEEDED
                    || result == Publication.NOT_CONNECTED) {
                // Session gone; nothing to deliver to. Do not spin forever.
                metrics.onBackpressure();
                return;
            }
            metrics.onBackpressure();
            idleStrategy.idle();
        }
    }
}
