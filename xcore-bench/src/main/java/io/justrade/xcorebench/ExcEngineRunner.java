package io.justrade.xcorebench;

import io.justrade.bench.LatencyResult;
import io.justrade.config.CoreConfig;
import io.justrade.core.CommandOutcome;
import io.justrade.engine.MatchingEngine;
import io.justrade.protocol.CommandEnvelopeDecoder;
import io.justrade.protocol.CommandEnvelopeEncoder;
import io.justrade.protocol.CommandResultCode;
import io.justrade.protocol.MessageHeaderDecoder;
import io.justrade.protocol.MessageHeaderEncoder;
import io.justrade.protocol.OrderAction;
import io.justrade.protocol.OrderCommandType;
import io.justrade.protocol.OrderType;
import io.justrade.telemetry.CoreMetrics;
import org.HdrHistogram.Histogram;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Single-thread closed-loop dispatch latency through justrade's full engine
 * path: SBE decode, dedup, symbol/user checks, risk reserve/settle, matching,
 * and dedup store - everything the clustered service does per command except
 * the Aeron transport and consensus. Commands are pre-encoded into one buffer
 * so the steady-state loop is allocation-free.
 */
public final class ExcEngineRunner {

    private static final int SYMBOL = BookComparison.SYMBOL_ID;
    // Must match WorkloadGenerator.spotSymbol so both engines trade the same pair.
    private static final int BASE = 11;
    private static final int QUOTE = 15;
    private static final long MAKER = 1L;
    private static final long TAKER = 2L;
    private static final long PRICE = 100L;
    private static final long CLIENT = 7L;
    private static final int STRIDE = 192;
    private static final int SETUP_COMMANDS = 6;

    private ExcEngineRunner() {}

    /** Runs warmup plus measured taker fills against one deep resting maker. */
    public static LatencyResult run(final int warmupOps, final int measureOps) {
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final CommandOutcome outcome = new CommandOutcome(1024);

        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final CommandEnvelopeEncoder encoder = new CommandEnvelopeEncoder();
        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final CommandEnvelopeDecoder decoder = new CommandEnvelopeDecoder();

        final int totalOps = warmupOps + measureOps;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[(totalOps + SETUP_COMMANDS) * STRIDE]);
        final EncoderSession session = new EncoderSession(buffer, encoder, headerEncoder);

        final long makerSize = (long) totalOps + 16L;
        encodeAddSymbol(session);
        encodeAddUser(session, MAKER);
        encodeAddUser(session, TAKER);
        encodeAdjust(session, MAKER, BASE, makerSize);
        encodeAdjust(session, TAKER, QUOTE, (long) totalOps * PRICE + 1_000L);
        encodePlace(session, 1L, true, makerSize, MAKER);

        // The measured commands: one full taker fill each, pre-encoded.
        final int[] takerOffsets = new int[totalOps];
        for (int i = 0; i < totalOps; i++) {
            takerOffsets[i] = session.peekOffset();
            encodePlace(session, 2L + i, false, 1L, TAKER);
        }

        for (int i = 0; i < SETUP_COMMANDS; i++) {
            process(engine, decoder, headerDecoder, buffer, i * STRIDE, outcome);
        }
        for (int i = 0; i < warmupOps; i++) {
            process(engine, decoder, headerDecoder, buffer, takerOffsets[i], outcome);
        }

        final Histogram histogram = new Histogram(1L, 60_000_000_000L, 3);
        final long began = System.nanoTime();
        for (int i = 0; i < measureOps; i++) {
            final long t0 = System.nanoTime();
            process(engine, decoder, headerDecoder, buffer, takerOffsets[warmupOps + i], outcome);
            histogram.recordValue(System.nanoTime() - t0);
        }
        final long elapsedNanos = System.nanoTime() - began;

        final double throughput = measureOps / (elapsedNanos / 1_000_000_000.0);
        return new LatencyResult(
                measureOps,
                throughput,
                histogram.getValueAtPercentile(50.0),
                histogram.getValueAtPercentile(99.0),
                histogram.getValueAtPercentile(99.9),
                histogram.getMaxValue());
    }

    private static void process(
            final MatchingEngine engine,
            final CommandEnvelopeDecoder decoder,
            final MessageHeaderDecoder headerDecoder,
            final UnsafeBuffer buffer,
            final int offset,
            final CommandOutcome outcome) {
        headerDecoder.wrap(buffer, offset);
        decoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        engine.process(decoder, 1L, outcome);
        if (outcome.resultCode() != CommandResultCode.SUCCESS) {
            throw new IllegalStateException("command failed: " + outcome.resultCode());
        }
    }

    private static void encodeAddSymbol(final EncoderSession s) {
        begin(s, OrderCommandType.ADD_SYMBOL)
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
    }

    private static void encodeAddUser(final EncoderSession s, final long uid) {
        begin(s, OrderCommandType.ADD_USER)
                .uid(uid)
                .symbolId(CommandEnvelopeEncoder.symbolIdNullValue())
                .orderId(CommandEnvelopeEncoder.orderIdNullValue())
                .price(CommandEnvelopeEncoder.priceNullValue())
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(CommandEnvelopeEncoder.sizeNullValue())
                .action(OrderAction.NULL_VAL)
                .orderType(OrderType.NULL_VAL)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue())
                .takerFee(CommandEnvelopeEncoder.takerFeeNullValue())
                .makerFee(CommandEnvelopeEncoder.makerFeeNullValue());
    }

    private static void encodeAdjust(final EncoderSession s, final long uid, final int currency, final long amount) {
        begin(s, OrderCommandType.BALANCE_ADJUSTMENT)
                .uid(uid)
                .symbolId(CommandEnvelopeEncoder.symbolIdNullValue())
                .orderId(CommandEnvelopeEncoder.orderIdNullValue())
                .price(CommandEnvelopeEncoder.priceNullValue())
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(CommandEnvelopeEncoder.sizeNullValue())
                .action(OrderAction.NULL_VAL)
                .orderType(OrderType.NULL_VAL)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(currency)
                .balanceAmount(amount)
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue())
                .takerFee(CommandEnvelopeEncoder.takerFeeNullValue())
                .makerFee(CommandEnvelopeEncoder.makerFeeNullValue());
    }

    private static void encodePlace(
            final EncoderSession s, final long orderId, final boolean ask, final long size, final long uid) {
        begin(s, OrderCommandType.PLACE_ORDER)
                .uid(uid)
                .symbolId(SYMBOL)
                .orderId(orderId)
                .price(PRICE)
                .reserveBidPrice(ask ? CommandEnvelopeEncoder.reserveBidPriceNullValue() : PRICE)
                .size(size)
                .action(ask ? OrderAction.ASK : OrderAction.BID)
                .orderType(OrderType.GTC)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue())
                .takerFee(CommandEnvelopeEncoder.takerFeeNullValue())
                .makerFee(CommandEnvelopeEncoder.makerFeeNullValue());
    }

    private static CommandEnvelopeEncoder begin(final EncoderSession s, final OrderCommandType type) {
        final long seq = ++s.seq;
        s.offset = s.nextOffset();
        return s.encoder
                .wrapAndApplyHeader(s.buffer, s.offset, s.headerEncoder)
                .clientId(CLIENT)
                .clientSeq(seq)
                .commandIdHi(CLIENT)
                .commandIdLo(seq)
                .commandType(type);
    }

    /** Tracks the write cursor and monotonic client sequence during encoding. */
    private static final class EncoderSession {
        final UnsafeBuffer buffer;
        final CommandEnvelopeEncoder encoder;
        final MessageHeaderEncoder headerEncoder;
        long seq;
        int commands;
        int offset;

        EncoderSession(
                final UnsafeBuffer buffer,
                final CommandEnvelopeEncoder encoder,
                final MessageHeaderEncoder headerEncoder) {
            this.buffer = buffer;
            this.encoder = encoder;
            this.headerEncoder = headerEncoder;
        }

        int peekOffset() {
            return commands * STRIDE;
        }

        int nextOffset() {
            return commands++ * STRIDE;
        }
    }
}
