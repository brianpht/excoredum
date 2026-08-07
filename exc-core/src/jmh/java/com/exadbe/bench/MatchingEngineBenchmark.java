package com.exadbe.bench;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.MatchingEngine;
import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandEnvelopeEncoder;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderAction;
import com.exadbe.protocol.OrderCommandType;
import com.exadbe.protocol.OrderType;
import com.exadbe.telemetry.CoreMetrics;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Full-dispatch micro-benchmark: a place-then-cancel round-trip through
 * {@link MatchingEngine#process} including dedup, symbol/user checks, risk
 * reserve/release, and pooled book insert/remove. Run with {@code -prof gc} to
 * assert zero steady-state allocation per command.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class MatchingEngineBenchmark {

    private static final long CLIENT = 7L;
    private static final int SYMBOL = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long UID = 1L;

    private MatchingEngine engine;
    private CommandOutcome outcome;
    private MessageHeaderEncoder headerEncoder;
    private CommandEnvelopeEncoder encoder;
    private MessageHeaderDecoder headerDecoder;
    private CommandEnvelopeDecoder decoder;
    private UnsafeBuffer buffer;
    private long seq;

    @Setup(Level.Trial)
    public void setup() {
        engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        outcome = new CommandOutcome(1024);
        headerEncoder = new MessageHeaderEncoder();
        encoder = new CommandEnvelopeEncoder();
        headerDecoder = new MessageHeaderDecoder();
        decoder = new CommandEnvelopeDecoder();
        buffer = new UnsafeBuffer(new byte[256]);

        process(encodeAddSymbol(nextSeq()));
        process(encodeAddUser(nextSeq()));
        process(encodeAdjust(nextSeq(), QUOTE, 1_000_000_000_000L));
    }

    @Benchmark
    public boolean placeThenCancel() {
        final long orderId = ++seq;
        final boolean dup = process(encodePlaceBid(nextSeq(), orderId));
        process(encodeCancel(nextSeq(), orderId));
        return dup;
    }

    private boolean process(final CommandEnvelopeDecoder cmd) {
        return engine.process(cmd, 1L, outcome);
    }

    private long nextSeq() {
        return ++seq;
    }

    private CommandEnvelopeDecoder encodePlaceBid(final long s, final long orderId) {
        begin(s, OrderCommandType.PLACE_ORDER)
                .uid(UID)
                .symbolId(SYMBOL)
                .orderId(orderId)
                .price(50L)
                .reserveBidPrice(50L)
                .size(10L)
                .action(OrderAction.BID)
                .orderType(OrderType.GTC)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue());
        return wrap();
    }

    private CommandEnvelopeDecoder encodeCancel(final long s, final long orderId) {
        begin(s, OrderCommandType.CANCEL_ORDER)
                .uid(UID)
                .symbolId(SYMBOL)
                .orderId(orderId)
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
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue());
        return wrap();
    }

    private CommandEnvelopeDecoder encodeAddSymbol(final long s) {
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
                .quoteScaleK(1L);
        return wrap();
    }

    private CommandEnvelopeDecoder encodeAddUser(final long s) {
        begin(s, OrderCommandType.ADD_USER)
                .uid(UID)
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
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue());
        return wrap();
    }

    private CommandEnvelopeDecoder encodeAdjust(final long s, final int currency, final long amount) {
        begin(s, OrderCommandType.BALANCE_ADJUSTMENT)
                .uid(UID)
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
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue());
        return wrap();
    }

    private CommandEnvelopeEncoder begin(final long s, final OrderCommandType type) {
        return encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(CLIENT)
                .clientSeq(s)
                .commandIdHi(CLIENT)
                .commandIdLo(s)
                .commandType(type);
    }

    private CommandEnvelopeDecoder wrap() {
        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return decoder;
    }
}
