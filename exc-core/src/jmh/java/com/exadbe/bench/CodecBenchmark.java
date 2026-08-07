package com.exadbe.bench;

import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandEnvelopeEncoder;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderAction;
import com.exadbe.protocol.OrderCommandType;
import com.exadbe.protocol.OrderType;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Wire-format micro-benchmarks: encoding and decoding a {@link CommandEnvelopeDecoder}.
 * Run with {@code -prof gc} to assert zero steady-state allocation per operation.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class CodecBenchmark {

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CommandEnvelopeEncoder encoder = new CommandEnvelopeEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommandEnvelopeDecoder decoder = new CommandEnvelopeDecoder();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    @Setup
    public void setup() {
        encodePlace(1L, 1L);
    }

    private void encodePlace(final long seq, final long orderId) {
        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(7L)
                .clientSeq(seq)
                .commandIdHi(7L)
                .commandIdLo(seq)
                .commandType(OrderCommandType.PLACE_ORDER)
                .uid(1L)
                .symbolId(1)
                .orderId(orderId)
                .price(100L)
                .reserveBidPrice(100L)
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
    }

    @Benchmark
    public long decodeEnvelope() {
        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return decoder.clientId() + decoder.clientSeq() + decoder.orderId() + decoder.price() + decoder.size();
    }

    @Benchmark
    public UnsafeBuffer encodeEnvelope() {
        encodePlace(2L, 2L);
        return buffer;
    }
}
