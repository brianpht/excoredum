package io.justrade.xcorebench;

import exchange.core2.collections.objpool.ObjectsPool;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.LoggingConfiguration;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.orderbook.OrderBookDirectImpl;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import exchange.core2.core.orderbook.OrderBookNaiveImpl;
import io.justrade.core.CommandOutcome;
import io.justrade.engine.orderbook.OrderBookNaive;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Matching-level comparison under JMH: the same shapes measured against
 * justrade's {@link OrderBookNaive} and both exchange-core implementations.
 * {@code replayChunk} mirrors exchange-core's ITOrderBookBase shape (fresh book
 * plus full workload replay per invocation); the micro-shapes mirror
 * core's own OrderBookBenchmark so both projects' numbers are relatable.
 *
 * <p>Run with {@code -Pjmh.profilers=gc}: the justrade implementations are
 * expected to allocate zero in steady state; exchange-core's are not.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class OrderBookComparisonBenchmark {

    /** Which order-book implementation the shapes run against. */
    public enum Impl {
        JUSTRADE_NAIVE,
        XCORE_NAIVE,
        XCORE_DIRECT
    }

    private static final int SYMBOL = BookComparison.SYMBOL_ID;
    private static final long TAKER_UID = 2L;

    @Param
    private Impl impl;

    private OrderBookNaive justradePlaceCancelBook;
    private OrderBookNaive justradeMatchBook;
    private IOrderBook xcorePlaceCancelBook;
    private IOrderBook xcoreMatchBook;
    private CommandOutcome outcome;
    private OrderCommand placeCmd;
    private OrderCommand cancelCmd;
    private Workload replayChunk;
    private XcoreBookRunner.BookFactory replayFactory;
    private long orderId;

    @Setup(Level.Trial)
    public void setup() {
        outcome = new CommandOutcome(1024);
        replayChunk = WorkloadGenerator.generate(2048, 512, 256, SYMBOL, false, false, 42);

        if (impl == Impl.JUSTRADE_NAIVE) {
            justradePlaceCancelBook = new OrderBookNaive(SYMBOL);
            justradeMatchBook = new OrderBookNaive(SYMBOL);
            outcome.reset(0L, 0L);
            justradeMatchBook.placeGtc(1L, true, 100L, Long.MAX_VALUE / 4L, 0L, 99L, 0L, outcome);
        } else {
            xcorePlaceCancelBook = newBook();
            xcoreMatchBook = newBook();
            final OrderCommand seed = OrderCommand.builder()
                    .command(OrderCommandType.PLACE_ORDER)
                    .orderId(1L)
                    .uid(99L)
                    .price(100L)
                    .reserveBidPrice(0L)
                    .size(Long.MAX_VALUE / 4L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .symbol(SYMBOL)
                    .resultCode(CommandResultCode.VALID_FOR_MATCHING_ENGINE)
                    .build();
            IOrderBook.processCommand(xcoreMatchBook, seed);
            seed.matcherEvent = null;

            placeCmd = OrderCommand.builder().build();
            cancelCmd = OrderCommand.builder().build();
            replayFactory = new XcoreBookRunner.BookFactory() {
                @Override
                public IOrderBook create(final int symbolId) {
                    return newBook(symbolId);
                }

                @Override
                public String name() {
                    return impl.name();
                }
            };
        }
    }

    /** Resting place of a far-from-touch bid plus its cancel. */
    @Benchmark
    public long placeThenCancel() {
        final long id = ++orderId;
        if (impl == Impl.JUSTRADE_NAIVE) {
            outcome.reset(0L, id);
            justradePlaceCancelBook.placeGtc(id, false, 50L, 10L, 50L, 1L, 0L, outcome);
            justradePlaceCancelBook.cancel(id, 1L, outcome);
        } else {
            fillPlace(id);
            IOrderBook.processCommand(xcorePlaceCancelBook, placeCmd);
            placeCmd.matcherEvent = null;
            cancelCmd.command = OrderCommandType.CANCEL_ORDER;
            cancelCmd.orderId = id;
            cancelCmd.uid = 1L;
            cancelCmd.symbol = SYMBOL;
            cancelCmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            cancelCmd.matcherEvent = null;
            IOrderBook.processCommand(xcorePlaceCancelBook, cancelCmd);
            cancelCmd.matcherEvent = null;
        }
        return id;
    }

    /** One-lot IOC bid fully matching against a deep resting ask. */
    @Benchmark
    public long iocMatch() {
        final long id = ++orderId;
        if (impl == Impl.JUSTRADE_NAIVE) {
            outcome.reset(0L, id);
            return justradeMatchBook.matchIoc(id, false, 100L, 1L, 100L, TAKER_UID, outcome);
        }
        fillPlace(id);
        placeCmd.orderType = OrderType.IOC;
        placeCmd.price = 100L;
        placeCmd.reserveBidPrice = 100L;
        placeCmd.size = 1L;
        placeCmd.uid = TAKER_UID;
        placeCmd.action = OrderAction.BID;
        final CommandResultCode code = IOrderBook.processCommand(xcoreMatchBook, placeCmd);
        placeCmd.matcherEvent = null;
        return code == CommandResultCode.SUCCESS ? id : -1L;
    }

    /** Fresh book plus full fill + benchmark replay, as ITOrderBookBase does. */
    @Benchmark
    public BookStats replayChunk() {
        if (impl == Impl.JUSTRADE_NAIVE) {
            return JustradeBookRunner.replay(replayChunk, SYMBOL);
        }
        return XcoreBookRunner.replay(replayChunk, SYMBOL, replayFactory);
    }

    private void fillPlace(final long id) {
        placeCmd.command = OrderCommandType.PLACE_ORDER;
        placeCmd.orderId = id;
        placeCmd.uid = 1L;
        placeCmd.price = 50L;
        placeCmd.reserveBidPrice = 50L;
        placeCmd.size = 10L;
        placeCmd.action = OrderAction.BID;
        placeCmd.orderType = OrderType.GTC;
        placeCmd.symbol = SYMBOL;
        placeCmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        placeCmd.matcherEvent = null;
    }

    private IOrderBook newBook() {
        return newBook(SYMBOL);
    }

    private IOrderBook newBook(final int symbolId) {
        if (impl == Impl.XCORE_NAIVE) {
            return new OrderBookNaiveImpl(WorkloadGenerator.spotSymbol(symbolId), LoggingConfiguration.DEFAULT);
        }
        return new OrderBookDirectImpl(
                WorkloadGenerator.spotSymbol(symbolId),
                ObjectsPool.createDefaultTestPool(),
                OrderBookEventsHelper.NON_POOLED_EVENTS_HELPER,
                LoggingConfiguration.DEFAULT);
    }
}
