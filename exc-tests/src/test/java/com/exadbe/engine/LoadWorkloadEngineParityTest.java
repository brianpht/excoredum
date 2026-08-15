package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.exadbe.bench.LoadWorkload;
import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.orderbook.OrderBookNaive;
import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.telemetry.CoreMetrics;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Cross-validates the {@link LoadWorkload} simulation against the real
 * {@link MatchingEngine} command by command: replays the exact workload the
 * load runner submits and asserts, after every command, that the simulated
 * book (resting orders per side, FIFO) equals the engine's book and that the
 * engine acknowledged the command with {@code SUCCESS}. A mismatch means the
 * simulation no longer models the engine and the end-to-end test would fail.
 */
class LoadWorkloadEngineParityTest {

    @Test
    void simulatedBookMatchesEngineForEveryCommand() {
        final int ops = 100_000;
        final int users = 100;
        final LoadWorkload workload = new LoadWorkload(ops, users);
        final MatchingEngine engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final CommandOutcome outcome = new CommandOutcome();
        final Commands commands = new Commands();
        long clientSeq = 0L;
        long commandId = 0L;

        // Setup mirrors the load runner: one symbol with zero fees, then users
        // with the workload's funding.
        engine.process(
                commands.addSymbol(
                        1L,
                        clientSeq++,
                        commandId++,
                        LoadWorkload.SYMBOL,
                        LoadWorkload.BASE_CURRENCY,
                        LoadWorkload.QUOTE_CURRENCY,
                        1L,
                        1L),
                1_000L,
                outcome);
        for (long uid = 1L; uid <= users; uid++) {
            engine.process(commands.addUser(1L, clientSeq++, commandId++, uid), 1_000L, outcome);
            engine.process(
                    commands.adjust(
                            1L,
                            clientSeq++,
                            commandId++,
                            uid,
                            LoadWorkload.BASE_CURRENCY,
                            LoadWorkload.BASE_FUNDING_PER_USER),
                    1_000L,
                    outcome);
            engine.process(
                    commands.adjust(
                            1L,
                            clientSeq++,
                            commandId++,
                            uid,
                            LoadWorkload.QUOTE_CURRENCY,
                            LoadWorkload.QUOTE_FUNDING_PER_USER),
                    1_000L,
                    outcome);
        }

        for (int i = 0; i < ops; i++) {
            final LoadWorkload.Command command = workload.next(i);
            final CommandEnvelopeDecoder decoder = encode(commands, clientSeq++, commandId++, command);
            final boolean duplicate = engine.process(decoder, 1_000L + i, outcome);
            if (duplicate) {
                fail("iteration " + i + " was treated as a duplicate: " + command);
            }
            if (outcome.resultCode() != CommandResultCode.SUCCESS) {
                fail("iteration " + i + " result " + outcome.resultCode() + " for " + command);
            }
            assertBooksMatch(engine, workload, i, command);
        }
    }

    private static CommandEnvelopeDecoder encode(
            final Commands commands, final long clientSeq, final long commandId, final LoadWorkload.Command command) {
        return switch (command.type()) {
            case PLACE -> commands.placeGtc(
                    1L,
                    clientSeq,
                    commandId,
                    LoadWorkload.SYMBOL,
                    command.orderId(),
                    command.ask(),
                    LoadWorkload.PRICE,
                    1L,
                    command.reserveBidPrice(),
                    command.uid());
            case CANCEL -> commands.cancel(
                    1L, clientSeq, commandId, LoadWorkload.SYMBOL, command.orderId(), command.uid());
            case REDUCE -> commands.reduce(
                    1L, clientSeq, commandId, LoadWorkload.SYMBOL, command.orderId(), 1L, command.uid());
            case ORDER_BOOK -> commands.orderBookRequest(1L, clientSeq, commandId, LoadWorkload.SYMBOL, command.uid());
        };
    }

    private static void assertBooksMatch(
            final MatchingEngine engine, final LoadWorkload workload, final int i, final LoadWorkload.Command command) {
        final OrderBookNaive book = engine.book(LoadWorkload.SYMBOL);
        final List<BookEntry> engineAsks = new ArrayList<>();
        final List<BookEntry> engineBids = new ArrayList<>();
        book.forEachOrderSorted((orderId, ask, price, size, filled, reserveBidPrice, uid, timestamp) -> {
            (ask ? engineAsks : engineBids).add(new BookEntry(orderId, uid));
        });

        final List<BookEntry> simAsks = new ArrayList<>();
        for (final LoadWorkload.Resting resting : workload.restingAsks()) {
            simAsks.add(new BookEntry(resting.orderId(), resting.uid()));
        }
        final List<BookEntry> simBids = new ArrayList<>();
        for (final LoadWorkload.Resting resting : workload.restingBids()) {
            simBids.add(new BookEntry(resting.orderId(), resting.uid()));
        }

        if (!engineAsks.equals(simAsks) || !engineBids.equals(simBids)) {
            fail("book divergence at iteration " + i + " for " + command + "\nengine asks:  " + engineAsks
                    + "\nsim asks:     " + simAsks + "\nengine bids:  " + engineBids + "\nsim bids:     " + simBids);
        }
        assertEquals(book.orderCount(), simAsks.size() + simBids.size(), "book count at iteration " + i);
    }

    private record BookEntry(long orderId, long uid) {}
}
