package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.risk.DirectExchangeRisk;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.read.report.ReportGenerator;
import com.exadbe.read.report.SingleUserReport;
import com.exadbe.read.report.TotalCurrencyBalance;
import com.exadbe.snapshot.SnapshotManager;
import com.exadbe.telemetry.CoreMetrics;
import com.exadbe.testkit.InMemorySnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Read-side report framework: single-user view, per-currency conservation, and state fingerprint. */
class ReportGeneratorTest {

    private static final int SYM = 1;
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final long MAKER = 1L;
    private static final long TAKER = 2L;
    private static final long TAKER_FEE = 2L;
    private static final long MAKER_FEE = 1L;

    private MatchingEngine engine;
    private CommandOutcome out;
    private Commands cmd;
    private ReportGenerator reports;
    private long seq;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        out = new CommandOutcome();
        cmd = new Commands();
        reports = new ReportGenerator(engine);
        seq = 1L;
        run(cmd.addSymbol(1L, seq, seq, SYM, BASE, QUOTE, 1L, 1L, TAKER_FEE, MAKER_FEE));
    }

    @Test
    void singleUserReportListsBalancesAndRestingOrders() {
        fund(MAKER, BASE, 1000L);
        placeGtc(1L, true, 100L, 10L, 0L, MAKER); // ask rests on an empty book

        final SingleUserReport report = reports.singleUser(MAKER);

        assertTrue(report.exists());
        assertEquals(MAKER, report.uid());
        // 10 base reserved by the resting ask; 990 remains free.
        assertEquals(990L, report.balance(BASE));
        assertEquals(0L, report.balance(QUOTE));
        assertEquals(1, report.orders().size());
        final SingleUserReport.OrderLine order = report.orders().get(0);
        assertEquals(SYM, order.symbolId());
        assertEquals(1L, order.orderId());
        assertTrue(order.ask());
        assertEquals(10L, order.size());
        assertEquals(0L, order.filled());
    }

    @Test
    void singleUserReportForUnknownUser() {
        final SingleUserReport report = reports.singleUser(999L);

        assertFalse(report.exists());
        assertEquals(0L, report.balance(BASE));
        assertTrue(report.orders().isEmpty());
    }

    @Test
    void totalCurrencyBalanceConservedAcrossAFeeTradeWithRestingRemainder() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 10L, 0L, MAKER); // maker ask 10 rests

        final TotalCurrencyBalance before = reports.totalCurrencyBalance();
        assertEquals(1000L, before.total(BASE), "all base is free balance plus the ask hold");
        assertEquals(100_000L, before.total(QUOTE));

        placeGtc(2L, false, 100L, 6L, 100L, TAKER); // taker buys 6, maker keeps 4 resting
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());
        assertEquals(18L, engine.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));

        final TotalCurrencyBalance after = reports.totalCurrencyBalance();
        // Trades and fees only move value between balances and holds; totals are invariant.
        assertEquals(1000L, after.total(BASE), "base conserved: balances plus remaining ask hold");
        assertEquals(100_000L, after.total(QUOTE), "quote conserved including fees on account 0");
    }

    @Test
    void stateHashIsDeterministicAndSensitive() {
        fund(MAKER, BASE, 1000L);
        placeGtc(1L, true, 100L, 10L, 0L, MAKER);

        final MatchingEngine other = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final Commands otherCmd = new Commands();
        final CommandOutcome otherOut = new CommandOutcome();
        long s = 1L;
        other.process(otherCmd.addSymbol(1L, s, s, SYM, BASE, QUOTE, 1L, 1L, TAKER_FEE, MAKER_FEE), 0L, otherOut);
        s++;
        other.process(otherCmd.addUser(1L, s, s, MAKER), 0L, otherOut);
        s++;
        other.process(otherCmd.adjust(1L, s, s, MAKER, BASE, 1000L), 0L, otherOut);
        s++;
        other.process(otherCmd.placeGtc(1L, s, s, SYM, 1L, true, 100L, 10L, 0L, MAKER), 0L, otherOut);

        assertEquals(engine.stateHash(), other.stateHash(), "identical command streams hash identically");

        other.process(otherCmd.adjust(1L, ++s, s, MAKER, QUOTE, 1L), 0L, otherOut);
        assertNotEquals(engine.stateHash(), other.stateHash(), "a diverging state must change the hash");
    }

    @Test
    void stateHashSurvivesSnapshotRoundTrip() {
        fund(MAKER, BASE, 1000L);
        placeGtc(1L, true, 100L, 10L, 0L, MAKER);
        final long hash = engine.stateHash();

        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), engine, 42L);
        final MatchingEngine restored = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        snapshot.readInto(new SnapshotManager(), restored);

        assertEquals(hash, restored.stateHash(), "the fingerprint is stable across a snapshot round trip");
    }

    private void fund(final long uid, final int currency, final long amount) {
        run(cmd.addUser(1L, seq, seq, uid));
        run(cmd.adjust(1L, seq, seq, uid, currency, amount));
    }

    private void placeGtc(
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long reserveBidPrice,
            final long uid) {
        run(cmd.placeGtc(1L, seq, seq, SYM, orderId, ask, price, size, reserveBidPrice, uid));
    }

    private void run(final com.exadbe.protocol.CommandEnvelopeDecoder decoded) {
        engine.process(decoded, 0L, out);
        seq++;
    }
}
