package com.exadbe.engine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.config.CoreConfig;
import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.risk.DirectExchangeRisk;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.snapshot.SnapshotManager;
import com.exadbe.telemetry.CoreMetrics;
import com.exadbe.testkit.InMemorySnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Maker/taker fees: charged per lot in quote, accrued to the fee account, value conserved. */
class FeeTest {

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
    private long seq;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        out = new CommandOutcome();
        cmd = new Commands();
        seq = 1L;
        run(cmd.addSymbol(1L, seq, seq, SYM, BASE, QUOTE, 1L, 1L, TAKER_FEE, MAKER_FEE));
    }

    @Test
    void feesChargedToBothSidesAndConserved() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 6L, 0L, MAKER); // ask fully filled, no remainder

        placeGtc(2L, false, 100L, 6L, 100L, TAKER);
        assertEquals(CommandResultCode.SUCCESS, out.resultCode());

        // Taker paid 6*100 + takerFee 2*6 = 612; keeps the 6 base bought.
        assertEquals(6L, engine.balance(TAKER, BASE));
        assertEquals(99_388L, engine.balance(TAKER, QUOTE));
        // Maker sold 6 base, received 6*100 - makerFee 1*6 = 594.
        assertEquals(994L, engine.balance(MAKER, BASE));
        assertEquals(594L, engine.balance(MAKER, QUOTE));
        // Fee account collected (takerFee + makerFee) * 6 = 18.
        assertEquals(18L, engine.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));

        // Value conservation across all three accounts.
        final long feeUid = DirectExchangeRisk.FEE_ACCOUNT_UID;
        assertEquals(
                100_000L, engine.balance(MAKER, QUOTE) + engine.balance(TAKER, QUOTE) + engine.balance(feeUid, QUOTE));
        assertEquals(1000L, engine.balance(MAKER, BASE) + engine.balance(TAKER, BASE));
    }

    @Test
    void takerBuyOverReserveReleasedFeeStaysConsumed() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 6L, 0L, MAKER);

        // Reserve at 120 but fill at 100: (120-100)*6 quote released, takerFee still charged.
        placeGtc(2L, false, 100L, 6L, 120L, TAKER);

        assertEquals(6L, engine.balance(TAKER, BASE));
        assertEquals(99_388L, engine.balance(TAKER, QUOTE)); // 100000 - 600 - 12 fee
        assertEquals(18L, engine.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));
    }

    @Test
    void feeAccountUidIsReserved() {
        run(cmd.addUser(1L, seq, seq, DirectExchangeRisk.FEE_ACCOUNT_UID));
        assertEquals(CommandResultCode.USER_ALREADY_EXISTS, out.resultCode());
    }

    @Test
    void askPriceBelowTakerFeeRejected() {
        final int sym2 = 2;
        run(cmd.addSymbol(1L, seq, seq, sym2, BASE, QUOTE, 1L, 1L, 200L, 1L));
        fund(MAKER, BASE, 1000L);

        run(cmd.placeGtc(1L, seq, seq, sym2, 1L, true, 100L, 6L, 0L, MAKER));

        assertEquals(CommandResultCode.RISK_ASK_PRICE_LOWER_THAN_FEE, out.resultCode());
    }

    @Test
    void snapshotRoundTripPreservesFeesAndFeeAccount() {
        fund(MAKER, BASE, 1000L);
        fund(TAKER, QUOTE, 100_000L);
        placeGtc(1L, true, 100L, 6L, 0L, MAKER);
        placeGtc(2L, false, 100L, 6L, 100L, TAKER);
        assertEquals(18L, engine.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));

        final InMemorySnapshot snapshot = new InMemorySnapshot();
        snapshot.writeFrom(new SnapshotManager(), engine, 42L);

        final MatchingEngine restored = new MatchingEngine(CoreConfig.defaults(), new CoreMetrics());
        final SnapshotManager readManager = new SnapshotManager();
        snapshot.readInto(readManager, restored);

        assertTrue(readManager.verifyInvariant(), "restored state must reproduce the footer checksum");
        assertEquals(18L, restored.balance(DirectExchangeRisk.FEE_ACCOUNT_UID, QUOTE));

        final InMemorySnapshot reSerialized = new InMemorySnapshot();
        reSerialized.writeFrom(new SnapshotManager(), restored, 42L);
        assertArrayEquals(snapshot.toByteArray(), reSerialized.toByteArray());
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
