package com.exadbe.engine.handlers;

import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.risk.SymbolSpec;
import com.exadbe.engine.risk.SymbolSpecStore;
import com.exadbe.protocol.CommandResultCode;

/**
 * Registers a spot symbol specification. A duplicate symbolId is rejected.
 *
 * <p>Spec sanity is validated up front because every money-math bound in the
 * risk path depends on it: positive scale factors, non-negative fees with
 * maker not exceeding taker (a resting bid's refund differential stays
 * non-negative), and distinct base / quote currencies.
 */
public final class AddSymbolHandler {

    /**
     * Upper bound for scale factors. Money math multiplies {@code size * price *
     * scaleK}, so an unbounded scale factor makes the placement-time overflow
     * guards the only defence; capping it keeps every resting hold within a
     * documented range and rejects degenerate specs up front.
     */
    public static final long MAX_SCALE_K = 1_000_000_000_000L;

    /**
     * Upper bound for per-lot fees (quote units). Fees multiply against swept
     * size, so the same reasoning as {@link #MAX_SCALE_K} applies.
     */
    public static final long MAX_FEE = 1_000_000_000_000L;

    private final SymbolSpecStore symbols;

    public AddSymbolHandler(final SymbolSpecStore symbols) {
        this.symbols = symbols;
    }

    public void handle(
            final int symbolId,
            final int baseCurrency,
            final int quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK,
            final long takerFee,
            final long makerFee,
            final CommandOutcome out) {
        if (baseScaleK <= 0L
                || quoteScaleK <= 0L
                || baseScaleK > MAX_SCALE_K
                || quoteScaleK > MAX_SCALE_K
                || takerFee < 0L
                || makerFee < 0L
                || takerFee > MAX_FEE
                || makerFee > MAX_FEE
                || makerFee > takerFee
                || baseCurrency == quoteCurrency) {
            out.resultCode(CommandResultCode.INVALID_AMOUNT);
            return;
        }
        final boolean added = symbols.add(
                new SymbolSpec(symbolId, baseCurrency, quoteCurrency, baseScaleK, quoteScaleK, takerFee, makerFee));
        out.resultCode(added ? CommandResultCode.SUCCESS : CommandResultCode.DUPLICATE);
    }
}
