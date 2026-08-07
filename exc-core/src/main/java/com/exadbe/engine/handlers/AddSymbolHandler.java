package com.exadbe.engine.handlers;

import com.exadbe.core.CommandOutcome;
import com.exadbe.engine.risk.SymbolSpec;
import com.exadbe.engine.risk.SymbolSpecStore;
import com.exadbe.protocol.CommandResultCode;

/** Registers a spot symbol specification. A duplicate symbolId is rejected. */
public final class AddSymbolHandler {

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
            final CommandOutcome out) {
        final boolean added =
                symbols.add(new SymbolSpec(symbolId, baseCurrency, quoteCurrency, baseScaleK, quoteScaleK));
        out.resultCode(added ? CommandResultCode.SUCCESS : CommandResultCode.DUPLICATE);
    }
}
