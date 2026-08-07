package com.exadbe.engine.risk;

import com.exadbe.collections.AccountStore;

/**
 * Direct-exchange (spot) risk: integer-only fund reservation and settlement.
 * No fees, no margin (both deferred). All math is signed 64-bit; the caller is
 * responsible for capacity limits.
 */
public final class DirectExchangeRisk {

    private final AccountStore accounts;

    public DirectExchangeRisk(final AccountStore accounts) {
        this.accounts = accounts;
    }

    /** Quote held for a bid: {@code size * reserveBidPrice * quoteScaleK}. */
    public static long bidHold(final SymbolSpec s, final long size, final long reserveBidPrice) {
        return size * reserveBidPrice * s.quoteScaleK();
    }

    /** Quote held for a fill-or-kill budget bid: {@code budget * quoteScaleK}. */
    public static long bidBudgetHold(final SymbolSpec s, final long budget) {
        return budget * s.quoteScaleK();
    }

    /** Base held for an ask: {@code size * baseScaleK}. */
    public static long askHold(final SymbolSpec s, final long size) {
        return size * s.baseScaleK();
    }

    /**
     * Speculatively reserves {@code amount} of {@code currency} from {@code uid};
     * reverts and returns {@code false} if it would overdraw.
     */
    public boolean reserve(final long uid, final int currency, final long amount) {
        final long newBalance = accounts.addToValue(uid, currency, -amount);
        if (newBalance < 0L) {
            accounts.addToValue(uid, currency, amount);
            return false;
        }
        return true;
    }

    /** Releases a previously held amount back to available balance. */
    public void release(final long uid, final int currency, final long amount) {
        if (amount != 0L) {
            accounts.addToValue(uid, currency, amount);
        }
    }

    /** Settles one fill for the resting maker at the actual fill price. */
    public void settleMaker(
            final SymbolSpec s,
            final boolean makerBid,
            final long makerUid,
            final long makerReserveBidPrice,
            final long price,
            final long size) {
        if (makerBid) {
            accounts.addToValue(makerUid, s.baseCurrency(), size * s.baseScaleK());
            accounts.addToValue(makerUid, s.quoteCurrency(), size * (makerReserveBidPrice - price) * s.quoteScaleK());
        } else {
            accounts.addToValue(makerUid, s.quoteCurrency(), size * price * s.quoteScaleK());
        }
    }

    /**
     * Settles a buying taker: gains base, releases the over-reserved quote.
     *
     * @param heldPriceSum reserved quote-steps ({@code reserveBidPrice * sizeSum}
     *     for a limit order, or the budget for a FOK-budget order)
     */
    public void settleTakerBuy(
            final SymbolSpec s,
            final long takerUid,
            final long heldPriceSum,
            final long sizePriceSum,
            final long sizeSum) {
        accounts.addToValue(takerUid, s.baseCurrency(), sizeSum * s.baseScaleK());
        accounts.addToValue(takerUid, s.quoteCurrency(), (heldPriceSum - sizePriceSum) * s.quoteScaleK());
    }

    /** Settles a selling taker: gains quote at the actual fill prices. */
    public void settleTakerSell(final SymbolSpec s, final long takerUid, final long sizePriceSum) {
        accounts.addToValue(takerUid, s.quoteCurrency(), sizePriceSum * s.quoteScaleK());
    }
}
