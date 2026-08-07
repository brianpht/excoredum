package com.exadbe.engine.handlers;

import com.exadbe.collections.AccountStore;
import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandResultCode;
import com.exadbe.util.Amounts;

/**
 * Applies a signed balance delta to {@code (uid, currency)}. Integer-only;
 * overflow is reported via {@link CommandResultCode#OVERFLOW}, never thrown.
 */
public final class BalanceAdjustmentHandler {

    private final AccountStore accounts;

    public BalanceAdjustmentHandler(final AccountStore accounts) {
        this.accounts = accounts;
    }

    public void handle(final long uid, final int currency, final long delta, final CommandOutcome out) {
        out.uid(uid);
        if (!accounts.userExists(uid)) {
            out.resultCode(CommandResultCode.USER_NOT_FOUND);
            return;
        }
        final long base = accounts.balance(uid, currency);
        if (Amounts.addOverflows(base, delta)) {
            out.resultCode(CommandResultCode.OVERFLOW);
            return;
        }
        accounts.set(uid, currency, base + delta);
        out.resultCode(CommandResultCode.SUCCESS);
    }
}
