package com.exadbe.engine.handlers;

import com.exadbe.collections.AccountStore;
import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandResultCode;

/** Suspends a user, blocking new order placement until resumed. */
public final class SuspendUserHandler {

    private final AccountStore accounts;

    public SuspendUserHandler(final AccountStore accounts) {
        this.accounts = accounts;
    }

    public void handle(final long uid, final CommandOutcome out) {
        out.uid(uid);
        if (!accounts.userExists(uid)) {
            out.resultCode(CommandResultCode.USER_NOT_FOUND);
            return;
        }
        if (accounts.isSuspended(uid)) {
            out.resultCode(CommandResultCode.USER_ALREADY_SUSPENDED);
            return;
        }
        accounts.setStatus(uid, AccountStore.STATUS_SUSPENDED);
        out.resultCode(CommandResultCode.SUCCESS);
    }
}
