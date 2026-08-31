package io.justrade.engine.handlers;

import io.justrade.collections.AccountStore;
import io.justrade.core.CommandOutcome;
import io.justrade.protocol.CommandResultCode;

/** Resumes a suspended user, re-enabling order placement. */
public final class ResumeUserHandler {

    private final AccountStore accounts;

    public ResumeUserHandler(final AccountStore accounts) {
        this.accounts = accounts;
    }

    public void handle(final long uid, final CommandOutcome out) {
        out.uid(uid);
        if (!accounts.userExists(uid)) {
            out.resultCode(CommandResultCode.USER_NOT_FOUND);
            return;
        }
        if (!accounts.isSuspended(uid)) {
            out.resultCode(CommandResultCode.USER_NOT_SUSPENDED);
            return;
        }
        accounts.setStatus(uid, AccountStore.STATUS_ACTIVE);
        out.resultCode(CommandResultCode.SUCCESS);
    }
}
