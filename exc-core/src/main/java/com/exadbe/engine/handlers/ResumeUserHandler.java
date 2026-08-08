package com.exadbe.engine.handlers;

import com.exadbe.collections.AccountStore;
import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandResultCode;

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
