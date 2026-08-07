package com.exadbe.engine.handlers;

import com.exadbe.collections.AccountStore;
import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandResultCode;

/**
 * Creates a spot account for a user. Idempotent creation is guaranteed upstream
 * by the dedup table; a genuine second {@code ADD_USER} for an existing uid
 * returns {@link CommandResultCode#USER_ALREADY_EXISTS}.
 */
public final class AddUserHandler {

    private final AccountStore accounts;

    public AddUserHandler(final AccountStore accounts) {
        this.accounts = accounts;
    }

    public void handle(final long uid, final CommandOutcome out) {
        out.uid(uid);
        out.resultCode(accounts.addUser(uid) ? CommandResultCode.SUCCESS : CommandResultCode.USER_ALREADY_EXISTS);
    }
}
