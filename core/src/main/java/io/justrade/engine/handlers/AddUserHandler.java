package io.justrade.engine.handlers;

import io.justrade.collections.AccountStore;
import io.justrade.core.CommandOutcome;
import io.justrade.engine.risk.DirectExchangeRisk;
import io.justrade.protocol.CommandResultCode;

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
        // uid 0 is reserved for the fee account and cannot be claimed by a user.
        if (uid == DirectExchangeRisk.FEE_ACCOUNT_UID) {
            out.resultCode(CommandResultCode.USER_ALREADY_EXISTS);
            return;
        }
        out.resultCode(accounts.addUser(uid) ? CommandResultCode.SUCCESS : CommandResultCode.USER_ALREADY_EXISTS);
    }
}
