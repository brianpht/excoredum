package com.exadbe.write.client;

import com.exadbe.protocol.CommandResultCode;

/**
 * Callback invoked when a {@code CommandResult} arrives for a previously
 * submitted command. Matched to the original request by command id.
 *
 * <p>Invoked on the client's polling thread; implementations must not block.
 */
@FunctionalInterface
public interface ResultHandler {

    /**
     * @param commandIdHi high 64 bits of the command id echoed by the core
     * @param commandIdLo low 64 bits of the command id echoed by the core
     * @param resultCode deterministic result code
     * @param uid resulting trading user id, valid only when {@code hasUid}
     * @param hasUid whether {@code uid} is present
     * @param orderId resulting order id, valid only when {@code hasOrderId}
     * @param hasOrderId whether {@code orderId} is present
     * @param filledSize filled size, valid only when {@code hasFilledSize}
     * @param hasFilledSize whether {@code filledSize} is present
     */
    void onResult(
            long commandIdHi,
            long commandIdLo,
            CommandResultCode resultCode,
            long uid,
            boolean hasUid,
            long orderId,
            boolean hasOrderId,
            long filledSize,
            boolean hasFilledSize);

    /**
     * Invoked when a command is abandoned because it exhausted
     * {@code ClientConfig.maxRetries()} without a result. Guarantees the caller
     * is notified of every submitted command (never a silent drop).
     *
     * @param commandIdHi high 64 bits of the abandoned command id
     * @param commandIdLo low 64 bits of the abandoned command id
     */
    default void onExpired(final long commandIdHi, final long commandIdLo) {}
}
