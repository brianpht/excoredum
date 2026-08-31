package io.justrade.gateway.write;

import io.justrade.gateway.dto.WriteResultDto;
import io.justrade.gateway.http.ApiException;
import io.justrade.protocol.CommandResultCode;
import io.justrade.write.client.ResultHandler;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Bridges {@link WriteClient} result delivery to the HTTP handlers: the single
 * {@link ResultHandler} completes a per-command {@link CompletableFuture} by
 * command id low word. Runs on the write pump thread (the same thread that
 * calls {@code WriteClient.poll()}), so it must not block.
 */
public final class WriteResultBridge implements ResultHandler {

    // Single-threaded: register() and onResult() both run on the write pump thread.
    private final HashMap<Long, CompletableFuture<WriteResultDto>> pending = new HashMap<>();

    /** Registered by the handler *after* a submit returns the command id (safe: submit and poll share one thread). */
    public CompletableFuture<WriteResultDto> register(final long commandIdLo) {
        final CompletableFuture<WriteResultDto> future = new CompletableFuture<>();
        pending.put(commandIdLo, future);
        return future;
    }

    @Override
    public void onResult(
            final long commandIdHi,
            final long commandIdLo,
            final CommandResultCode resultCode,
            final long uid,
            final boolean hasUid,
            final long orderId,
            final boolean hasOrderId,
            final long filledSize,
            final boolean hasFilledSize) {
        final CompletableFuture<WriteResultDto> future = pending.remove(commandIdLo);
        if (future == null) {
            return;
        }
        future.complete(new WriteResultDto(
                commandIdHi,
                commandIdLo,
                resultCode.name(),
                hasUid ? uid : null,
                hasOrderId ? orderId : null,
                hasFilledSize ? filledSize : null));
    }

    @Override
    public void onExpired(final long commandIdHi, final long commandIdLo) {
        final CompletableFuture<WriteResultDto> future = pending.remove(commandIdLo);
        if (future != null) {
            future.completeExceptionally(ApiException.timeout("command expired before a result arrived"));
        }
    }
}
