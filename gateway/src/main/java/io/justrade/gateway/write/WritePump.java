package io.justrade.gateway.write;

import io.justrade.gateway.dto.WriteResultDto;
import io.justrade.gateway.http.ApiException;
import io.justrade.gateway.stream.EgressStream;
import io.justrade.gateway.stream.StreamBroadcaster;
import io.justrade.write.client.BackpressureException;
import io.justrade.write.client.ExcClient;
import io.justrade.write.client.config.ClientConfig;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * Owns an {@link ExcClient} on a single dedicated thread. The thread loop
 * drains a lock-free queue of pending submits (submitting the command and
 * registering its future) and then calls {@link ExcClient#poll()}, so submit
 * and poll always happen on the one thread that owns the client (the client is
 * not thread-safe). A result is only ever delivered inside {@code poll()},
 * which runs after submits are registered, so a fast reply cannot be missed.
 */
public final class WritePump implements AutoCloseable {

    private final ExcClient client;
    private final WriteResultBridge bridge = new WriteResultBridge();
    private final EgressStream egress;
    private final ConcurrentLinkedQueue<Submit> queue = new ConcurrentLinkedQueue<>();
    private final IdleStrategy idle = new BackoffIdleStrategy();
    private final Thread thread;
    private volatile boolean running = true;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    public WritePump(final ClientConfig config) {
        this(config, new StreamBroadcaster());
    }

    public WritePump(final ClientConfig config, final StreamBroadcaster broadcaster) {
        this.client = new ExcClient(config, bridge);
        this.egress = new EgressStream(broadcaster);
        this.client.tradeListener(egress);
        this.client.reduceListener(egress);
        this.client.rejectListener(egress);
        this.client.orderBookListener(egress);
        this.thread = new Thread(this::pumpLoop, "gateway-write-pump");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /**
     * Submits a command. The {@code submitter} runs on the pump thread and must
     * call exactly one {@code ExcClient} typed submit returning a command id.
     *
     * @return a future completed on the pump thread when the command result arrives
     */
    public CompletableFuture<WriteResultDto> submit(final LongSupplier submitter) {
        final CompletableFuture<WriteResultDto> result = new CompletableFuture<>();
        queue.add(new Submit(submitter, result));
        return result;
    }

    // Typed submits; each runs the underlying ExcClient call on the pump thread.

    public CompletableFuture<WriteResultDto> addSymbol(
            final int symbolId,
            final int baseCurrency,
            final int quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK,
            final long takerFee,
            final long makerFee) {
        return submit(() ->
                client.addSymbol(symbolId, baseCurrency, quoteCurrency, baseScaleK, quoteScaleK, takerFee, makerFee));
    }

    public CompletableFuture<WriteResultDto> addUser(final long uid) {
        return submit(() -> client.addUser(uid));
    }

    public CompletableFuture<WriteResultDto> adjustBalance(final long uid, final int currency, final long amount) {
        return submit(() -> client.adjustBalance(uid, currency, amount));
    }

    public CompletableFuture<WriteResultDto> suspendUser(final long uid) {
        return submit(() -> client.suspendUser(uid));
    }

    public CompletableFuture<WriteResultDto> resumeUser(final long uid) {
        return submit(() -> client.resumeUser(uid));
    }

    public CompletableFuture<WriteResultDto> placeGtc(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long reserveBidPrice,
            final long uid,
            final int userCookie) {
        return submit(() -> client.placeGtc(symbolId, orderId, ask, price, size, reserveBidPrice, uid, userCookie));
    }

    public CompletableFuture<WriteResultDto> placeIoc(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long uid,
            final int userCookie) {
        return submit(() -> client.placeIoc(symbolId, orderId, ask, price, size, uid, userCookie));
    }

    public CompletableFuture<WriteResultDto> placeFokBudget(
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long budget,
            final long size,
            final long uid,
            final int userCookie) {
        return submit(() -> client.placeFokBudget(symbolId, orderId, ask, budget, size, uid, userCookie));
    }

    public CompletableFuture<WriteResultDto> cancelOrder(final int symbolId, final long orderId, final long uid) {
        return submit(() -> client.cancelOrder(symbolId, orderId, uid));
    }

    public CompletableFuture<WriteResultDto> moveOrder(
            final int symbolId, final long orderId, final long newPrice, final long uid) {
        return submit(() -> client.moveOrder(symbolId, orderId, newPrice, uid));
    }

    public CompletableFuture<WriteResultDto> reduceOrder(
            final int symbolId, final long orderId, final long size, final long uid) {
        return submit(() -> client.reduceOrder(symbolId, orderId, size, uid));
    }

    public CompletableFuture<WriteResultDto> requestOrderBook(final int symbolId, final long uid) {
        return submit(() -> client.requestOrderBook(symbolId, uid));
    }

    private void pumpLoop() {
        while (running) {
            int work = 0;
            Submit submit;
            while ((submit = queue.poll()) != null) {
                execSubmit(submit);
                work++;
            }
            work += client.poll();
            idle.idle(work);
        }
    }

    private void execSubmit(final Submit submit) {
        try {
            final long commandIdLo = submit.submitter().getAsLong();
            final CompletableFuture<WriteResultDto> commandFuture = bridge.register(commandIdLo);
            commandFuture.whenComplete((value, error) -> {
                final Throwable cause = unwrap(error);
                if (cause != null) {
                    submit.result().completeExceptionally(cause);
                } else {
                    submit.result().complete(value);
                }
            });
        } catch (final BackpressureException e) {
            submit.result().completeExceptionally(new ApiException(429, "write in-flight window full"));
        } catch (final Throwable t) {
            submit.result().completeExceptionally(ApiException.server("command submit failed: " + t.getMessage()));
        }
    }

    private static Throwable unwrap(final Throwable error) {
        if (error instanceof ExecutionException && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            running = false;
            thread.interrupt();
            joinQuietly(thread);
            client.close();
        }
    }

    private static void joinQuietly(final Thread thread) {
        try {
            thread.join(5_000L);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record Submit(LongSupplier submitter, CompletableFuture<WriteResultDto> result) {}
}
