package com.exadbe.gateway.read;

import com.exadbe.gateway.http.ApiException;
import com.exadbe.read.client.BackpressureException;
import com.exadbe.read.client.ReadClient;
import com.exadbe.read.client.config.ReadClientConfig;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * Owns a {@link ReadClient} on a single dedicated thread. The thread loop
 * drains a lock-free queue of pending submits (submitting the query and
 * registering its future) and then calls {@link ReadClient#poll()}, so submit
 * and poll always happen on the one thread that owns the client (the client is
 * not thread-safe). A response is only ever delivered inside {@code poll()},
 * which runs after submits are registered, so a fast reply cannot be missed.
 */
public final class ReadPump implements AutoCloseable {

    private final ReadClient client;
    private final ReadResultBridge bridge = new ReadResultBridge();
    private final ConcurrentLinkedQueue<Submit> queue = new ConcurrentLinkedQueue<>();
    private final IdleStrategy idle = new BackoffIdleStrategy();
    private final Thread thread;
    private volatile boolean running = true;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    public ReadPump(final ReadClientConfig config) {
        this.client = new ReadClient(config);
        this.client.setListener(bridge);
        this.thread = new Thread(this::pumpLoop, "exc-gateway-read-pump");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /**
     * Submits a query. The {@code submitter} runs on the pump thread and must
     * call exactly one {@code ReadClient.submit...} returning the request id.
     *
     * @return a future completed on the pump thread when the query is answered
     */
    public CompletableFuture<Object> submit(final LongSupplier submitter) {
        final CompletableFuture<Object> result = new CompletableFuture<>();
        queue.add(new Submit(submitter, result));
        return result;
    }

    /** The most recent applied position the replica reported (diagnostics). */
    public long lastAppliedPosition() {
        return client.lastAppliedPosition();
    }

    public long submitted() {
        return client.submitted();
    }

    public long completed() {
        return client.completed();
    }

    public long expired() {
        return client.expired();
    }

    public long backpressure() {
        return client.backpressureEvents();
    }

    // Typed submits; each runs the underlying ReadClient call on the pump thread.

    public CompletableFuture<Object> submitUserExists(final long uid) {
        return submit(() -> client.submitUserExists(uid));
    }

    public CompletableFuture<Object> submitBalance(final long uid, final int currency) {
        return submit(() -> client.submitBalance(uid, currency));
    }

    public CompletableFuture<Object> submitOrderBook(final int symbolId, final int maxLevels) {
        return submit(() -> client.submitOrderBook(symbolId, maxLevels));
    }

    public CompletableFuture<Object> submitSingleUserReport(final long uid) {
        return submit(() -> client.submitSingleUserReport(uid));
    }

    public CompletableFuture<Object> submitOrderHistory(final long uid) {
        return submit(() -> client.submitOrderHistory(uid));
    }

    public CompletableFuture<Object> submitActiveOrders(final long uid) {
        return submit(() -> client.submitActiveOrders(uid));
    }

    public CompletableFuture<Object> submitOrderById(final long orderId) {
        return submit(() -> client.submitOrderById(orderId));
    }

    public CompletableFuture<Object> submitUserTrades(final long uid, final int limit) {
        return submit(() -> client.submitUserTrades(uid, limit));
    }

    public CompletableFuture<Object> submitMarketTrades(final int symbolId, final int limit) {
        return submit(() -> client.submitMarketTrades(symbolId, limit));
    }

    public CompletableFuture<Object> submitTotalCurrencyBalance() {
        return submit(() -> client.submitTotalCurrencyBalance());
    }

    public CompletableFuture<Object> submitStateHash() {
        return submit(() -> client.submitStateHash());
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
            final long requestId = submit.submitter().getAsLong();
            final CompletableFuture<Object> queryFuture = bridge.register(requestId);
            queryFuture.whenComplete((value, error) -> {
                final Throwable cause = unwrap(error);
                if (cause != null) {
                    submit.result().completeExceptionally(cause);
                } else {
                    submit.result().complete(value);
                }
            });
        } catch (final BackpressureException e) {
            submit.result().completeExceptionally(ApiException.conflict("read in-flight window full"));
        } catch (final Throwable t) {
            submit.result().completeExceptionally(ApiException.server("query submit failed: " + t.getMessage()));
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
            client.close();
        }
    }

    private record Submit(LongSupplier submitter, CompletableFuture<Object> result) {}
}
