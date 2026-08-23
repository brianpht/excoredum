package com.exadbe.gateway.stream;

/**
 * A single WebSocket subscriber feed. {@link StreamBroadcaster} fans out a JSON
 * event string to every registered sink; a sink is invoked from a writer thread
 * and must be safe for concurrent invocation (Netty {@code Channel.writeAndFlush}
 * is thread-safe).
 */
@FunctionalInterface
public interface StreamSink {

    void send(String json);
}
