package com.exadbe.gateway.stream;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe fan-out of JSON events to a set of {@link StreamSink}s. Producers
 * (egress listeners, market snapshot pump) call {@link #publish(String)}; the
 * WebSocket layer registers and unregisters sinks. A failing sink never blocks
 * or breaks the others.
 */
public final class StreamBroadcaster {

    private final Set<StreamSink> sinks = ConcurrentHashMap.newKeySet();

    public void add(final StreamSink sink) {
        sinks.add(sink);
    }

    public void remove(final StreamSink sink) {
        sinks.remove(sink);
    }

    public void publish(final String json) {
        for (final StreamSink sink : sinks) {
            try {
                sink.send(json);
            } catch (final RuntimeException ignored) {
                // A closed or stalled subscriber must not break the broadcast.
            }
        }
    }

    public boolean isEmpty() {
        return sinks.isEmpty();
    }
}
