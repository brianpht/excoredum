package com.exadbe.gateway.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies the broadcaster fans out to every sink and tolerates a failing sink. */
class StreamBroadcasterTest {

    @Test
    void fansOutToEverySinkAndRemovesOnUnregister() {
        final StreamBroadcaster broadcaster = new StreamBroadcaster();
        final List<String> first = new ArrayList<>();
        final List<String> second = new ArrayList<>();
        final StreamSink sinkFirst = first::add;
        final StreamSink sinkSecond = second::add;
        broadcaster.add(sinkFirst);
        broadcaster.add(sinkSecond);

        broadcaster.publish("one");
        broadcaster.publish("two");
        assertEquals(List.of("one", "two"), first);
        assertEquals(List.of("one", "two"), second);

        broadcaster.remove(sinkFirst);
        broadcaster.publish("three");
        assertEquals(List.of("one", "two"), first);
        assertEquals(List.of("one", "two", "three"), second);
    }

    @Test
    void aFailingSinkDoesNotBreakOthers() {
        final StreamBroadcaster broadcaster = new StreamBroadcaster();
        final List<String> received = new ArrayList<>();
        broadcaster.add(json -> {
            throw new RuntimeException("boom");
        });
        broadcaster.add(received::add);

        broadcaster.publish("x");
        assertEquals(List.of("x"), received);
    }
}
