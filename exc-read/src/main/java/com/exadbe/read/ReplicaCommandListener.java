package com.exadbe.read;

import com.exadbe.core.CommandOutcome;
import com.exadbe.protocol.CommandEnvelopeDecoder;

/**
 * Optional callback fired by the read replica for every command applied from
 * the followed consensus log. Invoked on the replica's polling thread (the
 * gateway agent) immediately after the command was processed by the engine,
 * with the leader-assigned timestamp carried by the consensus session header.
 *
 * <p>The {@code envelope} and {@code outcome} are reused across commands and
 * are only valid for the duration of the call; a listener that needs them
 * beyond the callback must copy the values out synchronously.
 */
@FunctionalInterface
public interface ReplicaCommandListener {

    ReplicaCommandListener NONE = (timestamp, envelope, outcome) -> {};

    void onCommand(long timestamp, CommandEnvelopeDecoder envelope, CommandOutcome outcome);
}
