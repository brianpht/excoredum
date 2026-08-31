package io.justrade.journal;

/** Shared channel and stream id for the durable domain-event journal. */
public final class JournalStreams {

    /** IPC channel the journal is published and recorded on. */
    public static final String JOURNAL_CHANNEL = "aeron:ipc";

    /** Stream id of the recorded domain-event journal. */
    public static final int JOURNAL_STREAM_ID = 200;

    private JournalStreams() {}
}
