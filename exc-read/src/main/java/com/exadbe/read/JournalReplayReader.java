package com.exadbe.read;

import com.exadbe.journal.JournalStreams;
import com.exadbe.read.config.ReadReplicaConfig;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * Replays the recorded domain-event journal from a cluster member's Archive and
 * feeds it through a {@link JournalConsumer}, so audit/analytics consumers can
 * read committed events durably and idempotently (the consumer dedups any events
 * re-delivered across a failover or a repeated replay).
 *
 * <p>Owns its own media driver, Aeron client, and Archive connection. Poll-driven
 * from a single thread.
 */
public final class JournalReplayReader implements AutoCloseable {

    private static final int JOURNAL_REPLAY_STREAM_ID = 44;
    // Bounded so a stuck endpoint resolution cannot freeze the poll thread.
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 2_000L;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final String localHost;
    private final JournalConsumer.Listener listener;

    private Subscription subscription;
    private JournalConsumer consumer;
    private long recordingId = -1L;

    public JournalReplayReader(final ReadReplicaConfig config, final JournalConsumer.Listener listener) {
        this.localHost = config.localHost();
        this.listener = listener;

        MediaDriver driver = null;
        Aeron aeronClient = null;
        AeronArchive archiveClient = null;
        try {
            driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(config.aeronDirectoryName())
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectoryName()));
            archiveClient = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeronClient)
                    .controlRequestChannel(config.archiveControlChannel())
                    .controlRequestStreamId(config.archiveControlStreamId())
                    .controlResponseChannel("aeron:udp?endpoint=" + config.localHost() + ":0"));
        } catch (final RuntimeException e) {
            closeQuietly(archiveClient, aeronClient, driver);
            throw e;
        }
        this.mediaDriver = driver;
        this.aeron = aeronClient;
        this.archive = archiveClient;
    }

    /** Current recorded position of the journal stream, or {@code -1} if absent. */
    public long recordingPosition() {
        final long id = ensureRecordingId();
        return id < 0L ? -1L : archive.getRecordingPosition(id);
    }

    /** Starts a bounded replay of {@code [fromPosition, fromPosition + length)}. */
    public boolean startReplay(final long fromPosition, final long length) {
        final long id = ensureRecordingId();
        if (id < 0L) {
            return false;
        }
        if (subscription == null) {
            subscription = aeron.addSubscription("aeron:udp?endpoint=" + localHost + ":0", JOURNAL_REPLAY_STREAM_ID);
            consumer = new JournalConsumer(subscription, listener);
        }
        final String endpoint = awaitResolvedEndpoint(subscription);
        if (endpoint == null) {
            return false;
        }
        archive.startReplay(id, fromPosition, length, "aeron:udp?endpoint=" + endpoint, JOURNAL_REPLAY_STREAM_ID);
        return true;
    }

    /** Starts a live-following replay from {@code fromPosition} to the growing end. */
    public boolean startReplay(final long fromPosition) {
        return startReplay(fromPosition, AeronArchive.NULL_LENGTH);
    }

    /** Polls the replay, delivering up to {@code limit} deduped events. */
    public int poll(final int limit) {
        return consumer == null ? 0 : consumer.poll(limit);
    }

    /** Count of unique events delivered so far. */
    public long unique() {
        return consumer == null ? 0L : consumer.unique();
    }

    /** Count of re-delivered events dropped by dedup. */
    public long duplicates() {
        return consumer == null ? 0L : consumer.duplicates();
    }

    @Override
    public void close() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
        closeQuietly(archive, aeron, mediaDriver);
    }

    private long ensureRecordingId() {
        if (recordingId < 0L) {
            recordingId = findRecording();
        }
        return recordingId;
    }

    private long findRecording() {
        return ArchiveRecordings.latestRecordingId(archive, JournalStreams.JOURNAL_STREAM_ID);
    }

    private static String awaitResolvedEndpoint(final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + RESOLVE_ENDPOINT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final String endpoint = subscription.resolvedEndpoint();
            if (endpoint != null) {
                return endpoint;
            }
            Thread.onSpinWait();
        }
        return null;
    }

    private static void closeQuietly(final AutoCloseable... resources) {
        for (final AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (final Exception ignored) {
                    // Best-effort teardown.
                }
            }
        }
    }
}
