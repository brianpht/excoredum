package com.exadbe.read;

import com.exadbe.journal.JournalDedup;
import com.exadbe.journal.JournalStreams;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.concurrent.TimeUnit;

/**
 * Highly-available journal consumer: follows one member's recorded journal live
 * and, when that source dies, fails over to the next reachable member, resuming
 * from the start. A shared {@link JournalDedup} makes the merged delivery
 * exactly-once, so a leader (or source) loss never drops or duplicates an event.
 *
 * <p>Because every node records the committed journal, any surviving member holds
 * the full stream. Poll-driven from a single thread; owns its media driver and
 * Aeron client.
 */
public final class HaJournalConsumer implements AutoCloseable {

    private static final int JOURNAL_REPLAY_STREAM_ID = 45;
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 10_000L;
    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final String localHost;
    private final int archiveControlStreamId;
    private final String[] controlChannels;
    private final JournalConsumer.Listener listener;
    private final JournalDedup dedup = new JournalDedup();

    private Subscription subscription;
    private JournalConsumer consumer;
    private AeronArchive archive;
    private int currentSource = -1;
    private boolean hadImage;

    public HaJournalConsumer(
            final String aeronDirectoryName,
            final String localHost,
            final int archiveControlStreamId,
            final String[] controlChannels,
            final JournalConsumer.Listener listener) {
        this.localHost = localHost;
        this.archiveControlStreamId = archiveControlStreamId;
        this.controlChannels = controlChannels.clone();
        this.listener = listener;

        MediaDriver driver = null;
        Aeron aeronClient = null;
        try {
            driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
        } catch (final RuntimeException e) {
            closeQuietly(aeronClient, driver);
            throw e;
        }
        this.mediaDriver = driver;
        this.aeron = aeronClient;
    }

    /** Advances consumption, failing over to another member if the source is lost. */
    public int poll(final int limit) {
        if (subscription != null && hadImage && subscription.imageCount() == 0) {
            closeArchive();
        }
        if (archive == null) {
            connectNextSource();
        }
        if (consumer == null) {
            return 0;
        }
        final int fragments = consumer.poll(limit);
        if (subscription.imageCount() > 0) {
            hadImage = true;
        }
        return fragments;
    }

    /** Count of unique events delivered across all sources. */
    public long unique() {
        return consumer == null ? 0L : consumer.unique();
    }

    /** Count of re-delivered events dropped by dedup (source overlap on failover). */
    public long duplicates() {
        return consumer == null ? 0L : consumer.duplicates();
    }

    /** Index of the member currently followed, or {@code -1} if none. */
    public int currentSource() {
        return currentSource;
    }

    @Override
    public void close() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
        closeQuietly(archive, aeron, mediaDriver);
    }

    private void connectNextSource() {
        for (int attempt = 0; attempt < controlChannels.length; attempt++) {
            final int idx = (currentSource + 1 + attempt) % controlChannels.length;
            final AeronArchive candidate = tryConnect(controlChannels[idx]);
            if (candidate == null) {
                continue;
            }
            final long recordingId = findRecording(candidate);
            if (recordingId < 0L) {
                candidate.close();
                continue;
            }
            ensureSubscription();
            final String endpoint = awaitResolvedEndpoint(subscription);
            if (endpoint == null) {
                candidate.close();
                continue;
            }
            try {
                candidate.startReplay(
                        recordingId,
                        0L,
                        AeronArchive.NULL_LENGTH,
                        "aeron:udp?endpoint=" + endpoint,
                        JOURNAL_REPLAY_STREAM_ID);
            } catch (final RuntimeException e) {
                candidate.close();
                continue;
            }
            this.archive = candidate;
            this.currentSource = idx;
            this.hadImage = false;
            return;
        }
    }

    private AeronArchive tryConnect(final String controlChannel) {
        try {
            return AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel(controlChannel)
                    .controlRequestStreamId(archiveControlStreamId)
                    .controlResponseChannel("aeron:udp?endpoint=" + localHost + ":0")
                    .messageTimeoutNs(CONNECT_TIMEOUT_NS));
        } catch (final RuntimeException e) {
            return null;
        }
    }

    private void ensureSubscription() {
        if (subscription == null) {
            subscription = aeron.addSubscription("aeron:udp?endpoint=" + localHost + ":0", JOURNAL_REPLAY_STREAM_ID);
            consumer = new JournalConsumer(subscription, listener, dedup);
        }
    }

    private void closeArchive() {
        if (archive != null) {
            archive.close();
            archive = null;
        }
        hadImage = false;
    }

    private long findRecording(final AeronArchive archiveClient) {
        final long[] latest = {-1L};
        final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                        correlationId,
                        recId,
                        startTimestamp,
                        stopTimestamp,
                        startPos,
                        stopPos,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (streamId == JournalStreams.JOURNAL_STREAM_ID && recId > latest[0]) {
                        latest[0] = recId;
                    }
                };
        archiveClient.listRecordings(0L, 100, consumer);
        return latest[0];
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
