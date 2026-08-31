package io.justrade.read;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.justrade.journal.JournalDedup;
import io.justrade.journal.JournalStreams;
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
    // Bounded so a stuck endpoint resolution cannot freeze the poll thread for
    // the full default; connectNextSource retries the next member on timeout.
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 2_000L;
    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long DEFAULT_LIVENESS_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long RECONNECT_BACKOFF_MS = 250L;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final String localHost;
    private final int archiveControlStreamId;
    private final String[] controlChannels;
    private final JournalConsumer.Listener listener;
    private final long livenessTimeoutMs;
    private final JournalDedup dedup = new JournalDedup();

    private Subscription subscription;
    private JournalConsumer consumer;
    private AeronArchive archive;
    private int currentSource = -1;
    private boolean hadImage;
    private long recordingId = -1L;
    private long lastRecordingId = -1L;
    private long resumePosition;
    private long lastActivityMs;
    private long lastProbeMs;
    private long nextConnectMs;

    public HaJournalConsumer(
            final String aeronDirectoryName,
            final String localHost,
            final int archiveControlStreamId,
            final String[] controlChannels,
            final JournalConsumer.Listener listener) {
        this(
                aeronDirectoryName,
                localHost,
                archiveControlStreamId,
                controlChannels,
                listener,
                DEFAULT_LIVENESS_TIMEOUT_MS);
    }

    public HaJournalConsumer(
            final String aeronDirectoryName,
            final String localHost,
            final int archiveControlStreamId,
            final String[] controlChannels,
            final JournalConsumer.Listener listener,
            final long livenessTimeoutMs) {
        this.localHost = localHost;
        this.archiveControlStreamId = archiveControlStreamId;
        this.controlChannels = controlChannels.clone();
        this.listener = listener;
        this.livenessTimeoutMs = livenessTimeoutMs;

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
            if (System.currentTimeMillis() < nextConnectMs) {
                return 0;
            }
            connectNextSource();
            if (consumer == null) {
                // Every source is unreachable; back off rather than spin.
                nextConnectMs = System.currentTimeMillis() + RECONNECT_BACKOFF_MS;
                return 0;
            }
        }
        if (consumer == null) {
            return 0;
        }
        final long now = System.currentTimeMillis();
        probeIfDue(now);
        if (archive != null && now - lastActivityMs > livenessTimeoutMs) {
            // No fragments and no successful archive op within the window: the
            // source is silent (dead process holding the publication open, or a
            // blackholed network). Cycle to the next source.
            closeArchive();
            connectNextSource();
            if (consumer == null) {
                nextConnectMs = System.currentTimeMillis() + RECONNECT_BACKOFF_MS;
                return 0;
            }
        }
        final int fragments = consumer.poll(limit);
        if (fragments > 0) {
            lastActivityMs = now;
        }
        resumePosition = consumer.lastPosition();
        if (subscription.imageCount() > 0) {
            hadImage = true;
        }
        return fragments;
    }

    // A successful archive op counts as activity, so an idle but healthy source
    // is never failed over; a failed probe starts the timeout countdown.
    private void probeIfDue(final long now) {
        if (archive == null || recordingId < 0L) {
            return;
        }
        final long interval = Math.max(1L, livenessTimeoutMs / 2);
        if (now - lastProbeMs < interval) {
            return;
        }
        lastProbeMs = now;
        try {
            archive.getRecordingPosition(recordingId);
            lastActivityMs = now;
        } catch (final RuntimeException ignored) {
            // The timeout check in poll() fails the source over if it stays silent.
        }
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
            // Resume from the last consumed position when reconnecting to the same
            // recording (a transient image drop); a different member's recording
            // has its own position space, so it is replayed from the start (the
            // shared JournalDedup absorbs the overlap).
            final long fromPosition = recordingId == lastRecordingId ? resumePosition : 0L;
            try {
                candidate.startReplay(
                        recordingId,
                        fromPosition,
                        AeronArchive.NULL_LENGTH,
                        "aeron:udp?endpoint=" + endpoint,
                        JOURNAL_REPLAY_STREAM_ID);
            } catch (final RuntimeException e) {
                candidate.close();
                continue;
            }
            this.archive = candidate;
            this.currentSource = idx;
            this.recordingId = recordingId;
            this.lastRecordingId = recordingId;
            this.hadImage = false;
            this.lastActivityMs = System.currentTimeMillis();
            this.lastProbeMs = this.lastActivityMs;
            this.nextConnectMs = 0L;
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
        recordingId = -1L;
        hadImage = false;
    }

    private long findRecording(final AeronArchive archiveClient) {
        return ArchiveRecordings.latestRecordingId(archiveClient, JournalStreams.JOURNAL_STREAM_ID);
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
