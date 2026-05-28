package io.github.motherofbumps.flink.connector.activemq.split;

import jakarta.jms.Message;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Mutable per-split state held on the Flink main reader thread.
 *
 * <p>Tracks the last JMS {@link Message} seen for each split so that exactly
 * the right set of messages can be acknowledged when a checkpoint completes,
 * giving <strong>at-least-once</strong> delivery semantics.
 *
 * <h3>Lifecycle per checkpoint</h3>
 * <ol>
 *   <li>{@link #setLastConsumedMessage} — called by {@link
 *       io.github.motherofbumps.flink.connector.activemq.reader.ActiveMQRecordEmitter}
 *       on every emitted record.</li>
 *   <li>{@link #onCheckpointStarted(long)} — called during
 *       {@code snapshotState(checkpointId)} to capture the current high-water
 *       mark for this checkpoint.</li>
 *   <li>{@link #onCheckpointComplete(long)} — called from
 *       {@code notifyCheckpointComplete}; returns the message that should be
 *       acknowledged on the fetcher thread.</li>
 *   <li>{@link #onCheckpointAborted(long)} — called from
 *       {@code notifyCheckpointAborted}; discards the pending entry so the
 *       messages are included in the next successful checkpoint.</li>
 * </ol>
 *
 * <h3>Thread safety</h3>
 * <p>All methods are called exclusively from the main reader thread and are
 * therefore not thread-safe by design (no synchronisation needed).
 */
public final class ActiveMQSplitState {

    private final ActiveMQSourceSplit split;

    /**
     * The most recently emitted JMS message for this split.
     * Updated on every {@link #setLastConsumedMessage} call.
     */
    private Message lastConsumedMessage;

    /**
     * Maps {@code checkpointId -> last JMS Message included in that checkpoint}.
     * Entries are removed once the checkpoint completes or is aborted.
     */
    private final TreeMap<Long, Message> pendingCheckpointAcks = new TreeMap<>();

    public ActiveMQSplitState(ActiveMQSourceSplit split) {
        this.split = split;
    }

    // -------------------------------------------------------------------------
    // Called by RecordEmitter (main thread)
    // -------------------------------------------------------------------------

    /**
     * Records the most recently emitted JMS message. The reference is held
     * until the next call so it can be included in the current checkpoint.
     */
    public void setLastConsumedMessage(Message message) {
        this.lastConsumedMessage = message;
    }

    // -------------------------------------------------------------------------
    // Called by ActiveMQSourceReader during checkpoint lifecycle (main thread)
    // -------------------------------------------------------------------------

    /**
     * Captures the current {@link #lastConsumedMessage} as the high-water mark
     * for {@code checkpointId}.
     *
     * <p>Idempotent when no new messages have arrived since the previous snapshot.
     */
    public void onCheckpointStarted(long checkpointId) {
        if (lastConsumedMessage != null) {
            pendingCheckpointAcks.put(checkpointId, lastConsumedMessage);
        }
    }

    /**
     * Returns the JMS message to acknowledge when {@code checkpointId} completes.
     *
     * <p>In {@code CLIENT_ACKNOWLEDGE} mode, acknowledging this message also
     * acknowledges all prior messages on the same JMS session, so only the
     * last message of the checkpoint range needs to be acknowledged.
     *
     * <p>All entries for checkpoints {@code <= checkpointId} are removed.
     *
     * @return the highest-sequence message in the completed range, or
     *         {@link Optional#empty()} if no messages were consumed
     */
    public Optional<Message> onCheckpointComplete(long checkpointId) {
        NavigableMap<Long, Message> completed =
            pendingCheckpointAcks.headMap(checkpointId, /* inclusive= */ true);
        if (completed.isEmpty()) {
            return Optional.empty();
        }
        Message toAck = completed.lastEntry().getValue();
        completed.clear(); // removes the view's entries from the backing TreeMap
        return Optional.of(toAck);
    }

    /**
     * Discards the pending ack entry for {@code checkpointId} without
     * acknowledging. Those messages will be covered by the next
     * successful checkpoint.
     */
    public void onCheckpointAborted(long checkpointId) {
        pendingCheckpointAcks.remove(checkpointId);
    }

    public ActiveMQSourceSplit getSplit() {
        return split;
    }
}
