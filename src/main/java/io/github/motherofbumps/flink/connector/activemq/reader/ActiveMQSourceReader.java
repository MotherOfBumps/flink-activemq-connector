package io.github.motherofbumps.flink.connector.activemq.reader;

import io.github.motherofbumps.flink.connector.activemq.split.ActiveMQSourceSplit;
import io.github.motherofbumps.flink.connector.activemq.split.ActiveMQSplitState;
import jakarta.jms.Message;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink source reader for ActiveMQ queues.
 *
 * <p>Extends {@link SingleThreadMultiplexSourceReaderBase} to add
 * Flink-checkpoint-aware JMS message acknowledgment, giving
 * <strong>at-least-once</strong> delivery guarantees.
 *
 * <h3>Checkpoint integration</h3>
 * <ul>
 *   <li>{@link #snapshotState} delegates to the base class (which serialises
 *       split IDs) and then records the current message high-water mark per
 *       split in {@link ActiveMQSplitState}.</li>
 *   <li>{@link #notifyCheckpointComplete} retrieves each split's high-water-mark
 *       message and schedules a JMS {@code acknowledge()} on the fetcher thread
 *       via {@link ActiveMQSplitReader#scheduleAcknowledge}.</li>
 *   <li>{@link #notifyCheckpointAborted} discards the pending entry so those
 *       messages roll forward into the next checkpoint.</li>
 * </ul>
 *
 * <h3>Why a separate splitStates mirror?</h3>
 * <p>The base class holds split states internally in a private field. To access
 * them during the checkpoint lifecycle without reflection, this class maintains
 * a parallel {@link #ownedSplitStates} map that is kept in sync via
 * {@link #initializedState}.
 *
 * <h3>Supplier idiom</h3>
 * <p>The {@link ActiveMQSplitReader} is constructed before this reader and passed
 * in directly. The {@code () -> splitReader} supplier passed to the base class
 * is called exactly once by the {@code SingleThreadFetcherManager} when it
 * spawns the fetcher thread, so retaining the reference here is safe.
 */
public final class ActiveMQSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Message, T, ActiveMQSourceSplit, ActiveMQSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSourceReader.class);

    private final ActiveMQSplitReader splitReader;

    /**
     * Parallel view of the internal split states, keyed by split ID.
     * Maintained solely to give the checkpoint lifecycle methods access to
     * per-split state without reflection.
     */
    private final Map<String, ActiveMQSplitState> ownedSplitStates = new LinkedHashMap<>();

    public ActiveMQSourceReader(
            ActiveMQSplitReader splitReader,
            ActiveMQRecordEmitter<T> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
            () -> splitReader,   // SingleThreadFetcherManager calls this exactly once
            recordEmitter,
            config,
            context);
        this.splitReader = splitReader;
    }

    // -------------------------------------------------------------------------
    // SingleThreadMultiplexSourceReaderBase abstract methods
    // -------------------------------------------------------------------------

    /**
     * Called when a new split is assigned to this reader. Creates the mutable
     * split state and registers it in the local mirror map.
     */
    @Override
    protected ActiveMQSplitState initializedState(ActiveMQSourceSplit split) {
        ActiveMQSplitState state = new ActiveMQSplitState(split);
        ownedSplitStates.put(split.splitId(), state);
        return state;
    }

    /**
     * Converts mutable split state back to a serialisable {@link ActiveMQSourceSplit}
     * during {@code snapshotState}. The queue name embedded in the split is
     * sufficient to resume consumption — ActiveMQ re-delivers unacknowledged
     * messages when a new consumer connects.
     */
    @Override
    protected ActiveMQSourceSplit toSplitType(String splitId, ActiveMQSplitState splitState) {
        return splitState.getSplit();
    }

    // -------------------------------------------------------------------------
    // Checkpoint lifecycle
    // -------------------------------------------------------------------------

    /**
     * Called on the main reader thread when a checkpoint barrier is snapshotted.
     *
     * <p>Delegates to the base class (serialises split IDs into the Flink
     * checkpoint store) then records the current high-water-mark message in
     * each split state <em>before</em> any new records can be emitted for this
     * checkpoint ID.
     */
    @Override
    public List<ActiveMQSourceSplit> snapshotState(long checkpointId) {
        List<ActiveMQSourceSplit> splits = super.snapshotState(checkpointId);
        ownedSplitStates.values().forEach(s -> s.onCheckpointStarted(checkpointId));
        LOG.debug("Snapshotted {} split(s) for checkpoint {}", splits.size(), checkpointId);
        return splits;
    }

    /**
     * Called on the main reader thread once the checkpoint has been committed
     * by <em>all</em> operators in the pipeline.
     *
     * <p>For each split, retrieves the message high-water mark for this
     * checkpoint and enqueues an acknowledgment task on the fetcher thread.
     * The fetcher executes it at the next {@link ActiveMQSplitReader#fetch()}
     * entry, honoring the JMS session thread-safety contract.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        ownedSplitStates.forEach((splitId, state) ->
            state.onCheckpointComplete(checkpointId).ifPresent(msg -> {
                LOG.debug("Scheduling ACK for split '{}' at checkpoint {}", splitId, checkpointId);
                splitReader.scheduleAcknowledge(msg);
            })
        );
    }

    /**
     * Called on the main reader thread when a checkpoint is aborted (e.g.
     * alignment timeout, operator failure).
     *
     * <p>Discards the pending high-water-mark entry for this checkpoint
     * without acknowledging. The messages will be re-included in the next
     * successful checkpoint.
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        ownedSplitStates.values().forEach(s -> s.onCheckpointAborted(checkpointId));
        LOG.debug("Checkpoint {} aborted — pending ACK entries discarded", checkpointId);
    }
}
