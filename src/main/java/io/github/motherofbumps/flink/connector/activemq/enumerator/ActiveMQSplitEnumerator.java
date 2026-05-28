package io.github.motherofbumps.flink.connector.activemq.enumerator;

import io.github.motherofbumps.flink.connector.activemq.split.ActiveMQSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * {@link SplitEnumerator} for the ActiveMQ source.
 *
 * <p>Distributes the configured queue names across available reader subtasks
 * using a round-robin policy. Each queue maps to exactly one subtask for the
 * lifetime of the job (unbounded source — splits never complete).
 *
 * <h3>Assignment lifecycle</h3>
 * <ol>
 *   <li>On fresh start, {@link #create} pre-computes the round-robin mapping
 *       and stores it in {@code pendingBySubtask}.</li>
 *   <li>When a subtask registers via {@link #addReader}, all its pre-assigned
 *       splits are dispatched immediately.</li>
 *   <li>On recovery, {@link #restore} re-populates {@code pendingBySubtask}
 *       from the checkpoint; subtasks receive their splits again on
 *       re-registration.</li>
 *   <li>If a subtask fails, {@link #addSplitsBack} re-enqueues its splits so
 *       they are reassigned on the next {@link #addReader} call.</li>
 * </ol>
 *
 * <h3>Dynamic queue discovery</h3>
 * <p>This implementation uses a static queue set (configured upfront). For
 * dynamic discovery, override {@link #start()} to schedule periodic polls of
 * the broker via {@link SplitEnumeratorContext#callAsync}.
 */
public final class ActiveMQSplitEnumerator
        implements SplitEnumerator<ActiveMQSourceSplit, ActiveMQEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSplitEnumerator.class);

    private final SplitEnumeratorContext<ActiveMQSourceSplit> context;

    /**
     * Splits awaiting dispatch, keyed by the subtask that should receive them.
     * Entries are removed once the subtask calls {@code addReader}.
     */
    private final Map<Integer, Queue<ActiveMQSourceSplit>> pendingBySubtask;

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /**
     * Creates an enumerator for a fresh job. Queue names are distributed
     * round-robin across the current parallelism.
     */
    public static ActiveMQSplitEnumerator create(
            SplitEnumeratorContext<ActiveMQSourceSplit> context,
            List<String> queueNames) {
        int parallelism = context.currentParallelism();
        Map<Integer, Queue<ActiveMQSourceSplit>> pending = new HashMap<>();
        for (int i = 0; i < queueNames.size(); i++) {
            int subtask = i % parallelism;
            pending.computeIfAbsent(subtask, k -> new ArrayDeque<>())
                   .add(new ActiveMQSourceSplit(queueNames.get(i)));
        }
        return new ActiveMQSplitEnumerator(context, pending);
    }

    /**
     * Restores an enumerator from a checkpoint. The pending-assignment map is
     * reconstructed as-is; subtasks receive their splits again on re-registration.
     */
    public static ActiveMQSplitEnumerator restore(
            SplitEnumeratorContext<ActiveMQSourceSplit> context,
            ActiveMQEnumeratorState state) {
        Map<Integer, Queue<ActiveMQSourceSplit>> pending = new HashMap<>();
        state.getPendingAssignments().forEach((subtask, queues) -> {
            Queue<ActiveMQSourceSplit> q = new ArrayDeque<>();
            queues.forEach(name -> q.add(new ActiveMQSourceSplit(name)));
            pending.put(subtask, q);
        });
        return new ActiveMQSplitEnumerator(context, pending);
    }

    private ActiveMQSplitEnumerator(
            SplitEnumeratorContext<ActiveMQSourceSplit> context,
            Map<Integer, Queue<ActiveMQSourceSplit>> pendingBySubtask) {
        this.context         = context;
        this.pendingBySubtask = pendingBySubtask;
    }

    // -------------------------------------------------------------------------
    // SplitEnumerator API
    // -------------------------------------------------------------------------

    @Override
    public void start() {
        // All distribution is done in the factory methods; nothing to do here.
        // For dynamic queue discovery, schedule a callAsync poll here.
    }

    /**
     * Assigns all pre-computed splits for {@code subtaskId} when it registers.
     * Subtasks with no assigned queue (parallelism > queue count) are signalled
     * immediately so they do not wait indefinitely.
     */
    @Override
    public void addReader(int subtaskId) {
        Queue<ActiveMQSourceSplit> splits = pendingBySubtask.remove(subtaskId);
        if (splits != null && !splits.isEmpty()) {
            splits.forEach(split -> {
                context.assignSplit(split, subtaskId);
                LOG.info("Assigned queue '{}' to subtask {}", split.getQueueName(), subtaskId);
            });
        } else {
            context.signalNoMoreSplits(subtaskId);
            LOG.info("No queues for subtask {} — signalled noMoreSplits", subtaskId);
        }
    }

    /**
     * Handles a split request from a subtask that has no assigned splits.
     * For unbounded queue sources this should not occur in normal operation,
     * but we signal no-more-splits defensively.
     */
    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        LOG.debug("Subtask {} requested extra split — signalling noMoreSplits", subtaskId);
        context.signalNoMoreSplits(subtaskId);
    }

    /**
     * Re-queues splits from a failed subtask so they are re-assigned when the
     * subtask recovers and calls {@link #addReader}.
     */
    @Override
    public void addSplitsBack(List<ActiveMQSourceSplit> splits, int subtaskId) {
        Queue<ActiveMQSourceSplit> queue =
            pendingBySubtask.computeIfAbsent(subtaskId, k -> new ArrayDeque<>());
        queue.addAll(splits);
        LOG.info("Re-queued {} split(s) from failed subtask {}", splits.size(), subtaskId);
    }

    @Override
    public ActiveMQEnumeratorState snapshotState(long checkpointId) {
        Map<Integer, List<String>> snapshot = new HashMap<>();
        pendingBySubtask.forEach((subtask, splits) -> {
            List<String> names = new ArrayList<>(splits.size());
            splits.forEach(s -> names.add(s.getQueueName()));
            snapshot.put(subtask, names);
        });
        return new ActiveMQEnumeratorState(snapshot);
    }

    @Override
    public void close() throws IOException {
        // Nothing to release.
    }
}
