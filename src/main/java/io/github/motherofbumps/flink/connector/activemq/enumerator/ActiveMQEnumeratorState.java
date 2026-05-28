package io.github.motherofbumps.flink.connector.activemq.enumerator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Checkpoint state for {@link ActiveMQSplitEnumerator}.
 *
 * <p>Records which queue names are still pending assignment per subtask at
 * the time of the checkpoint. In steady-state (after all subtasks have
 * registered and received their splits), this map is empty. It is non-empty
 * only between a subtask failure and the point where that subtask
 * re-registers via {@code addReader}.
 */
public final class ActiveMQEnumeratorState {

    /** subtaskId → ordered list of queue names awaiting assignment. */
    private final Map<Integer, List<String>> pendingAssignments;

    public ActiveMQEnumeratorState(Map<Integer, List<String>> pendingAssignments) {
        this.pendingAssignments = Collections.unmodifiableMap(pendingAssignments);
    }

    public Map<Integer, List<String>> getPendingAssignments() {
        return pendingAssignments;
    }
}
