package io.github.motherofbumps.flink.connector.activemq.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link SimpleVersionedSerializer} for {@link ActiveMQEnumeratorState}.
 *
 * <h3>Wire format (version 1)</h3>
 * <pre>
 * int                      — number of subtask entries
 * foreach entry:
 *   int                    — subtask ID
 *   int                    — number of queue names
 *   foreach queue:
 *     UTF                  — queue name
 * </pre>
 */
public final class ActiveMQEnumeratorStateSerializer
        implements SimpleVersionedSerializer<ActiveMQEnumeratorState> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ActiveMQEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        Map<Integer, List<String>> assignments = state.getPendingAssignments();
        out.writeInt(assignments.size());
        for (Map.Entry<Integer, List<String>> entry : assignments.entrySet()) {
            out.writeInt(entry.getKey());
            List<String> queues = entry.getValue();
            out.writeInt(queues.size());
            for (String queue : queues) {
                out.writeUTF(queue);
            }
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public ActiveMQEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                "Unsupported ActiveMQEnumeratorState version: " + version);
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        int numEntries = in.readInt();
        Map<Integer, List<String>> assignments = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            int subtaskId = in.readInt();
            int numQueues = in.readInt();
            List<String> queues = new ArrayList<>(numQueues);
            for (int j = 0; j < numQueues; j++) {
                queues.add(in.readUTF());
            }
            assignments.put(subtaskId, queues);
        }
        return new ActiveMQEnumeratorState(assignments);
    }
}
