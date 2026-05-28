package io.github.motherofbumps.flink.connector.activemq.reader;

import io.github.motherofbumps.flink.connector.activemq.split.ActiveMQSplitState;
import jakarta.jms.Message;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import java.util.Objects;

/**
 * {@link RecordEmitter} that bridges a raw JMS {@link Message} to the
 * user-facing output type {@code T} and updates per-split state.
 *
 * <h3>Responsibilities</h3>
 * <ol>
 *   <li>Delegates deserialisation to the user-supplied
 *       {@link ActiveMQMessageDeserializer}.</li>
 *   <li>Emits the resulting record to the Flink {@link SourceOutput}.</li>
 *   <li>Stores the raw JMS {@link Message} reference in
 *       {@link ActiveMQSplitState} so it can be acknowledged after the
 *       enclosing checkpoint completes.</li>
 * </ol>
 *
 * <p>This method executes on the <em>main reader thread</em>, never the
 * fetcher thread.
 */
public final class ActiveMQRecordEmitter<T>
        implements RecordEmitter<Message, T, ActiveMQSplitState> {

    private final ActiveMQMessageDeserializer<T> deserializer;

    public ActiveMQRecordEmitter(ActiveMQMessageDeserializer<T> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    }

    @Override
    public void emitRecord(
            Message element,
            SourceOutput<T> output,
            ActiveMQSplitState splitState) throws Exception {
        T record = deserializer.deserialize(element);
        output.collect(record);
        // Retain the JMS Message reference for later acknowledgment.
        splitState.setLastConsumedMessage(element);
    }
}
