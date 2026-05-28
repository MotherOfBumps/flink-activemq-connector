package io.github.motherofbumps.flink.connector.activemq;

import io.github.motherofbumps.flink.connector.activemq.enumerator.ActiveMQEnumeratorState;
import io.github.motherofbumps.flink.connector.activemq.enumerator.ActiveMQEnumeratorStateSerializer;
import io.github.motherofbumps.flink.connector.activemq.enumerator.ActiveMQSplitEnumerator;
import io.github.motherofbumps.flink.connector.activemq.reader.ActiveMQMessageDeserializer;
import io.github.motherofbumps.flink.connector.activemq.reader.ActiveMQRecordEmitter;
import io.github.motherofbumps.flink.connector.activemq.reader.ActiveMQSourceReader;
import io.github.motherofbumps.flink.connector.activemq.reader.ActiveMQSplitReader;
import io.github.motherofbumps.flink.connector.activemq.split.ActiveMQSourceSplit;
import io.github.motherofbumps.flink.connector.activemq.split.ActiveMQSplitSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Objects;

/**
 * Apache Flink 2 {@link Source} for Apache ActiveMQ Classic 6.1 queues.
 *
 * <h2>Component topology</h2>
 * <pre>
 *  JobManager
 *  └── {@link ActiveMQSplitEnumerator}
 *        Distributes queue names (= splits) across reader subtasks.
 *
 *  TaskManager  (one per parallel subtask)
 *  └── {@link ActiveMQSourceReader}   ←  SingleThreadMultiplexSourceReaderBase
 *        │  Main thread: dequeues batches from the fetch buffer, calls
 *        │  RecordEmitter, tracks per-split checkpoint high-water marks.
 *        └── Single fetcher thread
 *              └── {@link ActiveMQSplitReader}
 *                    Multiplexes N JMS MessageConsumers (one per assigned queue)
 *                    via non-blocking receiveNoWait(); parks when idle.
 *                    Executes deferred ACKs from the pending-task queue.
 * </pre>
 *
 * <h2>At-least-once delivery</h2>
 * <p>Sessions are opened in {@code CLIENT_ACKNOWLEDGE} mode.  Messages are
 * not acknowledged until {@link ActiveMQSourceReader#notifyCheckpointComplete}
 * fires, ensuring that a job failure before that point causes the broker to
 * re-deliver all unacknowledged messages.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * ActiveMQSourceConfig config = ActiveMQSourceConfig.builder()
 *     .brokerUrl("tcp://broker:61616")
 *     .queueNames("orders", "payments")
 *     .build();
 *
 * Source<String, ?, ?> source = ActiveMQSource.create(
 *     config, ActiveMQMessageDeserializer.textMessage());
 *
 * env.enableCheckpointing(5_000);
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "activemq-source");
 * }</pre>
 *
 * @param <T> the type of records produced by this source
 */
public final class ActiveMQSource<T>
        implements Source<T, ActiveMQSourceSplit, ActiveMQEnumeratorState> {

    private final ActiveMQSourceConfig sourceConfig;
    private final ActiveMQMessageDeserializer<T> deserializer;

    private ActiveMQSource(
            ActiveMQSourceConfig sourceConfig,
            ActiveMQMessageDeserializer<T> deserializer) {
        this.sourceConfig = Objects.requireNonNull(sourceConfig, "sourceConfig");
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    }

    /**
     * Creates a new {@link ActiveMQSource}.
     *
     * @param config       broker and queue configuration
     * @param deserializer converts raw JMS {@link jakarta.jms.Message}s to records of type {@code T}
     * @param <T>          output record type
     */
    public static <T> ActiveMQSource<T> create(
            ActiveMQSourceConfig config,
            ActiveMQMessageDeserializer<T> deserializer) {
        return new ActiveMQSource<>(config, deserializer);
    }

    // -------------------------------------------------------------------------
    // Source contract
    // -------------------------------------------------------------------------

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<ActiveMQSourceSplit, ActiveMQEnumeratorState> createEnumerator(
            SplitEnumeratorContext<ActiveMQSourceSplit> enumContext) {
        return ActiveMQSplitEnumerator.create(enumContext, sourceConfig.getQueueNames());
    }

    @Override
    public SplitEnumerator<ActiveMQSourceSplit, ActiveMQEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<ActiveMQSourceSplit> enumContext,
            ActiveMQEnumeratorState checkpoint) {
        return ActiveMQSplitEnumerator.restore(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<ActiveMQSourceSplit> getSplitSerializer() {
        return new ActiveMQSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<ActiveMQEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new ActiveMQEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<T, ActiveMQSourceSplit> createReader(SourceReaderContext readerContext) {
        ActiveMQSplitReader splitReader = new ActiveMQSplitReader(sourceConfig);
        ActiveMQRecordEmitter<T> emitter = new ActiveMQRecordEmitter<>(deserializer);
        return new ActiveMQSourceReader<>(
            splitReader, emitter, readerContext.getConfiguration(), readerContext);
    }
}
