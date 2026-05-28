package io.github.motherofbumps.flink.connector.activemq.reader;

import io.github.motherofbumps.flink.connector.activemq.ActiveMQSourceConfig;
import io.github.motherofbumps.flink.connector.activemq.split.ActiveMQSourceSplit;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsDeletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * {@link SplitReader} for ActiveMQ queues.
 *
 * <p>A single instance multiplexes all splits (queues) assigned to one Flink
 * reader subtask — the "multiplex" in {@code SingleThreadMultiplexSourceReaderBase}.
 * Each split gets its own JMS {@link Session} and {@link MessageConsumer} so that
 * {@code CLIENT_ACKNOWLEDGE} is scoped independently per queue.
 *
 * <h3>Thread model</h3>
 * <p>All methods ({@link #fetch()}, {@link #handleSplitsChanges}, {@link #close()})
 * run on the single {@code SplitFetcher} background thread, with two exceptions:
 * <ul>
 *   <li>{@link #wakeUp()} — called from the main reader thread to unpark the
 *       fetcher thread.</li>
 *   <li>{@link #scheduleAcknowledge(Message)} — called from the main thread
 *       after a checkpoint completes; enqueues a task executed on the fetcher
 *       thread at the next {@link #fetch()} entry point.</li>
 * </ul>
 *
 * <h3>Polling strategy</h3>
 * <p>{@code fetch()} iterates all active consumers via non-blocking
 * {@code receiveNoWait()}.  When no messages are available it parks for
 * {@code pollIntervalMs} via {@link LockSupport#parkNanos}, which is immediately
 * interrupted by {@link LockSupport#unpark} from {@link #wakeUp()}.
 */
public final class ActiveMQSplitReader
        implements SplitReader<Message, ActiveMQSourceSplit>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSplitReader.class);

    private final ActiveMQSourceConfig config;

    /** Lazily initialised on the first fetch/handleSplitsChanges call. */
    private Connection connection;

    /**
     * Ordered map of splitId → consumer handle. {@link LinkedHashMap} preserves
     * insertion order for deterministic round-robin polling.
     */
    private final Map<String, ConsumerHandle> consumers = new LinkedHashMap<>();

    /**
     * Tasks submitted from the main thread (acknowledgments, etc.) that must
     * execute on the fetcher thread to satisfy JMS session thread-safety.
     */
    private final ConcurrentLinkedQueue<Runnable> pendingTasks = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean wakeupFlag = new AtomicBoolean(false);

    /** Set at the top of {@link #fetch()} so {@link #wakeUp()} can unpark it. */
    private volatile Thread fetcherThread;

    // -------------------------------------------------------------------------

    /** Holds the JMS session and consumer for one split (queue). */
    private record ConsumerHandle(Session session, MessageConsumer consumer) {}

    // -------------------------------------------------------------------------

    public ActiveMQSplitReader(ActiveMQSourceConfig config) {
        this.config = config;
    }

    // -------------------------------------------------------------------------
    // SplitReader API — all called on the fetcher thread
    // -------------------------------------------------------------------------

    @Override
    public RecordsWithSplitIds<Message> fetch() throws IOException {
        fetcherThread = Thread.currentThread();

        try {
            ensureConnection();
            processPendingTasks();

            if (consumers.isEmpty()) {
                // No splits yet; park and wait for handleSplitsChanges + wakeUp.
                parkUntilWakeup();
                return ActiveMQMessagesBatch.empty();
            }

            Map<String, List<Message>> recordsBySplit = new LinkedHashMap<>();

            for (Map.Entry<String, ConsumerHandle> entry : consumers.entrySet()) {
                String splitId        = entry.getKey();
                MessageConsumer cons  = entry.getValue().consumer();

                List<Message> messages = new ArrayList<>();
                Message msg = cons.receiveNoWait();
                while (msg != null && messages.size() < config.getMaxBatchSize()) {
                    messages.add(msg);
                    msg = cons.receiveNoWait();
                }
                if (!messages.isEmpty()) {
                    recordsBySplit.put(splitId, messages);
                }
            }

            if (recordsBySplit.isEmpty()) {
                // Nothing available across all queues; park before retrying.
                parkUntilWakeup();
            }

            return new ActiveMQMessagesBatch(recordsBySplit, Collections.emptySet());

        } catch (JMSException e) {
            throw new IOException("Error fetching messages from ActiveMQ", e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<ActiveMQSourceSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition<ActiveMQSourceSplit> addition) {
            addition.splits().forEach(this::addConsumer);
        } else if (splitsChanges instanceof SplitsDeletion<ActiveMQSourceSplit> deletion) {
            deletion.splits().forEach(s -> removeConsumer(s.splitId()));
        } else {
            LOG.warn("Unsupported SplitsChange type: {}", splitsChanges.getClass().getSimpleName());
        }
    }

    /**
     * Schedules a JMS {@link Message#acknowledge()} call to be executed on the
     * fetcher thread at the start of the next {@link #fetch()} round.
     *
     * <p>Called from the <em>main reader thread</em> during
     * {@code notifyCheckpointComplete}. In {@code CLIENT_ACKNOWLEDGE} mode,
     * acknowledging this message also acknowledges all earlier messages on the
     * same JMS session (i.e. all messages from the same queue/split that were
     * received before this one).
     *
     * @param message the checkpoint high-water-mark message to acknowledge
     */
    public void scheduleAcknowledge(Message message) {
        pendingTasks.offer(() -> {
            try {
                message.acknowledge();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Acknowledged JMS message: {}", message.getJMSMessageID());
                }
            } catch (JMSException e) {
                // Non-fatal: the broker will re-deliver on the next connect.
                LOG.error("Failed to acknowledge JMS message", e);
            }
        });
        wakeUp(); // ensure the fetcher thread processes the task promptly
    }

    @Override
    public void wakeUp() {
        wakeupFlag.set(true);
        Thread t = fetcherThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    @Override
    public void close() throws Exception {
        for (ConsumerHandle h : consumers.values()) {
            closeQuietly(h);
        }
        consumers.clear();
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                LOG.warn("Error closing ActiveMQ connection", e);
            }
            connection = null;
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers — all on the fetcher thread
    // -------------------------------------------------------------------------

    private void ensureConnection() throws JMSException {
        if (connection != null) {
            return;
        }
        ActiveMQConnectionFactory factory;
        if (config.getUsername() != null) {
            factory = new ActiveMQConnectionFactory(
                config.getUsername(), config.getPassword(), config.getBrokerUrl());
        } else {
            factory = new ActiveMQConnectionFactory(config.getBrokerUrl());
        }
        connection = factory.createConnection();
        connection.start();
        LOG.info("Connected to ActiveMQ broker at {}", config.getBrokerUrl());
    }

    private void addConsumer(ActiveMQSourceSplit split) {
        if (consumers.containsKey(split.splitId())) {
            LOG.warn("Consumer for split '{}' already exists — ignoring duplicate", split.splitId());
            return;
        }
        try {
            ensureConnection();
            // One session per split: CLIENT_ACKNOWLEDGE is scoped per session.
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue     = session.createQueue(split.getQueueName());
            MessageConsumer consumer = session.createConsumer(queue);
            consumers.put(split.splitId(), new ConsumerHandle(session, consumer));
            LOG.info("Created consumer for queue '{}'", split.getQueueName());
        } catch (JMSException e) {
            throw new RuntimeException(
                "Failed to create JMS consumer for split: " + split, e);
        }
    }

    private void removeConsumer(String splitId) {
        ConsumerHandle h = consumers.remove(splitId);
        if (h != null) {
            closeQuietly(h);
            LOG.info("Removed consumer for split '{}'", splitId);
        }
    }

    private void processPendingTasks() {
        Runnable task;
        while ((task = pendingTasks.poll()) != null) {
            task.run();
        }
    }

    private void parkUntilWakeup() {
        if (!wakeupFlag.getAndSet(false)) {
            LockSupport.parkNanos(Duration.ofMillis(config.getPollIntervalMs()).toNanos());
            wakeupFlag.set(false);
        }
    }

    private static void closeQuietly(ConsumerHandle h) {
        try { h.consumer().close(); } catch (JMSException ignored) { /* best-effort */ }
        try { h.session().close();  } catch (JMSException ignored) { /* best-effort */ }
    }
}
