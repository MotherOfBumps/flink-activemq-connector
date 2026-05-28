package io.github.motherofbumps.flink.connector.activemq;

import io.github.motherofbumps.flink.connector.activemq.reader.ActiveMQMessageDeserializer;
import io.github.motherofbumps.flink.connector.activemq.reader.ActiveMQSplitReader;
import io.github.motherofbumps.flink.connector.activemq.split.ActiveMQSourceSplit;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ActiveMQSplitReader} using an embedded
 * in-process ActiveMQ broker (vm:// transport — no external broker required).
 */
class ActiveMQSplitReaderTest {

    private static final String BROKER_NAME = "test-broker";
    private static final String BROKER_URL  = "vm://" + BROKER_NAME;
    private static final String QUEUE_NAME  = "test.queue";

    private BrokerService broker;

    @BeforeEach
    void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName(BROKER_NAME);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
    }

    @AfterEach
    void stopBroker() throws Exception {
        if (broker != null && broker.isStarted()) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void fetchesTextMessagesFromSingleQueue() throws Exception {
        sendTextMessages(QUEUE_NAME, "hello", "world", "flink");

        ActiveMQSourceConfig config = ActiveMQSourceConfig.builder()
            .brokerUrl(BROKER_URL)
            .queueNames(QUEUE_NAME)
            .pollIntervalMs(50)
            .build();

        try (ActiveMQSplitReader reader = new ActiveMQSplitReader(config)) {
            reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(new ActiveMQSourceSplit(QUEUE_NAME))));

            List<String> received = drainText(reader, 3);

            assertThat(received).containsExactlyInAnyOrder("hello", "world", "flink");
        }
    }

    @Test
    void fetchesFromMultipleQueues() throws Exception {
        sendTextMessages("queue.a", "msg-a1", "msg-a2");
        sendTextMessages("queue.b", "msg-b1");

        ActiveMQSourceConfig config = ActiveMQSourceConfig.builder()
            .brokerUrl(BROKER_URL)
            .queueNames("queue.a", "queue.b")
            .pollIntervalMs(50)
            .build();

        try (ActiveMQSplitReader reader = new ActiveMQSplitReader(config)) {
            reader.handleSplitsChanges(new SplitsAddition<>(List.of(
                new ActiveMQSourceSplit("queue.a"),
                new ActiveMQSourceSplit("queue.b"))));

            List<String> received = drainText(reader, 3);

            assertThat(received)
                .containsExactlyInAnyOrder("msg-a1", "msg-a2", "msg-b1");
        }
    }

    @Test
    void scheduleAcknowledgeIsProcessedOnNextFetch() throws Exception {
        sendTextMessages(QUEUE_NAME, "ack-me");

        ActiveMQSourceConfig config = ActiveMQSourceConfig.builder()
            .brokerUrl(BROKER_URL)
            .queueNames(QUEUE_NAME)
            .pollIntervalMs(50)
            .build();

        try (ActiveMQSplitReader reader = new ActiveMQSplitReader(config)) {
            reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(new ActiveMQSourceSplit(QUEUE_NAME))));

            // Drain the raw JMS message
            List<Message> raw = drainRaw(reader, 1);
            assertThat(raw).hasSize(1);

            // Simulate notifyCheckpointComplete scheduling an ACK
            reader.scheduleAcknowledge(raw.get(0));

            // The next fetch should process the pending ACK without throwing
            RecordsWithSplitIds<Message> batch = reader.fetch();
            assertThat(batch).isNotNull();
        }
    }

    @Test
    void wakeUpInterruptsPark() throws Exception {
        // Queue with no messages — reader will park; wakeUp should return promptly
        ActiveMQSourceConfig config = ActiveMQSourceConfig.builder()
            .brokerUrl(BROKER_URL)
            .queueNames(QUEUE_NAME)
            .pollIntervalMs(10_000) // very long park, wakeUp must cut it short
            .build();

        try (ActiveMQSplitReader reader = new ActiveMQSplitReader(config)) {
            reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(new ActiveMQSourceSplit(QUEUE_NAME))));

            Thread fetchThread = new Thread(() -> {
                try {
                    reader.fetch(); // will park — no messages
                } catch (Exception ignored) {}
            });
            fetchThread.start();

            // Give the fetcher thread a moment to enter parkUntilWakeup
            Thread.sleep(100);

            long before = System.currentTimeMillis();
            reader.wakeUp();
            fetchThread.join(2_000);
            long elapsed = System.currentTimeMillis() - before;

            assertThat(fetchThread.isAlive()).isFalse();
            assertThat(elapsed).isLessThan(5_000);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void sendTextMessages(String queueName, String... texts) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        try (Connection conn = factory.createConnection();
             Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            conn.start();
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);
            for (String text : texts) {
                producer.send(session.createTextMessage(text));
            }
        }
    }

    private List<String> drainText(ActiveMQSplitReader reader, int expected) throws Exception {
        ActiveMQMessageDeserializer<String> deser = ActiveMQMessageDeserializer.textMessage();
        List<String> results = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (results.size() < expected && System.currentTimeMillis() < deadline) {
            RecordsWithSplitIds<Message> batch = reader.fetch();
            String splitId;
            while ((splitId = batch.nextSplit()) != null) {
                Message msg;
                while ((msg = batch.nextRecordFromSplit()) != null) {
                    results.add(deser.deserialize(msg));
                }
            }
        }
        return results;
    }

    private List<Message> drainRaw(ActiveMQSplitReader reader, int expected) throws Exception {
        List<Message> results = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (results.size() < expected && System.currentTimeMillis() < deadline) {
            RecordsWithSplitIds<Message> batch = reader.fetch();
            String splitId;
            while ((splitId = batch.nextSplit()) != null) {
                Message msg;
                while ((msg = batch.nextRecordFromSplit()) != null) {
                    results.add(msg);
                }
            }
        }
        return results;
    }
}
