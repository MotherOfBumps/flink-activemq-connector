# flink-activemq-connector

Apache Flink 2 Source Connector for [Apache ActiveMQ Classic 6.1](https://activemq.apache.org/components/classic/) queues.

## Features

- **Flink 2 New Source API** — built on `SingleThreadMultiplexSourceReaderBase` for a clean split between work discovery (JobManager) and data reading (TaskManager)
- **At-least-once delivery** — JMS sessions opened with `CLIENT_ACKNOWLEDGE`; messages are acknowledged only after the enclosing Flink checkpoint has fully committed
- **Multi-queue multiplexing** — a single background thread handles all queues assigned to one subtask via non-blocking `receiveNoWait()` polling
- **Pluggable deserialization** — implement `ActiveMQMessageDeserializer<T>` or use the built-in `textMessage()` / `bytesMessage()` helpers

## Requirements

| Component | Version |
|-----------|---------|
| Java | 21 |
| Apache Flink | 2.0.x |
| Apache ActiveMQ Classic | 6.1.x |

## Quick Start

```java
ActiveMQSourceConfig config = ActiveMQSourceConfig.builder()
    .brokerUrl("tcp://broker-host:61616")
    .username("admin")
    .password("admin")
    .queueNames("orders", "payments")
    .pollIntervalMs(50)
    .maxBatchSize(500)
    .build();

Source<String, ?, ?> source = ActiveMQSource.create(
    config,
    ActiveMQMessageDeserializer.textMessage());

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5_000); // required for at-least-once guarantees

DataStream<String> stream = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "activemq-source");
```

## Architecture

```
JobManager
└── ActiveMQSplitEnumerator
      Distributes queue names round-robin across subtasks on startup.
      Re-enqueues splits for a failed subtask via addSplitsBack.

TaskManager (one per parallel subtask)
└── ActiveMQSourceReader  ←  SingleThreadMultiplexSourceReaderBase
      │  Main thread: dequeues records from the fetch buffer,
      │  calls RecordEmitter, manages per-split checkpoint state.
      └── Single fetcher thread
            └── ActiveMQSplitReader
                  Polls N MessageConsumers (one per assigned queue)
                  via non-blocking receiveNoWait(); parks when idle.
                  Executes deferred JMS ACKs from a pending-task queue.
```

### Split model

Each ActiveMQ queue name becomes one `ActiveMQSourceSplit`. Splits are distributed
once at startup and retained for the lifetime of the job (unbounded source). One
`Session` is created per split so that `CLIENT_ACKNOWLEDGE` is scoped independently
to each queue.

### Checkpoint flow (at-least-once)

```
1.  Flink checkpoint barriers arrive at the source operator.
2.  snapshotState(checkpointId)
      - Delegates to base class (serialises split IDs for the checkpoint store).
      - Records lastConsumedMessage per split in ActiveMQSplitState.
3.  Downstream operators complete their checkpoint snapshots.
4.  notifyCheckpointComplete(checkpointId)
      - For each split: retrieves the high-water-mark message for that checkpoint.
      - Schedules message.acknowledge() onto the fetcher thread's pending-task queue.
      - Fetcher thread processes it at the next fetch() entry point.
      - In CLIENT_ACKNOWLEDGE mode, this ACKs all prior messages on the session.
5.  On job failure before step 4:
      - JMS session closes without ACK.
      - ActiveMQ re-delivers all unacknowledged messages to the new consumer.
      - This gives at-least-once: messages may be re-processed but never lost.
6.  notifyCheckpointAborted(checkpointId)
      - Drops the pending high-water-mark entry without scheduling an ACK.
      - Those messages are included in the next successful checkpoint instead.
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `brokerUrl` | `tcp://localhost:61616` | ActiveMQ broker TCP URL |
| `username` | `null` | Optional JMS username |
| `password` | `null` | Optional JMS password |
| `queueNames` | — | **Required.** One or more queue names |
| `pollIntervalMs` | `100` | Park duration when no messages are available |
| `maxBatchSize` | `500` | Max messages drained per consumer per `fetch()` |

## Custom Deserialization

```java
ActiveMQMessageDeserializer<MyEvent> deserializer = message -> {
    if (message instanceof BytesMessage bm) {
        byte[] bytes = new byte[(int) bm.getBodyLength()];
        bm.readBytes(bytes);
        return MyEvent.parseFrom(bytes); // e.g. Protobuf
    }
    throw new IllegalArgumentException("Unexpected type: " + message.getClass());
};

Source<MyEvent, ?, ?> source = ActiveMQSource.create(config, deserializer);
```

## Building

```bash
mvn clean package
```

## Running Tests

```bash
mvn test
```

Tests use an embedded ActiveMQ broker via the `vm://` transport — no external broker required.

## Extending for Exactly-Once

This connector provides **at-least-once** delivery. Exactly-once would require:

1. Switching from `CLIENT_ACKNOWLEDGE` to JMS transacted sessions.
2. Aligning the JMS transaction commit/rollback with Flink's two-phase commit protocol.
3. Implementing `TwoPhaseCommitSinkFunction`-style coordination so the JMS commit only fires after all downstream sinks have also committed.

## License

Apache 2.0
