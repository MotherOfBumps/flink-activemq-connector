package io.github.motherofbumps.flink.connector.activemq.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * A {@link SourceSplit} representing a single ActiveMQ queue.
 *
 * <p>The split ID equals the queue name, making it stable across
 * checkpoint/restore cycles and human-readable in logs.
 */
public final class ActiveMQSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String queueName;

    public ActiveMQSourceSplit(String queueName) {
        this.queueName = Objects.requireNonNull(queueName, "queueName");
    }

    /** The split ID is the queue name. */
    @Override
    public String splitId() {
        return queueName;
    }

    public String getQueueName() {
        return queueName;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ActiveMQSourceSplit other && queueName.equals(other.queueName);
    }

    @Override
    public int hashCode() {
        return queueName.hashCode();
    }

    @Override
    public String toString() {
        return "ActiveMQSourceSplit{queue='" + queueName + "'}";
    }
}
