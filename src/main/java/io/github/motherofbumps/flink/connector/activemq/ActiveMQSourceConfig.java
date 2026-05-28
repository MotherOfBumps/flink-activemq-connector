package io.github.motherofbumps.flink.connector.activemq;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Immutable configuration for {@link ActiveMQSource}.
 *
 * <p>Create instances via {@link #builder()}:
 * <pre>{@code
 * ActiveMQSourceConfig config = ActiveMQSourceConfig.builder()
 *     .brokerUrl("tcp://broker:61616")
 *     .queueNames("orders", "payments")
 *     .build();
 * }</pre>
 */
public final class ActiveMQSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String brokerUrl;
    private final String username;
    private final String password;
    private final List<String> queueNames;
    private final long pollIntervalMs;
    private final int maxBatchSize;

    private ActiveMQSourceConfig(Builder builder) {
        this.brokerUrl     = builder.brokerUrl;
        this.username      = builder.username;
        this.password      = builder.password;
        this.queueNames    = List.copyOf(builder.queueNames);
        this.pollIntervalMs = builder.pollIntervalMs;
        this.maxBatchSize  = builder.maxBatchSize;
    }

    public String getBrokerUrl()       { return brokerUrl; }
    public String getUsername()        { return username; }
    public String getPassword()        { return password; }
    public List<String> getQueueNames() { return queueNames; }
    public long getPollIntervalMs()    { return pollIntervalMs; }
    public int getMaxBatchSize()       { return maxBatchSize; }

    public static Builder builder() { return new Builder(); }

    // -------------------------------------------------------------------------

    public static final class Builder {

        private String brokerUrl    = "tcp://localhost:61616";
        private String username     = null;
        private String password     = null;
        private List<String> queueNames = List.of();
        private long pollIntervalMs = 100L;
        private int maxBatchSize    = 500;

        private Builder() {}

        /** ActiveMQ broker TCP URL, e.g. {@code tcp://broker-host:61616}. */
        public Builder brokerUrl(String brokerUrl) {
            this.brokerUrl = Objects.requireNonNull(brokerUrl, "brokerUrl");
            return this;
        }

        /** Optional JMS username. */
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        /** Optional JMS password. */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /** Queue names to consume. At least one is required. */
        public Builder queueNames(List<String> queueNames) {
            this.queueNames = Objects.requireNonNull(queueNames, "queueNames");
            return this;
        }

        /** Queue names to consume (varargs overload). At least one is required. */
        public Builder queueNames(String... queueNames) {
            this.queueNames = List.of(queueNames);
            return this;
        }

        /**
         * How long (ms) the fetcher thread parks between poll rounds
         * when all consumers return empty. Default: 100 ms.
         */
        public Builder pollIntervalMs(long pollIntervalMs) {
            if (pollIntervalMs <= 0) {
                throw new IllegalArgumentException("pollIntervalMs must be > 0");
            }
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }

        /**
         * Maximum messages drained from one consumer per {@code fetch()} round.
         * Default: 500.
         */
        public Builder maxBatchSize(int maxBatchSize) {
            if (maxBatchSize <= 0) {
                throw new IllegalArgumentException("maxBatchSize must be > 0");
            }
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public ActiveMQSourceConfig build() {
            Objects.requireNonNull(brokerUrl, "brokerUrl must not be null");
            if (queueNames.isEmpty()) {
                throw new IllegalStateException("At least one queue name must be configured");
            }
            return new ActiveMQSourceConfig(this);
        }
    }
}
