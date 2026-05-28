package io.github.motherofbumps.flink.connector.activemq.reader;

import jakarta.jms.BytesMessage;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import java.io.Serializable;

/**
 * Converts a raw JMS {@link Message} to the target output type {@code T}.
 *
 * <p>Implementations must be {@link Serializable} as they are distributed to
 * TaskManagers as part of the source operator configuration.
 *
 * <h3>Built-in implementations</h3>
 * <ul>
 *   <li>{@link #textMessage()} — reads the body of a {@link TextMessage}.</li>
 *   <li>{@link #bytesMessage()} — reads the full byte array body of a
 *       {@link BytesMessage}.</li>
 * </ul>
 *
 * <h3>Custom implementation example</h3>
 * <pre>{@code
 * ActiveMQMessageDeserializer<MyEvent> d = message -> {
 *     BytesMessage bm = (BytesMessage) message;
 *     byte[] bytes = new byte[(int) bm.getBodyLength()];
 *     bm.readBytes(bytes);
 *     return MyEvent.parseFrom(bytes); // Protobuf, Avro, etc.
 * };
 * }</pre>
 */
@FunctionalInterface
public interface ActiveMQMessageDeserializer<T> extends Serializable {

    /**
     * Deserializes {@code message} into an output record.
     *
     * @param message the non-null JMS message
     * @return the deserialized record
     * @throws Exception on any deserialization error
     */
    T deserialize(Message message) throws Exception;

    // -------------------------------------------------------------------------
    // Built-in helpers
    // -------------------------------------------------------------------------

    /** Extracts the string body of a {@link TextMessage}. */
    static ActiveMQMessageDeserializer<String> textMessage() {
        return message -> {
            if (message instanceof TextMessage tm) {
                return tm.getText();
            }
            throw new IllegalArgumentException(
                "Expected TextMessage but received " + message.getClass().getSimpleName());
        };
    }

    /** Reads the full byte-array body of a {@link BytesMessage}. */
    static ActiveMQMessageDeserializer<byte[]> bytesMessage() {
        return message -> {
            if (message instanceof BytesMessage bm) {
                byte[] data = new byte[(int) bm.getBodyLength()];
                bm.readBytes(data);
                return data;
            }
            throw new IllegalArgumentException(
                "Expected BytesMessage but received " + message.getClass().getSimpleName());
        };
    }
}
