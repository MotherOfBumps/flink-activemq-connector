package io.github.motherofbumps.flink.connector.activemq.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * {@link SimpleVersionedSerializer} for {@link ActiveMQSourceSplit}.
 *
 * <p>Wire format (version 1): a single UTF-encoded queue name.
 */
public final class ActiveMQSplitSerializer
        implements SimpleVersionedSerializer<ActiveMQSourceSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ActiveMQSourceSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF(split.getQueueName());
        return out.getCopyOfBuffer();
    }

    @Override
    public ActiveMQSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                "Unsupported ActiveMQSourceSplit serialization version: " + version);
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        return new ActiveMQSourceSplit(in.readUTF());
    }
}
