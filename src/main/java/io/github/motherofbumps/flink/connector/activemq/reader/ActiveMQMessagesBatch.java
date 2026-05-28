package io.github.motherofbumps.flink.connector.activemq.reader;

import jakarta.jms.Message;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link RecordsWithSplitIds} implementation that groups JMS {@link Message}
 * objects by their originating split ID.
 *
 * <p>The cursor pattern required by the interface is implemented with two
 * nested iterators: the outer advances through splits, the inner through
 * messages within the current split.
 *
 * <p>Package-private — callers interact with this only via the
 * {@link RecordsWithSplitIds} interface returned from
 * {@link ActiveMQSplitReader#fetch()}.
 */
final class ActiveMQMessagesBatch implements RecordsWithSplitIds<Message> {

    private final Iterator<Map.Entry<String, List<Message>>> splitIterator;
    private Iterator<Message> currentRecordIterator = Collections.emptyIterator();
    private final Set<String> finishedSplits;

    ActiveMQMessagesBatch(
            Map<String, List<Message>> recordsBySplit,
            Set<String> finishedSplits) {
        this.splitIterator  = recordsBySplit.entrySet().iterator();
        this.finishedSplits = finishedSplits;
    }

    /** Returns an empty batch with no finished splits. */
    static ActiveMQMessagesBatch empty() {
        return new ActiveMQMessagesBatch(Collections.emptyMap(), Collections.emptySet());
    }

    // -------------------------------------------------------------------------
    // RecordsWithSplitIds contract
    // -------------------------------------------------------------------------

    /**
     * Advances to the next split group.
     *
     * @return the split ID of the next group, or {@code null} when exhausted
     */
    @Override
    public String nextSplit() {
        if (splitIterator.hasNext()) {
            Map.Entry<String, List<Message>> entry = splitIterator.next();
            currentRecordIterator = entry.getValue().iterator();
            return entry.getKey();
        }
        currentRecordIterator = Collections.emptyIterator();
        return null;
    }

    /**
     * Returns the next record from the current split group, or {@code null}
     * when the group is exhausted (caller must then call {@link #nextSplit()}).
     */
    @Override
    public Message nextRecordFromSplit() {
        return currentRecordIterator.hasNext() ? currentRecordIterator.next() : null;
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }
}
