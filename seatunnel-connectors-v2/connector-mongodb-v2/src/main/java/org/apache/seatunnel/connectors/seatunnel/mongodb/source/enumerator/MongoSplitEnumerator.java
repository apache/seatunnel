package org.apache.seatunnel.connectors.seatunnel.mongodb.source.enumerator;

import com.google.common.collect.Lists;
import com.mongodb.MongoNamespace;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplitStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * MongoSplitEnumerator generates {@link MongoSplit} according to partition strategies.
 **/
public class MongoSplitEnumerator implements SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSplitEnumerator.class);

    private final ArrayList<MongoSplit> pendingSplits = Lists.newArrayList();

    private final Context<MongoSplit> context;

    private final MongoClientProvider clientProvider;

    private MongoSplitStrategy strategy;

    public MongoSplitEnumerator(Context<MongoSplit> context,
                                MongoClientProvider clientProvider,
                                MongoSplitStrategy strategy) {
        this(context, clientProvider, strategy, Collections.emptyList());
    }

    public MongoSplitEnumerator(Context<MongoSplit> context,
                                MongoClientProvider clientProvider,
                                MongoSplitStrategy strategy,
                                List<MongoSplit> splits) {
        this.context = context;
        this.clientProvider = clientProvider;
        this.strategy = strategy;
        this.pendingSplits.addAll(splits);
    }


    @Override
    public void open() {

    }

    @Override
    public void run() throws Exception {
        LOG.info("Starting MongoSplitEnumerator.");
        pendingSplits.addAll(strategy.split());
        MongoNamespace namespace = clientProvider.getDefaultCollection().getNamespace();
        LOG.info("Added {} pending splits for namespace {}.",
                pendingSplits.size(), namespace.getFullName());
        assignSplit(context.registeredReaders());
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<MongoSplit> splits, int subtaskId) {
        if (splits != null) {
            LOG.info("Received {} split(s) back from subtask {}.", splits.size(), subtaskId);
            pendingSplits.addAll(splits);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) { // 处理增量的逻辑
        throw new MongodbConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        LOG.debug("Register reader {} to MongodbSourceSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public ArrayList<MongoSplit> snapshotState(long checkpointId) throws Exception {
        return pendingSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Do nothing
    }

    private void assignSplit(Collection<Integer> readers) {
        LOG.debug("Assign pendingSplits to readers {}", readers);
        for (int subtaskId : readers) {
            LOG.info("Received split request from task {} on host {}.", subtaskId);
            if (pendingSplits.size() > 0) {
                MongoSplit nextSplit = pendingSplits.remove(0);
                context.assignSplit(subtaskId, nextSplit);
                LOG.info("Assigned split {} to subtask {}, remaining splits: {}.", nextSplit.splitId(), subtaskId,
                        pendingSplits.size());
            } else {
                LOG.info("No more splits can be assign, signal subtask {}.", subtaskId);
                context.signalNoMoreSplits(subtaskId);
            }

        }
    }
}
