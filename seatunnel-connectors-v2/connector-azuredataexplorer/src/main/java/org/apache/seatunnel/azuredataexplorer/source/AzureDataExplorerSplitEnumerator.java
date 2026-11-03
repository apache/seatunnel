package org.apache.seatunnel.azuredataexplorer.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class AzureDataExplorerSplitEnumerator
        implements SourceSplitEnumerator<
                AzureDataExplorerSourceSplit, AzureDataExplorerSourceState> {

    private static final String SINGLE_SPLIT_ID = "adx-query";

    private final Context<AzureDataExplorerSourceSplit> context;
    private final Object stateLock = new Object();
    private final Map<Integer, List<AzureDataExplorerSourceSplit>> pendingSplit;
    private volatile boolean shouldEnumerate;

    public AzureDataExplorerSplitEnumerator(Context<AzureDataExplorerSourceSplit> context) {
        this(context, null);
    }

    public AzureDataExplorerSplitEnumerator(
            Context<AzureDataExplorerSourceSplit> context, AzureDataExplorerSourceState state) {
        this.context = context;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = state == null;
        if (state != null) {
            this.shouldEnumerate = state.isShouldEnumerate();
            this.pendingSplit.putAll(state.getPendingSplit());
        }
    }

    @Override
    public void open() {
        // No-op.
    }

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<AzureDataExplorerSourceSplit> splits =
                    Collections.singletonList(new AzureDataExplorerSourceSplit(SINGLE_SPLIT_ID));
            synchronized (stateLock) {
                addPendingSplit(splits);
                shouldEnumerate = false;
            }
            assignSplit(readers);
        }
        readers.forEach(context::signalNoMoreSplits);
    }

    private void addPendingSplit(Collection<AzureDataExplorerSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (AzureDataExplorerSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            pendingSplit
                    .computeIfAbsent(ownerReader, ignoredReader -> new ArrayList<>())
                    .add(split);
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        for (int reader : readers) {
            List<AzureDataExplorerSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    private static int getSplitOwner(String splitId, int numReaders) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public void addSplitsBack(List<AzureDataExplorerSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new SeaTunnelRuntimeException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                "Unsupported handleSplitRequest: " + subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public AzureDataExplorerSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new AzureDataExplorerSourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
