package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.List;

public class Neo4jSourceSplitEnumerator implements SourceSplitEnumerator<Neo4jSourceSplit, Neo4jSourceState> {

    @Override
    public void open() {

    }

    @Override
    public void run() throws Exception {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<Neo4jSourceSplit> splits, int subtaskId) {

    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {

    }

    @Override
    public Neo4jSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        SourceSplitEnumerator.super.handleSourceEvent(subtaskId, sourceEvent);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        SourceSplitEnumerator.super.notifyCheckpointAborted(checkpointId);
    }
}
