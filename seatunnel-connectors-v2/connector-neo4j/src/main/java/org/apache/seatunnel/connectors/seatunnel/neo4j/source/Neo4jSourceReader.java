package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.List;

public class Neo4jSourceReader implements SourceReader<SeaTunnelRow, Neo4jSourceSplit> {
    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {

    }

    @Override
    public List<Neo4jSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<Neo4jSourceSplit> splits) {

    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void handleSourceEvent(SourceEvent sourceEvent) {
        SourceReader.super.handleSourceEvent(sourceEvent);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        SourceReader.super.notifyCheckpointAborted(checkpointId);
    }
}
