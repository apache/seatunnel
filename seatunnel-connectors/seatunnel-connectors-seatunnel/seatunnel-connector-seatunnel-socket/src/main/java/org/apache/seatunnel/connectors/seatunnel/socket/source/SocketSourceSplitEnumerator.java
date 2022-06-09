package org.apache.seatunnel.connectors.seatunnel.socket.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.socket.state.SocketState;

import java.io.IOException;
import java.util.List;

public class SocketSourceSplitEnumerator implements SourceSplitEnumerator<SocketSourceSplit, SocketState> {

    private final SourceSplitEnumerator.Context<SocketSourceSplit> enumeratorContext;

    public SocketSourceSplitEnumerator(SourceSplitEnumerator.Context<SocketSourceSplit> enumeratorContext) {
        this.enumeratorContext = enumeratorContext;
    }

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
    public void addSplitsBack(List<SocketSourceSplit> splits, int subtaskId) {

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
    public SocketState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
