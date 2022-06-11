package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.http.state.HttpState;

import java.io.IOException;
import java.util.List;

public class HttpSourceSplitEnumerator implements SourceSplitEnumerator<HttpSourceSplit, HttpState> {
    private SourceSplitEnumerator.Context<HttpSourceSplit> enumeratorContext;
    private HttpState httpState;

    public HttpSourceSplitEnumerator(Context<HttpSourceSplit> enumeratorContext) {
        this.enumeratorContext = enumeratorContext;
    }

    public HttpSourceSplitEnumerator(Context<HttpSourceSplit> enumeratorContext, HttpState httpState) {
        this.enumeratorContext = enumeratorContext;
        this.httpState = httpState;
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
    public void addSplitsBack(List<HttpSourceSplit> splits, int subtaskId) {

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
    public HttpState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
