package org.apache.seatunnel.connectors.seatunnel.sls.sink;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsCommitInfo;

import java.io.IOException;
import java.util.List;

public class SlsSinkCommitter implements SinkCommitter<SlsCommitInfo> {
    @Override
    public List<SlsCommitInfo> commit(List<SlsCommitInfo> commitInfos) throws IOException {
        // nothing to do, when write function, data had sended
        return null;
    }

    @Override
    public void abort(List<SlsCommitInfo> commitInfos) throws IOException {}
}
