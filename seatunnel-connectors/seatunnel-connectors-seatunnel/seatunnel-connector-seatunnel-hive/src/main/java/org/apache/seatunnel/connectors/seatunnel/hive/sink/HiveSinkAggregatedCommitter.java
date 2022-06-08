package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import java.io.IOException;
import java.util.List;

public class HiveSinkAggregatedCommitter implements SinkAggregatedCommitter<HiveCommitInfo, HiveAggregatedCommitInfo> {

    @Override
    public List<HiveAggregatedCommitInfo> commit(List<HiveAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        return null;
    }

    @Override
    public HiveAggregatedCommitInfo combine(List<HiveCommitInfo> commitInfos) {
        return null;
    }

    @Override
    public void abort(List<HiveAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {

    }

    @Override
    public void close() throws IOException {

    }
}
