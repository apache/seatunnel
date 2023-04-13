package org.apache.seatunnel.connectors.seatunnel.mongodbv2.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import java.io.IOException;
import java.util.List;

public class MongodbSinkAggregatedCommitter  implements SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo> {
    @Override
    public List<MongodbAggregatedCommitInfo> commit(List<MongodbAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        return null;
    }

    @Override
    public MongodbAggregatedCommitInfo combine(List<MongodbCommitInfo> commitInfos) {
        return null;
    }

    @Override
    public void abort(List<MongodbAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {

    }

    @Override
    public void close() throws IOException {

    }
}
