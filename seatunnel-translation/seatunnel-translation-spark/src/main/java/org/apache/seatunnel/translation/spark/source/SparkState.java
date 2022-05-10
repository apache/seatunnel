package org.apache.seatunnel.translation.spark.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.ParallelSource;

import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

import java.util.List;

public class SparkState<SplitT extends SourceSplit, StateT> extends Offset {

    protected final ParallelSource<SeaTunnelRow, SplitT, StateT> parallelSource;
    protected volatile Integer checkpointId;

    public SparkState(ParallelSource<SeaTunnelRow, SplitT, StateT> parallelSource, int checkpointId) {
        this.parallelSource = parallelSource;
        this.checkpointId = checkpointId;
    }

    @Override
    public String json() {
        try {
            List<byte[]> bytes = this.parallelSource.snapshotState(checkpointId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
