package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Hive Sink implementation by using SeaTunnel sink API.
 * This class contains the method to create {@link HiveSinkWriter} and {@link HiveSinkAggregatedCommitter}.
 */
@AutoService(SeaTunnelSink.class)
public class HiveSink implements SeaTunnelSink<SeaTunnelRow, HiveSinkState, HiveCommitInfo, HiveAggregatedCommitInfo> {

    private Config config;
    private long sinkId;
    private SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void setTypeInfo(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = pluginConfig;
        this.sinkId = System.currentTimeMillis();
    }

    @Override
    public SinkWriter<SeaTunnelRow, HiveCommitInfo, HiveSinkState> createWriter(SinkWriter.Context context) throws IOException {
        return new HiveSinkWriter(seaTunnelRowTypeInfo, config, context, System.currentTimeMillis());
    }

    @Override
    public SinkWriter<SeaTunnelRow, HiveCommitInfo, HiveSinkState> restoreWriter(SinkWriter.Context context, List<HiveSinkState> states) throws IOException {
        return new HiveSinkWriter(seaTunnelRowTypeInfo, config, context, System.currentTimeMillis());
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return null;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {

    }

    @Override
    public Optional<SinkAggregatedCommitter<HiveCommitInfo, HiveAggregatedCommitInfo>> createAggregatedCommitter() throws IOException {
        return Optional.of(new HiveSinkAggregatedCommitter());
    }
}
