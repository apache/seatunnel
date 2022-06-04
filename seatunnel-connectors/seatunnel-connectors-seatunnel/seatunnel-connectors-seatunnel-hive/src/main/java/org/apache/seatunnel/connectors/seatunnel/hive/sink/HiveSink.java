package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Hive Sink implementation by using SeaTunnel sink API.
 * This class contains the method to create {@link HiveSinkWriter} and {@link HiveSinkCommitter}.
 */
@AutoService(SeaTunnelSink.class)
public class HiveSink implements SeaTunnelSink{
    private Config config;
    private SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = pluginConfig;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
    }

    @Override
    public SinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return null;
    }

    @Override
    public SinkWriter restoreWriter(SinkWriter.Context context, List states) throws IOException {
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<Serializer> getWriterStateSerializer() {
        return SeaTunnelSink.super.getWriterStateSerializer();
    }

    @Override
    public Optional<SinkCommitter> createCommitter() throws IOException {
        return SeaTunnelSink.super.createCommitter();
    }

    @Override
    public Optional<Serializer> getCommitInfoSerializer() {
        return SeaTunnelSink.super.getCommitInfoSerializer();
    }

    @Override
    public Optional<SinkAggregatedCommitter> createAggregatedCommitter() throws IOException {
        return SeaTunnelSink.super.createAggregatedCommitter();
    }

    @Override
    public Optional<Serializer> getAggregatedCommitInfoSerializer() {
        return SeaTunnelSink.super.getAggregatedCommitInfoSerializer();
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return SeaTunnelSink.super.getSeaTunnelContext();
    }
}
