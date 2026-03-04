package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarSinkState;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PulsarSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        PulsarSinkState,
                        PulsarCommitInfo,
                        PulsarAggregatedCommitInfo>,
                SupportMultiTableSink {

    private final SeaTunnelRowType seaTunnelRowType;
    private final PulsarClientConfig clientConfig;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    public PulsarSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();

        PulsarClientConfig.Builder clientConfigBuilder =
                PulsarClientConfig.builder()
                        .serviceUrl(readonlyConfig.get(PulsarSinkOptions.CLIENT_SERVICE_URL));

        clientConfigBuilder.authPluginClassName(
                readonlyConfig.get(PulsarSinkOptions.AUTH_PLUGIN_CLASS));

        clientConfigBuilder.authParams(readonlyConfig.get(PulsarSinkOptions.AUTH_PARAMS));

        this.clientConfig = clientConfigBuilder.build();
    }

    @Override
    public SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState> createWriter(
            SinkWriter.Context context) {

        return new PulsarSinkWriter(
                context, clientConfig, seaTunnelRowType, readonlyConfig, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState> restoreWriter(
            SinkWriter.Context context, List<PulsarSinkState> states) {

        return new PulsarSinkWriter(
                context, clientConfig, seaTunnelRowType, readonlyConfig, states);
    }

    @Override
    public Optional<Serializer<PulsarSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<PulsarCommitInfo>> createCommitter() {
        return Optional.of(new PulsarSinkCommitter(clientConfig));
    }

    @Override
    public Optional<Serializer<PulsarCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return PulsarSinkOptions.IDENTIFIER;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
