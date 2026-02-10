package org.apache.seatunnel.connectors.seatunnel.mqtt.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.Optional;

public class MqttSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final SinkWriter.Context context;
    private final SeaTunnelRowType rowType;
    private final ReadonlyConfig pluginConfig;

    public MqttSinkWriter(
            SinkWriter.Context context, SeaTunnelRowType rowType, ReadonlyConfig pluginConfig) {
        this.context = context;
        this.rowType = rowType;
        this.pluginConfig = pluginConfig;
    }

    @Override
    public void write(SeaTunnelRow element) {
        // TODO: publish to MQTT
    }

    @Override
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        // no-op
    }

    @Override
    public void close() {
        // TODO: close mqtt client
    }
}
