package org.apache.seatunnel.connectors.seatunnel.adbpostgresql.sink;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;

public class ADBPostgreSQLSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private Config pluginConfig;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "ADB PostgreSQL";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {

    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return null;
    }
}
