package org.apache.seatunnel.connectors.seatunnel.druid.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.druid.config.DruidSinkOptions;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;

/**
 *guanbo
 */
@AutoService(SeaTunnelSink.class)
public class DruidSink  extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private Config config;
    private SeaTunnelRowType seaTunnelRowType;
    private DruidSinkOptions druidSinkOptions;

    @Override
    public String getPluginName() {
        return "Druid";
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new DruidSinkWriter(seaTunnelRowType,druidSinkOptions);
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = pluginConfig;
        this.druidSinkOptions = new DruidSinkOptions(this.config);
    }

}
