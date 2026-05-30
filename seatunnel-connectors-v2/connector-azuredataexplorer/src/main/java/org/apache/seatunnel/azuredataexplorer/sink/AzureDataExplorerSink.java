package org.apache.seatunnel.azuredataexplorer.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerConfig;

public class AzureDataExplorerSink
        implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void>, TableSink {

    private final ReadonlyConfig config;
    private SeaTunnelRowType rowType;

    public AzureDataExplorerSink(ReadonlyConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return AzureDataExplorerSinkFactory.IDENTIFIER;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return rowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
        return new AzureDataExplorerSinkWriter(
                AzureDataExplorerConfig.fromSinkConfig(config), rowType);
    }

    @Override
    public SeaTunnelSink createSink() {
        return this;
    }
}
