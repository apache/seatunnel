package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

public class AerospikeSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private final ReadonlyConfig pluginConfig;
    private final CatalogTable catalogTable;

    public AerospikeSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.pluginConfig = pluginConfig;
        this.catalogTable = catalogTable;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new AerospikeSinkWriter(catalogTable.getSeaTunnelRowType(), pluginConfig);
    }

    @Override
    public String getPluginName() {
        return "aerospike";
    }
}
