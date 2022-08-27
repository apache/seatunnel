package org.apache.seatunnel.connectors.seatunnel.druid.sink;

import lombok.NonNull;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.druid.client.DruidOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.druid.config.DruidSinkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * guanbo
 */
public class DruidSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>{
    private static final Logger LOGGER = LoggerFactory.getLogger(DruidSinkWriter.class);
    private static final long serialVersionUID = -7210857670269773005L;
    private SeaTunnelRowType seaTunnelRowType;
    private DruidOutputFormat druidOutputFormat;


    public DruidSinkWriter(@NonNull SeaTunnelRowType seaTunnelRowType,
                          @NonNull DruidSinkOptions druidSinkOptions) {
        this.seaTunnelRowType = seaTunnelRowType;
        druidOutputFormat = new DruidOutputFormat(druidSinkOptions.getCoordinatorURL(),
                druidSinkOptions.getDatasource(),
                druidSinkOptions.getTimestampColumn(),
                druidSinkOptions.getTimestampFormat(),
                druidSinkOptions.getTimestampMissingValue(),
                druidSinkOptions.getColumns()
                );
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        druidOutputFormat.write(element);
    }

    @Override
    public void close() throws IOException {
        druidOutputFormat.closeOutputFormat();
    }
}
