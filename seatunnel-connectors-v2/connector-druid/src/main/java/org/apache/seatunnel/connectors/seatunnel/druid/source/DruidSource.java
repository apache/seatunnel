package org.apache.seatunnel.connectors.seatunnel.druid.source;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.connectors.seatunnel.druid.client.DruidInputFormat;
import org.apache.seatunnel.connectors.seatunnel.druid.config.DruidSourceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * guanbo
 */
@AutoService(SeaTunnelSource.class)
public class DruidSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DruidSource.class);

    private SeaTunnelRowType rowTypeInfo;
    private DruidInputFormat druidInputFormat;
    private  DruidSourceOptions druidSourceOptions;

    @Override
    public String getPluginName() {
        return "Druid";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        LOGGER.info("Druid source prepare");
        try {
            druidSourceOptions = new DruidSourceOptions(pluginConfig);
            druidInputFormat = new DruidInputFormat(druidSourceOptions);
            this.rowTypeInfo = druidInputFormat.getRowTypeInfo();
        }catch (Exception e){
            throw new PrepareFailException("Druid", PluginType.SOURCE, e.toString());
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        LOGGER.info("Druid source createReader");
        return new DruidSourceReader(readerContext,this.druidInputFormat);
    }
}
