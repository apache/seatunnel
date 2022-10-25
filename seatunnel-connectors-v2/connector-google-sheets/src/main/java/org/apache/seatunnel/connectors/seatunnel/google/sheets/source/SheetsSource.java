package org.apache.seatunnel.connectors.seatunnel.google.sheets.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsParameters;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class SheetsSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private SeaTunnelRowType seaTunnelRowType;

    private SheetsParameters sheetsParameters;

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    @Override
    public String getPluginName() {
        return "GoogleSheets";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.sheetsParameters = new SheetsParameters().buildWithConfig(pluginConfig);
        if (pluginConfig.hasPath(SeaTunnelSchema.SCHEMA)) {
            Config schema = pluginConfig.getConfig(SeaTunnelSchema.SCHEMA);
            this.seaTunnelRowType = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
        } else {
            this.seaTunnelRowType = SeaTunnelSchema.buildSimpleTextSchema();
        }
        this.deserializationSchema = new JsonDeserializationSchema(false, false, seaTunnelRowType);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new SheetsSourceReader(sheetsParameters, readerContext, deserializationSchema);
    }
}
