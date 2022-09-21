package org.apache.seatunnel.connectors.seatunnel.tikv.source;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVConfig;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

/**
 * TiKV is similar to Redis KV database
 *
 * @author XuJiaWei
 * @since 2022-09-15 18:11
 */
@AutoService(SeaTunnelSource.class)
public class TiKVSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final TiKVParameters tikvParameters = new TiKVParameters();

    private SeaTunnelRowType seaTunnelRowType;

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;


    @Override
    public String getPluginName() {
        return TiKVConfig.NAME;
    }

    /**
     * init configuration parameters
     *
     * @param config plugin config.
     * @throws PrepareFailException prepare failed
     */
    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, TiKVConfig.HOST, TiKVConfig.DATA_TYPE, TiKVConfig.DATA_TYPE);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        // init tikv configuration
        this.tikvParameters.initConfig(config);

        // init seaTunnelRowType
        if (config.hasPath(SeaTunnelSchema.SCHEMA)) {
            Config schema = config.getConfig(SeaTunnelSchema.SCHEMA);
            this.seaTunnelRowType = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
        } else {
            this.seaTunnelRowType = SeaTunnelSchema.buildSimpleTextSchema();
        }

        if (config.hasPath(TiKVConfig.FORMAT)) {
            this.deserializationSchema = null;
        } else {
            this.deserializationSchema = new JsonDeserializationSchema(false, false, seaTunnelRowType);
        }
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
        return new TiKVSourceReader(tikvParameters, readerContext, deserializationSchema);
    }

    @Override
    public Serializer<SingleSplitEnumeratorState> getEnumeratorStateSerializer() {
        return super.getEnumeratorStateSerializer();
    }


}
