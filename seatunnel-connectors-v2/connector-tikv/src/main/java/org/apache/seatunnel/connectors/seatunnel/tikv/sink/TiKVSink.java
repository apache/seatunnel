package org.apache.seatunnel.connectors.seatunnel.tikv.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVConfig;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.Optional;

/**
 * @author XuJiaWei
 * @since 2022-09-15 18:12
 */
@AutoService(SeaTunnelSink.class)
public class TiKVSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final TiKVParameters tiKVParameters = new TiKVParameters();
    private SeaTunnelRowType seaTunnelRowType;
    private Config config;

    @Override
    public String getPluginName() {
        return TiKVConfig.NAME;
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        this.config = config;
        CheckResult result = CheckConfigUtil.checkAllExists(config, TiKVConfig.HOST, TiKVConfig.PD_PORT, TiKVConfig.DATA_TYPE);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK, result.getMsg());
        }
        this.tiKVParameters.initConfig(config);
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
    public Optional<Serializer<Void>> getWriterStateSerializer() {
        return super.getWriterStateSerializer();
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new TiKVSinkWriter(seaTunnelRowType, tiKVParameters);

    }
}
