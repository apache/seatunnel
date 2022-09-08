package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.URI;

import org.apache.seatunnel.api.common.PrepareFailException;
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
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbParameters;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigBeanFactory;

import com.google.auto.service.AutoService;

import java.io.IOException;

@AutoService(SeaTunnelSink.class)
public class MongodbSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private SeaTunnelRowType rowType;

    private MongodbParameters params;

    @Override
    public String getPluginName() {
        return "MongoDB";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, URI, DATABASE, COLLECTION);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }

        this.params = ConfigBeanFactory.create(config, MongodbParameters.class);
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
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new MongodbSinkWriter(rowType, params);
    }
}
