package org.apache.seatunnel.connectors.seatunnel.tdengine.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class TDengineSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "TDengine";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        TDengineSinkOptions.URL,
                        TDengineSinkOptions.USERNAME,
                        TDengineSinkOptions.PASSWORD,
                        TDengineSinkOptions.DATABASE,
                        TDengineSinkOptions.STABLE)
                .optional(
                        TDengineSinkOptions.TIMEZONE,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        TDengineSinkConfig tdengineSinkConfig = TDengineSinkConfig.of(context.getOptions());
        return () -> new TDengineSink(tdengineSinkConfig, context.getCatalogTable());
    }
}
