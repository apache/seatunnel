package org.apache.seatunnel.connectors.seatunnel.sls.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.sls.config.Config;

import java.io.Serializable;

@AutoService(Factory.class)
public class SlsSourceFactory implements TableSourceFactory {
    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SlsSource.class;
    }

    @Override
    public String factoryIdentifier() {
        return Config.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(Config.ENDPOINT, Config.PROJECT, Config.LOGSTORE, Config.CONSUMER_GROUP,
                        Config.ACCESS_KEY_ID, Config.ACCESS_KEY_SECRET)
                .optional(
                        Config.BATCH_SIZE,
                        Config.POSITION_MODE,
                        Config.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                        Config.CONSUMER_NUM)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
    TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new SlsSource(context.getOptions());
    }
}
