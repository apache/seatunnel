package org.apache.seatunnel.connectors.pinecone.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.pinecone.config.PineconeSourceConfig;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@Slf4j
@AutoService(Factory.class)
public class PineconeSourceFactory implements TableSourceFactory {

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new PineconeSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(PineconeSourceConfig.API_KEY).optional().build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return PineconeSource.class;
    }

    @Override
    public String factoryIdentifier() {
        return PineconeSourceConfig.CONNECTOR_IDENTITY;
    }
}
