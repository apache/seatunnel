package org.apache.seatunnel.connectors.seatunnel.typesense.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.typesense.config.SourceConfig.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseConnectionConfig.APIKEY;
import static org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseConnectionConfig.HOSTS;

// 不配置该类，无法成功加载，会提示找不到插件
@AutoService(Factory.class)
public class TypesenseSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Typesense";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(HOSTS, APIKEY).optional(COLLECTION).build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new TypesenseSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return TypesenseSource.class;
    }
}
