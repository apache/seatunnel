package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.MetaLakeType;

import java.util.Map;
import java.util.Optional;

public class MetaLakeSchemaDiscoverer {

    private ReadonlyConfig envOptions;
    private ReadonlyConfig sourceOptions;
    private Optional<Option<String>> schemaKeyOptional;
    private MetalakeClient metalakeClient;
    private MetaLakeTypeMapper metaLakeTypeMapper;

    public MetaLakeSchemaDiscoverer(
            TableSourceFactoryContext context, Optional<Option<String>> schemaKeyOptional) {
        this.envOptions = context.getEnvOptions();
        this.sourceOptions = context.getOptions();
        this.schemaKeyOptional = schemaKeyOptional;
    }

    public Map<String, TableSchema> discoverTableSchemas() {
        // schema 配置优先

        return null;
    }

    private MetaLakeType getMetaLakeType(TableSourceFactoryContext context) {
        // 查询env里的metaLake配置

        // 如果没有查询环境变量

        // 都没有就使用source中的配置
        return null;
    }
}
