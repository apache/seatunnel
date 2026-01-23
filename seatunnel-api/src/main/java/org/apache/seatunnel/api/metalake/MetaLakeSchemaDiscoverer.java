package org.apache.seatunnel.api.metalake;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.schema.ReadonlyConfigParser;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.MetaLakeType;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
        this.metalakeClient = MetaLakeFactory.createClient(getMetaLakeType());
        this.metaLakeTypeMapper = MetaLakeFactory.createTypeMapper(getMetaLakeType());
    }

    // 改成 List<CatalogTable> ?
    public Map<String, CatalogTable> discoverTableSchemas() {
        final Map<String,CatalogTable> tableSchemaMap = new LinkedHashMap<>();
        // 单表 schema
        if(sourceOptions.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()){
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(sourceOptions);
            if (!schemaKeyOptional.isPresent()) {
                tableSchemaMap.put("",catalogTable);
                return tableSchemaMap;
            }
            Optional<String> optional = sourceOptions.getOptional(schemaKeyOptional.get());
            if (!optional.isPresent()){
                tableSchemaMap.put("",catalogTable);
                return tableSchemaMap;
            } else {
                tableSchemaMap.put(optional.get(),catalogTable);
                return tableSchemaMap;
            }
        }
        //

        return tableSchemaMap;
    }

    /*private CatalogTable discoverTableSchema(ReadonlyConfig readonlyConfig) {

    }

    private CatalogTable discoverTableSchemaFromConfig(ReadonlyConfig readonlyConfig) {

    }

    private CatalogTable discoverTableSchemaFromMetaLake(ReadonlyConfig readonlyConfig) {

    }*/

    private MetaLakeType getMetaLakeType() {
        // 应该就近原则 优先级分别是 source配置 env配置 环境变量配置
        // first evn
        if (envOptions.getOptional(EnvCommonOptions.METALAKE_TYPE).isPresent()){
            return envOptions.get(EnvCommonOptions.METALAKE_TYPE);
        }
        // second system
        if(StringUtils.isNotEmpty(System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase()))){
            return MetaLakeType.valueOf(System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase()));
        }
        // third source config
        if (sourceOptions.getOptional(TableSchemaOptions.METALAKE_TYPE).isPresent()){
            return sourceOptions.get(TableSchemaOptions.METALAKE_TYPE);
        }
        // default
        return MetaLakeType.GRAVITINO;
    }
}
