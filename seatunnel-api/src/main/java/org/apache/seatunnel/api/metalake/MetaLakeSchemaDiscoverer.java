package org.apache.seatunnel.api.metalake;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.MetaLakeType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode.INVALID_SCHEMA_STRUCTURE;

@Slf4j
public class MetaLakeSchemaDiscoverer {

    private ReadonlyConfig envOptions;
    private ReadonlyConfig sourceOptions;
    private String catalogName;
    private MetalakeClient metalakeClient;
    private MetaLakeTypeMapper metaLakeTypeMapper;

    public MetaLakeSchemaDiscoverer(
            TableSourceFactoryContext context,String catalogName) {
        this.envOptions = context.getEnvOptions();
        this.sourceOptions = context.getOptions();
        this.catalogName = catalogName;
        this.metalakeClient = MetaLakeFactory.createClient(getMetaLakeType());
        this.metaLakeTypeMapper = MetaLakeFactory.createTypeMapper(getMetaLakeType());
    }


    public List<CatalogTable> discoverTableSchemas() {
        List<CatalogTable> catalogTables = new ArrayList<>();
        // 单表 schema
        if(sourceOptions.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()){
            catalogTables.add(discoverTableSchema(sourceOptions));
            return catalogTables;
        }
        // table_config and table_list

        return catalogTables;
    }

    private CatalogTable discoverTableSchema(ReadonlyConfig readonlyConfig) {
        // 里边有field 或者 columns属性
        if (){
            return discoverTableSchemaFromConfig(readonlyConfig);
        }
        // 里边有schema_url属性
        if (){
            return discoverTableSchemaFromMetaLake();
        }
        throw new SeaTunnelRuntimeException(INVALID_SCHEMA_STRUCTURE,
                "Schema config need option [schema], please correct your config first");
    }

    private CatalogTable discoverTableSchemaFromConfig(ReadonlyConfig readonlyConfig) {
        return CatalogTableUtil.buildWithConfig(catalogName,readonlyConfig);
    }

    private CatalogTable discoverTableSchemaFromMetaLake(String schemaUrl) {
        try {
            JsonNode schemaNode = metalakeClient.getSchema(schemaUrl);
            return metaLakeTypeMapper.convertor(schemaNode);
        } catch (IOException e) {
            // 返回默认
            log.error("", e);
        }
    }

    private MetaLakeType getMetaLakeType() {
        // first source
        if (sourceOptions.getOptional(TableSchemaOptions.METALAKE_TYPE).isPresent()){
            return sourceOptions.get(TableSchemaOptions.METALAKE_TYPE);
        }
        // second env
        if (envOptions.getOptional(EnvCommonOptions.METALAKE_TYPE).isPresent()){
            return envOptions.get(EnvCommonOptions.METALAKE_TYPE);
        }
        // third system
        if(StringUtils.isNotEmpty(System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase()))){
            return MetaLakeType.valueOf(System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase()));
        }
        // default
        return MetaLakeType.GRAVITINO;
    }
}
