/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.schema.ReadonlyConfigParser;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Utils contains some common methods for construct CatalogTable. */
@Slf4j
public class CatalogTableUtil implements Serializable {

    private static final SeaTunnelRowType SIMPLE_SCHEMA =
            new SeaTunnelRowType(
                    new String[] {"content"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

    @Deprecated
    public static CatalogTable getCatalogTable(String tableName, SeaTunnelRowType rowType) {
        return getCatalogTable("schema", "default", null, tableName, rowType);
    }

    public static CatalogTable getCatalogTable(
            String catalog,
            String database,
            String schema,
            String tableName,
            SeaTunnelRowType rowType) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            PhysicalColumn column =
                    PhysicalColumn.of(
                            rowType.getFieldName(i), rowType.getFieldType(i), 0, true, null, null);
            schemaBuilder.column(column);
        }
        return CatalogTable.of(
                TableIdentifier.of(catalog, database, schema, tableName),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "It is converted from RowType and only has column information.");
    }

    /**
     * Get catalog table from config, if schema is specified, return a catalog table with specified
     * schema, otherwise, return a catalog table with schema from catalog.
     *
     * @deprecated DO NOT invoke it in any new TableSourceFactory/TableSinkFactory, please directly
     *     use TableSourceFactory/TableSinkFactory instance to get CatalogTable. We just use it to
     *     transition the old CatalogTable creation logic. Details please <a
     *     href="https://cwiki.apache.org/confluence/display/SEATUNNEL/STIP5-Refactor+Catalog+and+CatalogTable">check
     *     </a>
     */
    @Deprecated
    public static List<CatalogTable> getCatalogTables(
            ReadonlyConfig readonlyConfig, ClassLoader classLoader) {

        // We use plugin_name as factoryId, so MySQL-CDC should be MySQL
        String factoryId =
                readonlyConfig.get(ConnectorCommonOptions.PLUGIN_NAME).replace("-CDC", "");
        return getCatalogTables(factoryId, readonlyConfig, classLoader);
    }

    @Deprecated
    public static List<CatalogTable> getCatalogTables(
            String factoryId, ReadonlyConfig readonlyConfig, ClassLoader classLoader) {
        // Highest priority: specified schema
        Map<String, Object> schemaMap = readonlyConfig.get(ConnectorCommonOptions.SCHEMA);
        if (schemaMap != null) {
            if (schemaMap.isEmpty()) {
                throw new SeaTunnelException("Schema config can not be empty");
            }
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(factoryId, readonlyConfig);
            return Collections.singletonList(catalogTable);
        }

        Optional<Catalog> optionalCatalog =
                FactoryUtil.createOptionalCatalog(
                        factoryId, readonlyConfig, classLoader, factoryId);
        return optionalCatalog
                .map(
                        c -> {
                            try (Catalog catalog = c) {
                                long startTime = System.currentTimeMillis();
                                catalog.open();
                                List<CatalogTable> catalogTables =
                                        catalog.getTables(readonlyConfig);
                                log.info(
                                        String.format(
                                                "Get catalog tables, cost time: %d ms",
                                                System.currentTimeMillis() - startTime));
                                if (catalogTables.isEmpty()) {
                                    throw new SeaTunnelException(
                                            String.format(
                                                    "Can not find catalog table with factoryId [%s]",
                                                    factoryId));
                                }
                                return catalogTables;
                            }
                        })
                .orElseThrow(
                        () ->
                                new SeaTunnelException(
                                        String.format(
                                                "Can not find catalog with factoryId [%s]",
                                                factoryId)));
    }

    public static CatalogTable buildWithConfig(Config config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        return buildWithConfig(readonlyConfig);
    }

    public static SeaTunnelDataType<SeaTunnelRow> convertToDataType(
            List<CatalogTable> catalogTables) {
        if (catalogTables.size() == 1) {
            return catalogTables.get(0).getTableSchema().toPhysicalRowDataType();
        } else {
            return convertToMultipleRowType(catalogTables);
        }
    }

    @Deprecated
    private static MultipleRowType convertToMultipleRowType(List<CatalogTable> catalogTables) {
        Map<String, SeaTunnelRowType> rowTypeMap = new HashMap<>();
        for (CatalogTable catalogTable : catalogTables) {
            String tableId = catalogTable.getTableId().toTablePath().toString();
            rowTypeMap.put(tableId, catalogTable.getTableSchema().toPhysicalRowDataType());
        }
        return new MultipleRowType(rowTypeMap);
    }

    // We need to use buildWithConfig(String catalogName, ReadonlyConfig readonlyConfig);
    // Since this method will not inject the correct catalogName into CatalogTable
    @Deprecated
    public static List<CatalogTable> convertDataTypeToCatalogTables(
            SeaTunnelDataType<?> seaTunnelDataType, String tableId) {
        List<CatalogTable> catalogTables;
        if (seaTunnelDataType instanceof MultipleRowType) {
            catalogTables = new ArrayList<>();
            for (String id : ((MultipleRowType) seaTunnelDataType).getTableIds()) {
                catalogTables.add(
                        CatalogTableUtil.getCatalogTable(
                                id, ((MultipleRowType) seaTunnelDataType).getRowType(id)));
            }
        } else {
            catalogTables =
                    Collections.singletonList(
                            CatalogTableUtil.getCatalogTable(
                                    tableId, (SeaTunnelRowType) seaTunnelDataType));
        }
        return catalogTables;
    }

    public static CatalogTable buildWithConfig(ReadonlyConfig readonlyConfig) {
        return buildWithConfig("", readonlyConfig);
    }

    public static CatalogTable buildWithConfig(String catalogName, ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.get(ConnectorCommonOptions.SCHEMA) == null) {
            throw new RuntimeException(
                    "Schema config need option [schema], please correct your config first");
        }
        TableSchema tableSchema = new ReadonlyConfigParser().parse(readonlyConfig);

        ReadonlyConfig schemaConfig =
                readonlyConfig
                        .getOptional(ConnectorCommonOptions.SCHEMA)
                        .map(ReadonlyConfig::fromMap)
                        .orElseThrow(
                                () -> new IllegalArgumentException("Schema config can't be null"));

        TablePath tablePath;
        if (StringUtils.isNotEmpty(schemaConfig.get(ConnectorCommonOptions.TABLE))) {
            tablePath =
                    TablePath.of(
                            schemaConfig.get(ConnectorCommonOptions.TABLE),
                            schemaConfig.get(ConnectorCommonOptions.SCHEMA_FIRST));
        } else {
            Optional<String> pluginOutputIdentifierOptional =
                    readonlyConfig.getOptional(ConnectorCommonOptions.PLUGIN_OUTPUT);
            tablePath = pluginOutputIdentifierOptional.map(TablePath::of).orElse(TablePath.DEFAULT);
        }

        return CatalogTable.of(
                TableIdentifier.of(catalogName, tablePath),
                tableSchema,
                new HashMap<>(),
                // todo: add partitionKeys?
                new ArrayList<>(),
                readonlyConfig.get(ConnectorCommonOptions.TABLE_COMMENT));
    }

    public static SeaTunnelRowType buildSimpleTextSchema() {
        return SIMPLE_SCHEMA;
    }

    public static CatalogTable buildSimpleTextTable() {
        return getCatalogTable("default", buildSimpleTextSchema());
    }

    public static CatalogTable newCatalogTable(
            CatalogTable catalogTable, SeaTunnelRowType seaTunnelRowType) {
        TableSchema tableSchema = catalogTable.getTableSchema();

        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();

        List<Column> finalColumns = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            Column column = columnMap.get(fieldNames[i]);
            if (column != null) {
                finalColumns.add(column);
            } else {
                finalColumns.add(
                        PhysicalColumn.of(fieldNames[i], fieldTypes[i], 0, true, null, null));
            }
        }

        TableSchema finalSchema =
                TableSchema.builder()
                        .columns(finalColumns)
                        .primaryKey(tableSchema.getPrimaryKey())
                        .constraintKey(tableSchema.getConstraintKeys())
                        .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                finalSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName());
    }
}
