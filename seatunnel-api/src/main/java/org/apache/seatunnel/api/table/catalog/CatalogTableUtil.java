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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.utils.JsonUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class CatalogTableUtil implements Serializable {
    public static final Option<Map<String, String>> SCHEMA =
            Options.key("schema").mapType().noDefaultValue().withDescription("SeaTunnel Schema");

    public static final Option<String> FIELDS =
            Options.key("schema.fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Fields");
    private static final String FIELD_KEY = "fields";

    private static final SeaTunnelRowType SIMPLE_SCHEMA =
            new SeaTunnelRowType(
                    new String[] {"content"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

    @Getter private final CatalogTable catalogTable;

    private CatalogTableUtil(CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
    }

    @Deprecated
    public static CatalogTable getCatalogTable(String tableName, SeaTunnelRowType rowType) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            PhysicalColumn column =
                    PhysicalColumn.of(
                            rowType.getFieldName(i), rowType.getFieldType(i), 0, true, null, null);
            schemaBuilder.column(column);
        }
        return CatalogTable.of(
                TableIdentifier.of("schema", "default", tableName),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "It is converted from RowType and only has column information.");
    }

    public static List<CatalogTable> getCatalogTables(Config config, ClassLoader classLoader) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        Map<String, String> catalogOptions =
                readonlyConfig.getOptional(CatalogOptions.CATALOG_OPTIONS).orElse(new HashMap<>());

        Map<String, Object> catalogAllOptions = new HashMap<>();
        catalogAllOptions.putAll(readonlyConfig.toMap());
        catalogAllOptions.putAll(catalogOptions);
        ReadonlyConfig catalogConfig = ReadonlyConfig.fromMap(catalogAllOptions);
        String factoryId = catalogConfig.get(CommonOptions.FACTORY_ID);
        // Highest priority: specified schema
        Map<String, String> schemaMap = readonlyConfig.get(CatalogTableUtil.SCHEMA);
        if (schemaMap != null && schemaMap.size() > 0) {
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(config).getCatalogTable();
            return Collections.singletonList(catalogTable);
        }

        Optional<Catalog> optionalCatalog =
                FactoryUtil.createOptionalCatalog(
                        catalogConfig.get(CatalogOptions.NAME),
                        catalogConfig,
                        classLoader,
                        factoryId);
        return optionalCatalog
                .map(
                        c -> {
                            long startTime = System.currentTimeMillis();
                            try (Catalog catalog = c) {
                                catalog.open();
                                List<CatalogTable> catalogTables = catalog.getTables(catalogConfig);
                                log.info(
                                        String.format(
                                                "Get catalog tables, cost time: %d",
                                                System.currentTimeMillis() - startTime));
                                return catalogTables;
                            }
                        })
                .orElse(Collections.emptyList());
    }

    public static CatalogTableUtil buildWithConfig(Config config) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(config, "schema");
        if (!checkResult.isSuccess()) {
            throw new RuntimeException(
                    "Schema config need option [schema], please correct your config first");
        }
        TableSchema tableSchema = parseTableSchema(config.getConfig("schema"));
        return new CatalogTableUtil(
                CatalogTable.of(
                        // TODO: other table info
                        TableIdentifier.of("", "", ""),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        ""));
    }

    public static SeaTunnelRowType buildSimpleTextSchema() {
        return SIMPLE_SCHEMA;
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    public static SeaTunnelDataType<?> parseDataType(String columnStr) {
        SqlType sqlType = null;
        try {
            sqlType = SqlType.valueOf(columnStr.toUpperCase().replace(" ", ""));
        } catch (IllegalArgumentException e) {
            // nothing
        }
        if (sqlType == null) {
            return parseComplexDataType(columnStr);
        }
        switch (sqlType) {
            case STRING:
                return BasicType.STRING_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case BYTES:
                return PrimitiveByteArrayType.INSTANCE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case NULL:
                return BasicType.VOID_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw new UnsupportedOperationException(
                        String.format("the type[%s] is not support", columnStr));
        }
    }

    private static SeaTunnelDataType<?> parseComplexDataType(String columnStr) {
        String column = columnStr.toUpperCase().replace(" ", "");
        if (column.startsWith(SqlType.MAP.name())) {
            return parseMapType(column);
        }
        if (column.startsWith(SqlType.ARRAY.name())) {
            return parseArrayType(column);
        }
        if (column.startsWith(SqlType.DECIMAL.name())) {
            return parseDecimalType(column);
        }
        return parseRowType(columnStr);
    }

    private static SeaTunnelDataType<?> parseRowType(String columnStr) {
        Map<String, String> fieldsMap = convertJsonToMap(columnStr);
        String[] fieldsName = new String[fieldsMap.size()];
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[fieldsMap.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
            fieldsName[i] = entry.getKey();
            seaTunnelDataTypes[i] = parseDataType(entry.getValue());
            i++;
        }
        return new SeaTunnelRowType(fieldsName, seaTunnelDataTypes);
    }

    private static SeaTunnelDataType<?> parseMapType(String columnStr) {
        String genericType = getGenericType(columnStr);
        int index =
                genericType.startsWith(SqlType.DECIMAL.name())
                        ?
                        // if map key is decimal, we should find the index of second ','
                        genericType.indexOf(",", genericType.indexOf(",") + 1)
                        :
                        // if map key is not decimal, we should find the index of first ','
                        genericType.indexOf(",");
        String keyGenericType = genericType.substring(0, index);
        String valueGenericType = genericType.substring(index + 1);
        return new MapType<>(parseDataType(keyGenericType), parseDataType(valueGenericType));
    }

    private static String getGenericType(String columnStr) {
        // get the content between '<' and '>'
        return columnStr.substring(columnStr.indexOf("<") + 1, columnStr.lastIndexOf(">"));
    }

    private static SeaTunnelDataType<?> parseArrayType(String columnStr) {
        String genericType = getGenericType(columnStr);
        SeaTunnelDataType<?> dataType = parseDataType(genericType);
        switch (dataType.getSqlType()) {
            case STRING:
                return ArrayType.STRING_ARRAY_TYPE;
            case BOOLEAN:
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            case TINYINT:
                return ArrayType.BYTE_ARRAY_TYPE;
            case SMALLINT:
                return ArrayType.SHORT_ARRAY_TYPE;
            case INT:
                return ArrayType.INT_ARRAY_TYPE;
            case BIGINT:
                return ArrayType.LONG_ARRAY_TYPE;
            case FLOAT:
                return ArrayType.FLOAT_ARRAY_TYPE;
            case DOUBLE:
                return ArrayType.DOUBLE_ARRAY_TYPE;
            default:
                String errorMsg =
                        String.format("Array type not support this genericType [%s]", genericType);
                throw new UnsupportedOperationException(errorMsg);
        }
    }

    private static SeaTunnelDataType<?> parseDecimalType(String columnStr) {
        String[] decimalInfos = columnStr.split(",");
        if (decimalInfos.length < 2) {
            throw new RuntimeException(
                    "Decimal type should assign precision and scale information");
        }
        int precision = Integer.parseInt(decimalInfos[0].replaceAll("\\D", ""));
        int scale = Integer.parseInt(decimalInfos[1].replaceAll("\\D", ""));
        return new DecimalType(precision, scale);
    }

    private static Map<String, String> convertConfigToMap(Config config) {
        // Because the entrySet in typesafe config couldn't keep key-value order
        // So use jackson parsing schema information into a map to keep key-value order
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        String schema = config.root().render(options);
        return convertJsonToMap(schema);
    }

    private static Map<String, String> convertJsonToMap(String json) {
        ObjectNode jsonNodes = JsonUtils.parseObject(json);
        LinkedHashMap<String, String> fieldsMap = new LinkedHashMap<>();
        jsonNodes
                .fields()
                .forEachRemaining(
                        field -> {
                            String key = field.getKey();
                            JsonNode value = field.getValue();
                            if (value.getNodeType() == JsonNodeType.OBJECT) {
                                fieldsMap.put(key, value.toString());
                            } else {
                                fieldsMap.put(key, value.textValue());
                            }
                        });
        return fieldsMap;
    }

    private static TableSchema parseTableSchema(Config config) {
        Map<String, String> fieldsMap = convertConfigToMap(config.getConfig(FIELD_KEY));
        int fieldsNum = fieldsMap.size();
        List<Column> columns = new ArrayList<>(fieldsNum);
        for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            SeaTunnelDataType<?> dataType = parseDataType(value);
            // TODO: column
            PhysicalColumn column = PhysicalColumn.of(key, dataType, 0, true, null, null);
            columns.add(column);
        }
        return TableSchema.builder().columns(columns).build();
    }
}
