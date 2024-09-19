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

package org.apache.seatunnel.connectors.seatunnel.weaviate.convert;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateParameters;
import org.apache.seatunnel.connectors.seatunnel.weaviate.exception.WeaviateConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.weaviate.exception.WeaviateConnectorException;

import org.apache.commons.lang3.StringUtils;

import io.weaviate.client.Config;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.schema.model.DataType;
import io.weaviate.client.v1.schema.model.Property;
import io.weaviate.client.v1.schema.model.WeaviateClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WeaviateConverter {

    private static final String CATALOG_NAME = "Weaviate";

    public static Map<TablePath, CatalogTable> getSourceTables(WeaviateParameters parameters) {
        Config config =
                new Config(
                        parameters.getClassName(),
                        parameters.getUrl(),
                        parameters.getHeader(),
                        parameters.getConnectionTimeout(),
                        parameters.getReadTimeout(),
                        parameters.getWriteTimeout());
        WeaviateClient client = new WeaviateClient(config);

        List<WeaviateClass> classList = new ArrayList<>();

        List<WeaviateClass> classes = client.schema().getter().run().getResult().getClasses();
        if (StringUtils.isEmpty(parameters.getClassName())) {
            for (WeaviateClass weaviateClass : classes) {
                if (weaviateClass.getClassName().equals(parameters.getClassName())) {
                    classList.add(weaviateClass);
                }
            }
        } else {
            for (WeaviateClass weaviateClass : classes) {
                classList.add(weaviateClass);
            }
        }

        Map<TablePath, CatalogTable> map = new HashMap<>();
        for (WeaviateClass weaviateClass : classList) {
            CatalogTable catalogTable = getCatalogTable(weaviateClass);
            map.put(TablePath.of(weaviateClass.getClassName()), catalogTable);
        }
        return map;
    }

    public static CatalogTable getCatalogTable(WeaviateClass weaviateClass) {

        // build tableSchema
        List<Column> columns = new ArrayList<>();
        List<Property> properties = weaviateClass.getProperties();
        for (Property property : properties) {
            columns.add(WeaviateConverter.convertColumn(property));
        }
        TableSchema tableSchema = TableSchema.builder().columns(columns).build();

        // description
        String description = weaviateClass.getDescription();

        // build tableId
        TableIdentifier tableId =
                TableIdentifier.of(CATALOG_NAME, TablePath.of(weaviateClass.getClassName()));

        // build options info
        Map<String, String> options = new HashMap<>();

        return CatalogTable.of(tableId, tableSchema, options, new ArrayList<>(), description);
    }

    private static PhysicalColumn convertColumn(Property property) {
        // By default, one can be extended
        PhysicalColumn.PhysicalColumnBuilder builder = PhysicalColumn.builder();
        builder.name(property.getName());
        builder.sourceType(property.getDataType().get(0));
        builder.comment(property.getDescription());
        switch (property.getDataType().get(0)) {
            case DataType.TEXT:
            case DataType.PHONE_NUMBER:
            case DataType.UUID:
            case DataType.OBJECT:
            case DataType.GEO_COORDINATES:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DataType.INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DataType.NUMBER:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DataType.DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + property.getDataType().get(0));
        }

        return builder.build();
    }

    public static Object convertBySeaTunnelType(SeaTunnelDataType<?> fieldType, Object value) {
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case INT:
                return Integer.parseInt(value.toString());
            case BIGINT:
                return Long.parseLong(value.toString());
            case SMALLINT:
                return Short.parseShort(value.toString());
            case STRING:
            case DATE:
                return value.toString();
            case FLOAT:
                return Float.parseFloat(value.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(value.toString());
            case DOUBLE:
                return Double.parseDouble(value.toString());
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                switch (arrayType.getElementType().getSqlType()) {
                    case STRING:
                        String[] stringArray = (String[]) value;
                        return Arrays.asList(stringArray);
                    case INT:
                        Integer[] intArray = (Integer[]) value;
                        return Arrays.asList(intArray);
                    case BIGINT:
                        Long[] longArray = (Long[]) value;
                        return Arrays.asList(longArray);
                    case FLOAT:
                        Float[] floatArray = (Float[]) value;
                        return Arrays.asList(floatArray);
                    case DOUBLE:
                        Double[] doubleArray = (Double[]) value;
                        return Arrays.asList(doubleArray);
                }
            case ROW:
                SeaTunnelRow row = (SeaTunnelRow) value;
                return JsonUtils.toJsonString(row.getFields());
            case MAP:
                return JsonUtils.toJsonString(value);
            default:
                throw new WeaviateConnectorException(
                        WeaviateConnectionErrorCode.NOT_SUPPORT_TYPE, sqlType.name());
        }
    }

    public static String convertSqlTypeToDataType(SqlType sqlType) {
        switch (sqlType) {
            case BOOLEAN:
                return DataType.BOOLEAN;
            case INT:
                return DataType.INT;
            case SMALLINT:
            case TINYINT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return DataType.NUMBER;
            case STRING:
                return DataType.STRING;
            case DATE:
                return DataType.DATE;
            case ROW:
                return DataType.OBJECT;
        }
        throw new CatalogException(
                String.format("Not support convert to milvus type, sqlType is %s", sqlType));
    }
}
