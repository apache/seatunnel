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

package org.apache.seatunnel.connectors.seatunnel.milvus.utils.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.milvus.common.utils.JacksonUtils;
import io.milvus.grpc.DataType;
import io.milvus.param.collection.FieldType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.ENABLE_AUTO_ID;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig.ENABLE_DYNAMIC_FIELD;

public class MilvusSinkConverter {
    private static final Gson gson = new Gson();

    public Object convertBySeaTunnelType(
            SeaTunnelDataType<?> fieldType, Boolean isJson, Object value) {
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case INT:
                return Integer.parseInt(value.toString());
            case TINYINT:
                return Byte.parseByte(value.toString());
            case BIGINT:
                return Long.parseLong(value.toString());
            case SMALLINT:
                return Short.parseShort(value.toString());
            case STRING:
            case DATE:
                if (isJson) {
                    return gson.fromJson(value.toString(), JsonObject.class);
                }
                return value.toString();
            case FLOAT_VECTOR:
                ByteBuffer floatVectorBuffer = (ByteBuffer) value;
                Float[] floats = BufferUtils.toFloatArray(floatVectorBuffer);
                return Arrays.stream(floats).collect(Collectors.toList());
            case BINARY_VECTOR:
            case BFLOAT16_VECTOR:
            case FLOAT16_VECTOR:
                ByteBuffer binaryVector = (ByteBuffer) value;
                return gson.toJsonTree(binaryVector.array());
            case SPARSE_FLOAT_VECTOR:
                return JsonParser.parseString(JacksonUtils.toJsonString(value)).getAsJsonObject();
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
                    case SMALLINT:
                        Short[] shortArray = (Short[]) value;
                        return Arrays.asList(shortArray);
                    case TINYINT:
                        Byte[] byteArray = (Byte[]) value;
                        return Arrays.asList(byteArray);
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
                return JacksonUtils.toJsonString(value);
            default:
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.NOT_SUPPORT_TYPE, sqlType.name());
        }
    }

    public static FieldType convertToFieldType(
            Column column, PrimaryKey primaryKey, String partitionKeyField, Boolean autoId) {
        SeaTunnelDataType<?> seaTunnelDataType = column.getDataType();
        DataType milvusDataType = convertSqlTypeToDataType(seaTunnelDataType.getSqlType());
        FieldType.Builder build =
                FieldType.newBuilder().withName(column.getName()).withDataType(milvusDataType);
        if (StringUtils.isNotEmpty(column.getComment())) {
            build.withDescription(column.getComment());
        }
        switch (seaTunnelDataType.getSqlType()) {
            case ROW:
                build.withMaxLength(65535);
                break;
            case DATE:
                build.withMaxLength(20);
                break;
            case STRING:
                if (column.getOptions() != null
                        && column.getOptions().get(CommonOptions.JSON.getName()) != null
                        && (Boolean) column.getOptions().get(CommonOptions.JSON.getName())) {
                    // check if is json
                    build.withDataType(DataType.JSON);
                } else if (column.getColumnLength() == null || column.getColumnLength() == 0) {
                    build.withMaxLength(65535);
                } else {
                    build.withMaxLength((int) (column.getColumnLength() / 4));
                }
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) column.getDataType();
                SeaTunnelDataType elementType = arrayType.getElementType();
                build.withElementType(convertSqlTypeToDataType(elementType.getSqlType()));
                build.withMaxCapacity(4095);
                switch (elementType.getSqlType()) {
                    case STRING:
                        if (column.getColumnLength() == null || column.getColumnLength() == 0) {
                            build.withMaxLength(65535);
                        } else {
                            build.withMaxLength((int) (column.getColumnLength() / 4));
                        }
                        break;
                }
                break;
            case BINARY_VECTOR:
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
                build.withDimension(column.getScale());
                break;
        }

        // check is primaryKey
        if (null != primaryKey && primaryKey.getColumnNames().contains(column.getName())) {
            build.withPrimaryKey(true);
            List<SqlType> integerTypes = new ArrayList<>();
            integerTypes.add(SqlType.INT);
            integerTypes.add(SqlType.SMALLINT);
            integerTypes.add(SqlType.TINYINT);
            integerTypes.add(SqlType.BIGINT);
            if (integerTypes.contains(seaTunnelDataType.getSqlType())) {
                build.withDataType(DataType.Int64);
            } else {
                build.withDataType(DataType.VarChar);
                build.withMaxLength(65535);
            }
            if (null != primaryKey.getEnableAutoId()) {
                build.withAutoID(primaryKey.getEnableAutoId());
            } else {
                build.withAutoID(autoId);
            }
        }

        // check is partitionKey
        if (column.getName().equals(partitionKeyField)) {
            build.withPartitionKey(true);
        }

        return build.build();
    }

    public static DataType convertSqlTypeToDataType(SqlType sqlType) {
        switch (sqlType) {
            case BOOLEAN:
                return DataType.Bool;
            case TINYINT:
                return DataType.Int8;
            case SMALLINT:
                return DataType.Int16;
            case INT:
                return DataType.Int32;
            case BIGINT:
                return DataType.Int64;
            case FLOAT:
                return DataType.Float;
            case DOUBLE:
                return DataType.Double;
            case STRING:
                return DataType.VarChar;
            case ARRAY:
                return DataType.Array;
            case MAP:
                return DataType.JSON;
            case FLOAT_VECTOR:
                return DataType.FloatVector;
            case BINARY_VECTOR:
                return DataType.BinaryVector;
            case FLOAT16_VECTOR:
                return DataType.Float16Vector;
            case BFLOAT16_VECTOR:
                return DataType.BFloat16Vector;
            case SPARSE_FLOAT_VECTOR:
                return DataType.SparseFloatVector;
            case DATE:
                return DataType.VarChar;
            case ROW:
                return DataType.VarChar;
        }
        throw new CatalogException(
                String.format("Not support convert to milvus type, sqlType is %s", sqlType));
    }

    public JsonObject buildMilvusData(
            CatalogTable catalogTable,
            ReadonlyConfig config,
            List<String> jsonFields,
            String dynamicField,
            SeaTunnelRow element) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        Boolean autoId = config.get(ENABLE_AUTO_ID);

        JsonObject data = new JsonObject();
        Gson gson = new Gson();
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            String fieldName = seaTunnelRowType.getFieldNames()[i];
            Boolean isJson = jsonFields.contains(fieldName);
            if (autoId && isPrimaryKeyField(primaryKey, fieldName)) {
                continue; // if create table open AutoId, then don't need insert data with
                // primaryKey field.
            }

            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            Object value = element.getField(i);
            if (null == value) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.FIELD_IS_NULL, fieldName);
            }
            // if the field is dynamic field, then parse the dynamic field
            if (dynamicField != null
                    && dynamicField.equals(fieldName)
                    && config.get(ENABLE_DYNAMIC_FIELD)) {
                JsonObject dynamicData = gson.fromJson(value.toString(), JsonObject.class);
                dynamicData
                        .entrySet()
                        .forEach(
                                entry -> {
                                    data.add(entry.getKey(), entry.getValue());
                                });
                continue;
            }
            Object object = convertBySeaTunnelType(fieldType, isJson, value);
            data.add(fieldName, gson.toJsonTree(object));
        }
        return data;
    }
}
