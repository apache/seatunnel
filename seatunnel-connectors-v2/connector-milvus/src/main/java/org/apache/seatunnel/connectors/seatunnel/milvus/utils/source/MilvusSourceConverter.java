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

package org.apache.seatunnel.connectors.seatunnel.milvus.utils.source;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.response.QueryResultsWrapper;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

public class MilvusSourceConverter {
    private final List<String> existField;
    private Gson gson = new Gson();

    public MilvusSourceConverter(TableSchema tableSchema) {
        this.existField =
                tableSchema.getColumns().stream()
                        .filter(
                                column ->
                                        column.getOptions() == null
                                                || !column.getOptions()
                                                        .containsValue(CommonOptions.METADATA))
                        .map(Column::getName)
                        .collect(Collectors.toList());
    }

    public SeaTunnelRow convertToSeaTunnelRow(
            QueryResultsWrapper.RowRecord record, TableSchema tableSchema, TablePath tablePath) {
        // get field names and types
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        String[] fieldNames = typeInfo.getFieldNames();

        Object[] seatunnelField = new Object[typeInfo.getTotalFields()];
        // get field values from source milvus
        Map<String, Object> fieldValuesMap = record.getFieldValues();
        // filter dynamic field
        JsonObject dynamicField = convertDynamicField(fieldValuesMap);

        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            if (fieldNames[fieldIndex].equals(CommonOptions.METADATA.getName())) {
                seatunnelField[fieldIndex] = dynamicField.toString();
                continue;
            }
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            Object filedValues = fieldValuesMap.get(fieldNames[fieldIndex]);
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    seatunnelField[fieldIndex] = filedValues.toString();
                    break;
                case BOOLEAN:
                    if (filedValues instanceof Boolean) {
                        seatunnelField[fieldIndex] = filedValues;
                    } else {
                        seatunnelField[fieldIndex] = Boolean.valueOf(filedValues.toString());
                    }
                    break;
                case TINYINT:
                    if (filedValues instanceof Byte) {
                        seatunnelField[fieldIndex] = filedValues;
                    } else {
                        seatunnelField[fieldIndex] = Byte.parseByte(filedValues.toString());
                    }
                    break;
                case SMALLINT:
                    if (filedValues instanceof Short) {
                        seatunnelField[fieldIndex] = filedValues;
                    } else {
                        seatunnelField[fieldIndex] = Short.parseShort(filedValues.toString());
                    }
                case INT:
                    if (filedValues instanceof Integer) {
                        seatunnelField[fieldIndex] = filedValues;
                    } else {
                        seatunnelField[fieldIndex] = Integer.valueOf(filedValues.toString());
                    }
                    break;
                case BIGINT:
                    if (filedValues instanceof Long) {
                        seatunnelField[fieldIndex] = filedValues;
                    } else {
                        seatunnelField[fieldIndex] = Long.parseLong(filedValues.toString());
                    }
                    break;
                case FLOAT:
                    if (filedValues instanceof Float) {
                        seatunnelField[fieldIndex] = filedValues;
                    } else {
                        seatunnelField[fieldIndex] = Float.parseFloat(filedValues.toString());
                    }
                    break;
                case DOUBLE:
                    if (filedValues instanceof Double) {
                        seatunnelField[fieldIndex] = filedValues;
                    } else {
                        seatunnelField[fieldIndex] = Double.parseDouble(filedValues.toString());
                    }
                    break;
                case ARRAY:
                    if (filedValues instanceof List) {
                        List<?> list = (List<?>) filedValues;
                        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelDataType;
                        SqlType elementType = arrayType.getElementType().getSqlType();
                        switch (elementType) {
                            case STRING:
                                String[] arrays = new String[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    arrays[i] = list.get(i).toString();
                                }
                                seatunnelField[fieldIndex] = arrays;
                                break;
                            case BOOLEAN:
                                Boolean[] booleanArrays = new Boolean[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    booleanArrays[i] = Boolean.valueOf(list.get(i).toString());
                                }
                                seatunnelField[fieldIndex] = booleanArrays;
                                break;
                            case TINYINT:
                                Byte[] byteArrays = new Byte[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    byteArrays[i] = Byte.parseByte(list.get(i).toString());
                                }
                                seatunnelField[fieldIndex] = byteArrays;
                                break;
                            case SMALLINT:
                                Short[] shortArrays = new Short[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    shortArrays[i] = Short.parseShort(list.get(i).toString());
                                }
                                seatunnelField[fieldIndex] = shortArrays;
                                break;
                            case INT:
                                Integer[] intArrays = new Integer[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    intArrays[i] = Integer.valueOf(list.get(i).toString());
                                }
                                seatunnelField[fieldIndex] = intArrays;
                                break;
                            case BIGINT:
                                Long[] longArrays = new Long[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    longArrays[i] = Long.parseLong(list.get(i).toString());
                                }
                                seatunnelField[fieldIndex] = longArrays;
                                break;
                            case FLOAT:
                                Float[] floatArrays = new Float[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    floatArrays[i] = Float.parseFloat(list.get(i).toString());
                                }
                                seatunnelField[fieldIndex] = floatArrays;
                                break;
                            case DOUBLE:
                                Double[] doubleArrays = new Double[list.size()];
                                for (int i = 0; i < list.size(); i++) {
                                    doubleArrays[i] = Double.parseDouble(list.get(i).toString());
                                }
                                seatunnelField[fieldIndex] = doubleArrays;
                                break;
                            default:
                                throw new MilvusConnectorException(
                                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                        "Unexpected array value: " + filedValues);
                        }
                    } else {
                        throw new MilvusConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unexpected array value: " + filedValues);
                    }
                    break;
                case FLOAT_VECTOR:
                    if (filedValues instanceof List) {
                        List list = (List) filedValues;
                        Float[] arrays = new Float[list.size()];
                        for (int i = 0; i < list.size(); i++) {
                            arrays[i] = Float.parseFloat(list.get(i).toString());
                        }
                        seatunnelField[fieldIndex] = BufferUtils.toByteBuffer(arrays);
                        break;
                    } else {
                        throw new MilvusConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unexpected vector value: " + filedValues);
                    }
                case BINARY_VECTOR:
                case FLOAT16_VECTOR:
                case BFLOAT16_VECTOR:
                    if (filedValues instanceof ByteBuffer) {
                        seatunnelField[fieldIndex] = filedValues;
                        break;
                    } else {
                        throw new MilvusConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unexpected vector value: " + filedValues);
                    }
                case SPARSE_FLOAT_VECTOR:
                    if (filedValues instanceof Map) {
                        seatunnelField[fieldIndex] = filedValues;
                        break;
                    } else {
                        throw new MilvusConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unexpected vector value: " + filedValues);
                    }
                default:
                    throw new MilvusConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType.getSqlType().name());
            }
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(seatunnelField);
        seaTunnelRow.setTableId(tablePath.getFullName());
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }

    public static PhysicalColumn convertColumn(FieldSchema fieldSchema) {
        DataType dataType = fieldSchema.getDataType();
        PhysicalColumn.PhysicalColumnBuilder builder = PhysicalColumn.builder();
        builder.name(fieldSchema.getName());
        builder.sourceType(dataType.name());
        builder.comment(fieldSchema.getDescription());

        switch (dataType) {
            case Bool:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case Int8:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case Int16:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case Int32:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case Int64:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case Float:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case Double:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case VarChar:
                builder.dataType(BasicType.STRING_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("max_length")) {
                        builder.columnLength(Long.parseLong(keyValuePair.getValue()) * 4);
                        break;
                    }
                }
                break;
            case String:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case JSON:
                builder.dataType(STRING_TYPE);
                Map<String, Object> options = new HashMap<>();
                options.put(CommonOptions.JSON.getName(), true);
                builder.options(options);
                break;
            case Array:
                builder.dataType(ArrayType.STRING_ARRAY_TYPE);
                break;
            case FloatVector:
                builder.dataType(VectorType.VECTOR_FLOAT_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("dim")) {
                        builder.scale(Integer.valueOf(keyValuePair.getValue()));
                        break;
                    }
                }
                break;
            case BinaryVector:
                builder.dataType(VectorType.VECTOR_BINARY_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("dim")) {
                        builder.scale(Integer.valueOf(keyValuePair.getValue()));
                        break;
                    }
                }
                break;
            case SparseFloatVector:
                builder.dataType(VectorType.VECTOR_SPARSE_FLOAT_TYPE);
                break;
            case Float16Vector:
                builder.dataType(VectorType.VECTOR_FLOAT16_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("dim")) {
                        builder.scale(Integer.valueOf(keyValuePair.getValue()));
                        break;
                    }
                }
                break;
            case BFloat16Vector:
                builder.dataType(VectorType.VECTOR_BFLOAT16_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("dim")) {
                        builder.scale(Integer.valueOf(keyValuePair.getValue()));
                        break;
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }

        return builder.build();
    }

    private JsonObject convertDynamicField(Map<String, Object> fieldValuesMap) {
        JsonObject dynamicField = new JsonObject();
        for (Map.Entry<String, Object> entry : fieldValuesMap.entrySet()) {
            if (!existField.contains(entry.getKey())) {
                dynamicField.add(entry.getKey(), gson.toJsonTree(entry.getValue()));
            }
        }
        return dynamicField;
    }
}
