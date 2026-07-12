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

package org.apache.seatunnel.connectors.seatunnel.lance.data;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;

import com.google.auto.service.AutoService;
import com.lancedb.lance.namespace.model.JsonArrowDataType;
import com.lancedb.lance.namespace.model.JsonArrowField;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class LanceTypeMapper {

    public static final LanceTypeMapper INSTANCE = new LanceTypeMapper();

    public SeaTunnelDataType<?> convertDataType(String field, @NonNull JsonArrowDataType type) {

        switch (type.getType().toLowerCase()) {
            case "bool":
                return BasicType.BOOLEAN_TYPE;
            case "int":
            case "int8":
            case "int16":
            case "int32":
            case "int64":
            case "uint8":
            case "uint16":
            case "uint32":
            case "uint64":
                return BasicType.INT_TYPE;
            case "utf8":
            case "largeutf8":
            case "string":
                return BasicType.STRING_TYPE;
            case "decimal":
                return new DecimalType(8, 4);
            case "floatingpoint":
            case "float32":
                return BasicType.FLOAT_TYPE;
            case "float64":
                return BasicType.DOUBLE_TYPE;
            case "date":
            case "date32":
            case "date64":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "time":
            case "time32":
            case "time64":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "timestamp":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "binary":
                return BasicType.BYTE_TYPE;
            case "decimal128":
                return new DecimalType(38, 10);
                // TODO: struct|list|map
            default:
                throw CommonError.convertToSeaTunnelTypeError("Lance", type.getType(), field);
        }
    }

    public JsonArrowDataType convertJsonArrowType(
            String field, @NonNull SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case INT:
                JsonArrowDataType intType = new JsonArrowDataType();
                intType.setType("int32");
                return intType;
            case STRING:
                JsonArrowDataType stringType = new JsonArrowDataType();
                stringType.setType("utf8");
                return stringType;
            case MAP:
                JsonArrowDataType mapType = new JsonArrowDataType();
                mapType.setType("map");
                if (type instanceof MapType) {
                    MapType<?, ?> mapTypeInfo = (MapType<?, ?>) type;
                    JsonArrowField keyField = new JsonArrowField();
                    keyField.setName("key");
                    keyField.setType(convertJsonArrowType("key", mapTypeInfo.getKeyType()));
                    keyField.setNullable(false);

                    JsonArrowField valueField = new JsonArrowField();
                    valueField.setName("value");
                    valueField.setType(convertJsonArrowType("value", mapTypeInfo.getValueType()));
                    valueField.setNullable(true);

                    JsonArrowDataType structType = new JsonArrowDataType();
                    structType.setType("struct");
                    structType.setFields(Lists.newArrayList(keyField, valueField));

                    JsonArrowField entriesField = new JsonArrowField();
                    entriesField.setName("entries");
                    entriesField.setType(structType);
                    entriesField.setNullable(false);

                    mapType.setFields(Lists.newArrayList(entriesField));
                }
                return mapType;
            case ARRAY:
                JsonArrowDataType listType = new JsonArrowDataType();
                listType.setType("list");
                if (type instanceof ArrayType) {
                    ArrayType<?, ?> arrayType = (ArrayType<?, ?>) type;
                    JsonArrowField elementField = new JsonArrowField();
                    elementField.setName("element");
                    elementField.setType(
                            convertJsonArrowType("element", arrayType.getElementType()));
                    elementField.setNullable(true);
                    listType.setFields(Lists.newArrayList(elementField));
                }
                return listType;
            case BOOLEAN:
                JsonArrowDataType booleanType = new JsonArrowDataType();
                booleanType.setType("bool");
                return booleanType;
            case FLOAT:
                JsonArrowDataType floatType = new JsonArrowDataType();
                floatType.setType("float32");
                return floatType;
            case DOUBLE:
                JsonArrowDataType doubleType = new JsonArrowDataType();
                doubleType.setType("float64");
                return doubleType;
            case DECIMAL:
                JsonArrowDataType decType = new JsonArrowDataType();
                decType.setType("decimal128");
                return decType;
            case NULL:
                JsonArrowDataType nullType = new JsonArrowDataType();
                nullType.setType("null");
                return nullType;
            case BYTES:
                JsonArrowDataType bytesType = new JsonArrowDataType();
                bytesType.setType("binary");
                return bytesType;
            case DATE:
                JsonArrowDataType dateType = new JsonArrowDataType();
                dateType.setType("date32");
                return dateType;
            case TIME:
                JsonArrowDataType timeType = new JsonArrowDataType();
                timeType.setType("time32");
                return timeType;
            case TIMESTAMP:
                JsonArrowDataType timestampType = new JsonArrowDataType();
                timestampType.setType("timestamp");
                return timestampType;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        "Lance", type.getSqlType().name(), field);
        }
    }
}
