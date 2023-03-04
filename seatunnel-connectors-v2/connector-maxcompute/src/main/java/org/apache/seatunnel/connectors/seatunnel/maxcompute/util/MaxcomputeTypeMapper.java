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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;

import com.aliyun.odps.Column;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.SimpleArrayTypeInfo;
import com.aliyun.odps.type.SimpleMapTypeInfo;
import com.aliyun.odps.type.SimpleStructTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class MaxcomputeTypeMapper implements Serializable {

    public static SeaTunnelRow getSeaTunnelRowData(Record rs, SeaTunnelRowType typeInfo) {
        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();
        for (int i = 0; i < rs.getColumns().length; i++) {
            fields.add(resolveObject2SeaTunnel(rs.get(i), seaTunnelDataTypes[i]));
        }
        return new SeaTunnelRow(fields.toArray());
    }

    public static Record getMaxcomputeRowData(SeaTunnelRow seaTunnelRow, TableSchema tableSchema) {
        ArrayRecord arrayRecord = new ArrayRecord(tableSchema);
        List<Column> columns = tableSchema.getColumns();
        for (int i = 0; i < seaTunnelRow.getFields().length; i++) {
            arrayRecord.set(
                    i,
                    resolveObject2Maxcompute(
                            seaTunnelRow.getField(i), columns.get(i).getTypeInfo()));
        }
        return arrayRecord;
    }

    public static SeaTunnelRowType getSeaTunnelRowType(Config pluginConfig) {
        Table table = MaxcomputeUtil.getTable(pluginConfig);
        TableSchema tableSchema = table.getSchema();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            for (int i = 0; i < tableSchema.getColumns().size(); i++) {
                fieldNames.add(tableSchema.getColumns().get(i).getName());
                TypeInfo maxcomputeTypeInfo = tableSchema.getColumns().get(i).getTypeInfo();
                SeaTunnelDataType<?> seaTunnelDataType =
                        maxcompute2SeaTunnelType(maxcomputeTypeInfo);
                seaTunnelDataTypes.add(seaTunnelDataType);
            }
        } catch (Exception e) {
            throw new MaxcomputeConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED, e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[fieldNames.size()]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }

    private static SeaTunnelDataType<?> maxcompute2SeaTunnelType(TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return new MapType(
                        maxcompute2SeaTunnelType(mapTypeInfo.getKeyTypeInfo()),
                        maxcompute2SeaTunnelType(mapTypeInfo.getValueTypeInfo()));
            case ARRAY:
                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
                switch (arrayTypeInfo.getElementTypeInfo().getOdpsType()) {
                    case BOOLEAN:
                        return ArrayType.BOOLEAN_ARRAY_TYPE;
                    case INT:
                        return ArrayType.INT_ARRAY_TYPE;
                    case BIGINT:
                        return ArrayType.LONG_ARRAY_TYPE;
                    case FLOAT:
                        return ArrayType.FLOAT_ARRAY_TYPE;
                    case DOUBLE:
                        return ArrayType.DOUBLE_ARRAY_TYPE;
                    case STRING:
                        return ArrayType.STRING_ARRAY_TYPE;
                    default:
                        throw new MaxcomputeConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                String.format(
                                        "SeaTunnel type not support this type [%s] now",
                                        typeInfo.getTypeName()));
                }
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<TypeInfo> fields = structTypeInfo.getFieldTypeInfos();
                List<String> fieldNames = new ArrayList<>(fields.size());
                List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>(fields.size());
                for (TypeInfo field : fields) {
                    fieldNames.add(field.getTypeName());
                    fieldTypes.add(maxcompute2SeaTunnelType(field));
                }
                return new SeaTunnelRowType(
                        fieldNames.toArray(new String[0]),
                        fieldTypes.toArray(new SeaTunnelDataType[0]));
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                return new DecimalType(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
            case VARCHAR:
            case CHAR:
            case STRING:
                return BasicType.STRING_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DATETIME:
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case VOID:
                return BasicType.VOID_TYPE;
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            default:
                throw new MaxcomputeConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel type not support this type [%s] now",
                                typeInfo.getTypeName()));
        }
    }

    private static Object resolveObject2SeaTunnel(Object field, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                ArrayList<Object> origArray = new ArrayList<>();
                ((ArrayList) field).iterator().forEachRemaining(origArray::add);
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                switch (elementType.getSqlType()) {
                    case STRING:
                        return origArray.toArray(new String[0]);
                    case BOOLEAN:
                        return origArray.toArray(new Boolean[0]);
                    case INT:
                        return origArray.toArray(new Integer[0]);
                    case BIGINT:
                        return origArray.toArray(new Long[0]);
                    case FLOAT:
                        return origArray.toArray(new Float[0]);
                    case DOUBLE:
                        return origArray.toArray(new Double[0]);
                    default:
                        throw new MaxcomputeConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                String.format(
                                        "SeaTunnel type not support this type [%s] now",
                                        fieldType.getSqlType().name()));
                }
            case MAP:
                HashMap<Object, Object> dataMap = new HashMap<>();
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                HashMap<Object, Object> origDataMap = (HashMap<Object, Object>) field;
                origDataMap.forEach(
                        (key, value) ->
                                dataMap.put(
                                        resolveObject2SeaTunnel(key, keyType),
                                        resolveObject2SeaTunnel(value, valueType)));
                return dataMap;
            case ROW:
                SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) fieldType).getFieldTypes();
                Object[] objects = new Object[fieldTypes.length];
                List<Object> fieldValues = ((SimpleStruct) field).getFieldValues();
                for (int i = 0; i < fieldTypes.length; i++) {
                    Object object = resolveObject2SeaTunnel(fieldValues.get(i), fieldTypes[i]);
                    objects[i] = object;
                }
                return new SeaTunnelRow(objects);
            case TINYINT:
            case SMALLINT:
            case INT:
            case FLOAT:
            case DOUBLE:
            case BIGINT:
            case BOOLEAN:
                return field;
            case BYTES:
                return ((Binary) field).data();
            case DECIMAL:
                return null;
            case STRING:
                if (field instanceof byte[]) {
                    return new String((byte[]) field);
                }
                if (field instanceof Char) {
                    return rtrim(String.valueOf(field));
                }
                return String.valueOf(field);
            case DATE:
                if (field instanceof LocalDate) {
                    return Date.valueOf((LocalDate) field);
                }
                return ((Date) field).toLocalDate();
            case TIME:
                return ((Time) field).toLocalTime();
            case TIMESTAMP:
                return ((java.util.Date) field)
                        .toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
            case NULL:
            default:
                throw new MaxcomputeConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel type not support this type [%s] now",
                                fieldType.getSqlType().name()));
        }
    }

    private static Object resolveObject2Maxcompute(Object field, TypeInfo typeInfo) {
        if (field == null) {
            return null;
        }
        switch (typeInfo.getOdpsType()) {
            case ARRAY:
                ArrayList<Object> origArray = new ArrayList<>();
                Arrays.stream((Object[]) field).iterator().forEachRemaining(origArray::add);
                switch (((SimpleArrayTypeInfo) typeInfo).getElementTypeInfo().getOdpsType()) {
                    case STRING:
                    case BOOLEAN:
                    case INT:
                    case BIGINT:
                    case FLOAT:
                    case DOUBLE:
                        return origArray;
                    default:
                        throw new MaxcomputeConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                String.format(
                                        "Maxcompute type not support this type [%s] now",
                                        typeInfo.getTypeName()));
                }
            case MAP:
                HashMap<Object, Object> dataMap = new HashMap<>();
                TypeInfo keyTypeInfo = ((SimpleMapTypeInfo) typeInfo).getKeyTypeInfo();
                TypeInfo valueTypeInfo = ((SimpleMapTypeInfo) typeInfo).getValueTypeInfo();
                HashMap<Object, Object> origDataMap = (HashMap<Object, Object>) field;
                origDataMap.forEach(
                        (key, value) ->
                                dataMap.put(
                                        resolveObject2Maxcompute(key, keyTypeInfo),
                                        resolveObject2Maxcompute(value, valueTypeInfo)));
                return origDataMap;
            case STRUCT:
                Object[] fields = ((SeaTunnelRow) field).getFields();
                List<TypeInfo> typeInfos = ((SimpleStructTypeInfo) typeInfo).getFieldTypeInfos();
                ArrayList<Object> origStruct = new ArrayList<>();
                for (int i = 0; i < fields.length; i++) {
                    origStruct.add(resolveObject2Maxcompute(fields[i], typeInfos.get(i)));
                }
                return new SimpleStruct((StructTypeInfo) typeInfo, origStruct);
            case TINYINT:
            case SMALLINT:
            case INT:
            case FLOAT:
            case DOUBLE:
            case BIGINT:
            case BOOLEAN:
                return field;
            case BINARY:
                return new Binary((byte[]) field);
            case DECIMAL:
                return null;
            case VARCHAR:
                return new Varchar((String) field);
            case CHAR:
                return new Char((String) field);
            case STRING:
                if (field instanceof byte[]) {
                    return new String((byte[]) field);
                }
                if (field instanceof Char) {
                    return rtrim(String.valueOf(field));
                }
                return String.valueOf(field);
            case TIMESTAMP:
                return Timestamp.valueOf((LocalDateTime) field);
            case DATETIME:
                return Date.from(
                        ((LocalDateTime) field).atZone(ZoneId.systemDefault()).toInstant());
            case DATE:
                return Date.valueOf((LocalDate) field);
            default:
                throw new MaxcomputeConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "Maxcompute type not support this type [%s] now",
                                typeInfo.getTypeName()));
        }
    }

    private static String rtrim(String s) {
        int i = s.length() - 1;
        while (i >= 0 && Character.isWhitespace(s.charAt(i))) {
            i--;
        }
        return s.substring(0, i + 1);
    }
}
