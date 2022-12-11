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

import static com.aliyun.odps.OdpsType.ARRAY;
import static com.aliyun.odps.OdpsType.BIGINT;
import static com.aliyun.odps.OdpsType.BINARY;
import static com.aliyun.odps.OdpsType.BOOLEAN;
import static com.aliyun.odps.OdpsType.DATE;
import static com.aliyun.odps.OdpsType.DECIMAL;
import static com.aliyun.odps.OdpsType.DOUBLE;
import static com.aliyun.odps.OdpsType.FLOAT;
import static com.aliyun.odps.OdpsType.INT;
import static com.aliyun.odps.OdpsType.MAP;
import static com.aliyun.odps.OdpsType.SMALLINT;
import static com.aliyun.odps.OdpsType.STRING;
import static com.aliyun.odps.OdpsType.TIMESTAMP;
import static com.aliyun.odps.OdpsType.TINYINT;
import static com.aliyun.odps.OdpsType.VOID;

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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class MaxcomputeTypeMapper implements Serializable {

    private static SeaTunnelDataType<?> maxcomputeType2SeaTunnelType(TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BIGINT:
                return BasicType.LONG_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case DECIMAL:
                return mappingDecimalType((DecimalTypeInfo) typeInfo);
            case MAP:
                return mappingMapType((MapTypeInfo) typeInfo);
            case ARRAY:
                return mappingListType((ArrayTypeInfo) typeInfo);
            case VOID:
                return BasicType.VOID_TYPE;
            case TINYINT:
            case SMALLINT:
            case INT:
                return BasicType.INT_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case CHAR:
            case VARCHAR:
            case STRING:
                return BasicType.STRING_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIMESTAMP:
            case DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case STRUCT:
                return mappingStructType((StructTypeInfo) typeInfo);
            case INTERVAL_DAY_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case INTERVAL_YEAR_MONTH:
            default:
                throw new MaxcomputeConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format(
                    "Doesn't support Maxcompute type '%s' .",
                    typeInfo.getTypeName()));
        }
    }

    private static DecimalType mappingDecimalType(DecimalTypeInfo decimalTypeInfo) {
        return new DecimalType(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
    }

    private static MapType mappingMapType(MapTypeInfo mapTypeInfo) {
        return new MapType(maxcomputeType2SeaTunnelType(mapTypeInfo.getKeyTypeInfo()), maxcomputeType2SeaTunnelType(mapTypeInfo.getValueTypeInfo()));
    }

    private static ArrayType mappingListType(ArrayTypeInfo arrayTypeInfo) {
        switch (arrayTypeInfo.getOdpsType()) {
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
                throw new MaxcomputeConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format(
                    "Doesn't support Maxcompute type '%s' .",
                    arrayTypeInfo.getTypeName()));
        }
    }

    private static SeaTunnelRowType mappingStructType(StructTypeInfo structType) {
        List<TypeInfo> fields = structType.getFieldTypeInfos();
        List<String> fieldNames = new ArrayList<>(fields.size());
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>(fields.size());
        for (TypeInfo field : fields) {
            fieldNames.add(field.getTypeName());
            fieldTypes.add(maxcomputeType2SeaTunnelType(field));
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[0]),
            fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    private static OdpsType seaTunnelType2MaxcomputeType(SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case ARRAY:
                return ARRAY;
            case MAP:
                return MAP;
            case STRING:
                return STRING;
            case BOOLEAN:
                return BOOLEAN;
            case TINYINT:
                return TINYINT;
            case SMALLINT:
                return SMALLINT;
            case INT:
                return INT;
            case BIGINT:
                return BIGINT;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case DECIMAL:
                return DECIMAL;
            case BYTES:
                return BINARY;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case NULL:
                return VOID;
            case TIME:
            default:
                throw new MaxcomputeConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format(
                    "Doesn't support SeaTunnelDataType type '%s' .",
                    seaTunnelDataType.getSqlType()));
        }
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
                SeaTunnelDataType<?> seaTunnelDataType = maxcomputeType2SeaTunnelType(maxcomputeTypeInfo);
                seaTunnelDataTypes.add(seaTunnelDataType);
            }
        } catch (Exception e) {
            throw new MaxcomputeConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED, e);
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }

    public static TableSchema seaTunnelRowType2TableSchema(SeaTunnelRowType seaTunnelRowType) {
        TableSchema tableSchema = new TableSchema();
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            OdpsType odpsType = seaTunnelType2MaxcomputeType(seaTunnelRowType.getFieldType(i));
            Column column = new Column(seaTunnelRowType.getFieldName(i), odpsType);
            tableSchema.addColumn(column);
        }
        return tableSchema;
    }

    private static Object resolveObject(Object field, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                ArrayList<Object> origArray = new ArrayList<>();
                java.util.Arrays.stream(((Record) field).getColumns()).iterator().forEachRemaining(origArray::add);
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
                        String errorMsg = String.format("SeaTunnel array type not support this type [%s] now", fieldType.getSqlType());
                        throw new MaxcomputeConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "SeaTunnel not support this data type now");
                }
            case MAP:
                HashMap<Object, Object> dataMap = new HashMap<>();
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                HashMap<Object, Object> origDataMap = (HashMap<Object, Object>) field;
                origDataMap.forEach((key, value) -> dataMap.put(resolveObject(key, keyType), resolveObject(value, valueType)));
                return dataMap;
            case BOOLEAN:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
                return field;
            case STRING:
                return field.toString();
            case TINYINT:
                return Byte.parseByte(field.toString());
            case SMALLINT:
                return Short.parseShort(field.toString());
            case NULL:
                return null;
            case BYTES:
                ByteBuffer buffer = (ByteBuffer) field;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes, 0, bytes.length);
                return bytes;
            case TIMESTAMP:
                Instant instant = Instant.ofEpochMilli((long) field);
                return LocalDateTime.ofInstant(instant, ZoneId.of("+8"));
            default:
                // do nothing
                // never got in there
                throw new MaxcomputeConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "SeaTunnel not support this data type now");
        }
    }

    public static SeaTunnelRow getSeaTunnelRowData(Record rs, SeaTunnelRowType typeInfo) throws SQLException {
        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();
        for (int i = 0; i < rs.getColumns().length; i++) {
            fields.add(resolveObject(rs.get(i), seaTunnelDataTypes[i]));
        }
        return new SeaTunnelRow(fields.toArray());
    }

    public static Record getMaxcomputeRowData(SeaTunnelRow seaTunnelRow, SeaTunnelRowType seaTunnelRowType) {
        TableSchema tableSchema = seaTunnelRowType2TableSchema(seaTunnelRowType);
        ArrayRecord arrayRecord = new ArrayRecord(tableSchema);
        for (int i = 0; i < seaTunnelRow.getFields().length; i++) {
            arrayRecord.set(i, resolveObject(seaTunnelRow.getField(i), seaTunnelRowType.getFieldType(i)));
        }
        return arrayRecord;
    }
}
