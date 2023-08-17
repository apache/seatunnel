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

package org.apache.seatunnel.connectors.seatunnel.paimon.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.data.serializer.InternalMapSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/** The converter for converting {@link InternalRow} and {@link SeaTunnelRow} */
public class RowConverter {

    private RowConverter() {}

    /**
     * Convert Paimon array {@link InternalArray} to SeaTunnel array.
     *
     * @param array Paimon array object
     * @param dataType Data type of the array
     * @return SeaTunnel array object
     */
    public static Object convert(InternalArray array, SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                String[] strings = new String[array.size()];
                for (int j = 0; j < strings.length; j++) {
                    strings[j] = array.getString(j).toString();
                }
                return strings;
            case BOOLEAN:
                Boolean[] booleans = new Boolean[array.size()];
                for (int j = 0; j < booleans.length; j++) {
                    booleans[j] = array.getBoolean(j);
                }
                return booleans;
            case TINYINT:
                Byte[] bytes = new Byte[array.size()];
                for (int j = 0; j < bytes.length; j++) {
                    bytes[j] = array.getByte(j);
                }
                return bytes;
            case SMALLINT:
                Short[] shorts = new Short[array.size()];
                for (int j = 0; j < shorts.length; j++) {
                    shorts[j] = array.getShort(j);
                }
                return shorts;
            case INT:
                Integer[] integers = new Integer[array.size()];
                for (int j = 0; j < integers.length; j++) {
                    integers[j] = array.getInt(j);
                }
                return integers;
            case BIGINT:
                Long[] longs = new Long[array.size()];
                for (int j = 0; j < longs.length; j++) {
                    longs[j] = array.getLong(j);
                }
                return longs;
            case FLOAT:
                Float[] floats = new Float[array.size()];
                for (int j = 0; j < floats.length; j++) {
                    floats[j] = array.getFloat(j);
                }
                return floats;
            case DOUBLE:
                Double[] doubles = new Double[array.size()];
                for (int j = 0; j < doubles.length; j++) {
                    doubles[j] = array.getDouble(j);
                }
                return doubles;
            default:
                String errorMsg =
                        String.format("Array type not support this genericType [%s]", dataType);
                throw new PaimonConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    /**
     * Convert SeaTunnel array to Paimon array {@link InternalArray}
     *
     * @param array SeaTunnel array object
     * @param dataType SeaTunnel array data type
     * @return Paimon array object {@link BinaryArray}
     */
    public static BinaryArray convert(Object array, SeaTunnelDataType<?> dataType) {
        int length = ((Object[]) array).length;
        BinaryArray binaryArray = new BinaryArray();
        BinaryArrayWriter binaryArrayWriter;
        switch (dataType.getSqlType()) {
            case STRING:
                binaryArrayWriter =
                        new BinaryArrayWriter(
                                binaryArray,
                                length,
                                BinaryArray.calculateFixLengthPartSize(DataTypes.STRING()));
                for (int i = 0; i < ((Object[]) array).length; i++) {
                    binaryArrayWriter.writeString(
                            i, BinaryString.fromString((String) ((Object[]) array)[i]));
                }
                break;
            case BOOLEAN:
                binaryArrayWriter =
                        new BinaryArrayWriter(
                                binaryArray,
                                length,
                                BinaryArray.calculateFixLengthPartSize(DataTypes.BOOLEAN()));
                for (int i = 0; i < ((Object[]) array).length; i++) {
                    binaryArrayWriter.writeBoolean(i, (Boolean) ((Object[]) array)[i]);
                }
                break;
            case TINYINT:
                binaryArrayWriter =
                        new BinaryArrayWriter(
                                binaryArray,
                                length,
                                BinaryArray.calculateFixLengthPartSize(DataTypes.TINYINT()));
                for (int i = 0; i < ((Object[]) array).length; i++) {
                    binaryArrayWriter.writeByte(i, (Byte) ((Object[]) array)[i]);
                }
                break;
            case SMALLINT:
                binaryArrayWriter =
                        new BinaryArrayWriter(
                                binaryArray,
                                length,
                                BinaryArray.calculateFixLengthPartSize(DataTypes.SMALLINT()));
                for (int i = 0; i < ((Object[]) array).length; i++) {
                    binaryArrayWriter.writeShort(i, (Short) ((Object[]) array)[i]);
                }
                break;
            case INT:
                binaryArrayWriter =
                        new BinaryArrayWriter(
                                binaryArray,
                                length,
                                BinaryArray.calculateFixLengthPartSize(DataTypes.INT()));
                for (int i = 0; i < ((Object[]) array).length; i++) {
                    binaryArrayWriter.writeInt(i, (Integer) ((Object[]) array)[i]);
                }
                break;
            case BIGINT:
                binaryArrayWriter =
                        new BinaryArrayWriter(
                                binaryArray,
                                length,
                                BinaryArray.calculateFixLengthPartSize(DataTypes.BIGINT()));
                for (int i = 0; i < ((Object[]) array).length; i++) {
                    binaryArrayWriter.writeLong(i, (Long) ((Object[]) array)[i]);
                }
                break;
            case FLOAT:
                binaryArrayWriter =
                        new BinaryArrayWriter(
                                binaryArray,
                                length,
                                BinaryArray.calculateFixLengthPartSize(DataTypes.FLOAT()));
                for (int i = 0; i < ((Object[]) array).length; i++) {
                    binaryArrayWriter.writeFloat(i, (Float) ((Object[]) array)[i]);
                }
                break;
            case DOUBLE:
                binaryArrayWriter =
                        new BinaryArrayWriter(
                                binaryArray,
                                length,
                                BinaryArray.calculateFixLengthPartSize(DataTypes.DOUBLE()));
                for (int i = 0; i < ((Object[]) array).length; i++) {
                    binaryArrayWriter.writeDouble(i, (Double) ((Object[]) array)[i]);
                }
                break;
            default:
                String errorMsg =
                        String.format("Array type not support this genericType [%s]", dataType);
                throw new PaimonConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
        binaryArrayWriter.complete();
        return binaryArray;
    }

    /**
     * Convert Paimon row {@link InternalRow} to SeaTunnelRow {@link SeaTunnelRow}
     *
     * @param rowData Paimon row object
     * @param seaTunnelRowType SeaTunnel row type
     * @return SeaTunnel row
     */
    public static SeaTunnelRow convert(InternalRow rowData, SeaTunnelRowType seaTunnelRowType) {
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            // judge the field is or not equals null
            if (rowData.isNullAt(i)) {
                objects[i] = null;
                continue;
            }
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            switch (fieldType.getSqlType()) {
                case TINYINT:
                    objects[i] = rowData.getByte(i);
                    break;
                case SMALLINT:
                    objects[i] = rowData.getShort(i);
                    break;
                case INT:
                    objects[i] = rowData.getInt(i);
                    break;
                case BIGINT:
                    objects[i] = rowData.getLong(i);
                    break;
                case FLOAT:
                    objects[i] = rowData.getFloat(i);
                    break;
                case DOUBLE:
                    objects[i] = rowData.getDouble(i);
                    break;
                case DECIMAL:
                    SeaTunnelDataType<?> decimalType = seaTunnelRowType.getFieldType(i);
                    Decimal decimal =
                            rowData.getDecimal(
                                    i,
                                    ((DecimalType) decimalType).getPrecision(),
                                    ((DecimalType) decimalType).getScale());
                    objects[i] = decimal.toBigDecimal();
                    break;
                case STRING:
                    objects[i] = rowData.getString(i).toString();
                    break;
                case BOOLEAN:
                    objects[i] = rowData.getBoolean(i);
                    break;
                case BYTES:
                    objects[i] = rowData.getBinary(i);
                    break;
                case DATE:
                    objects[i] = rowData.getTimestamp(i, 3).toLocalDateTime().toLocalDate();
                    break;
                case TIMESTAMP:
                    // Now SeaTunnel not supported assigned the timezone for timestamp,
                    // so we use the default precision 6
                    Timestamp timestamp = rowData.getTimestamp(i, 6);
                    objects[i] = timestamp.toLocalDateTime();
                    break;
                case ARRAY:
                    SeaTunnelDataType<?> arrayType = seaTunnelRowType.getFieldType(i);
                    InternalArray array = rowData.getArray(i);
                    objects[i] = convert(array, ((ArrayType<?, ?>) arrayType).getElementType());
                    break;
                case MAP:
                    SeaTunnelDataType<?> mapType = seaTunnelRowType.getFieldType(i);
                    InternalMap map = rowData.getMap(i);
                    InternalArray keyArray = map.keyArray();
                    InternalArray valueArray = map.valueArray();
                    SeaTunnelDataType<?> keyType = ((MapType<?, ?>) mapType).getKeyType();
                    SeaTunnelDataType<?> valueType = ((MapType<?, ?>) mapType).getValueType();
                    Object[] key = (Object[]) convert(keyArray, keyType);
                    Object[] value = (Object[]) convert(valueArray, valueType);
                    Map<Object, Object> mapData = new HashMap<>();
                    for (int j = 0; j < key.length; j++) {
                        mapData.put(key[j], value[j]);
                    }
                    objects[i] = mapData;
                    break;
                case ROW:
                    SeaTunnelDataType<?> rowType = seaTunnelRowType.getFieldType(i);
                    InternalRow row =
                            rowData.getRow(i, ((SeaTunnelRowType) rowType).getTotalFields());
                    objects[i] = convert(row, (SeaTunnelRowType) rowType);
                    break;
                default:
                    throw new PaimonConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "SeaTunnel does not support this type");
            }
        }
        return new SeaTunnelRow(objects);
    }

    /**
     * Convert SeaTunnel row {@link SeaTunnelRow} to Paimon row {@link InternalRow}
     *
     * @param seaTunnelRow SeaTunnel row object
     * @param seaTunnelRowType SeaTunnel row type
     * @return Paimon row object
     */
    public static InternalRow convert(
            SeaTunnelRow seaTunnelRow, SeaTunnelRowType seaTunnelRowType) {
        BinaryRow binaryRow = new BinaryRow(seaTunnelRowType.getTotalFields());
        BinaryWriter binaryWriter = new BinaryRowWriter(binaryRow);
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        for (int i = 0; i < fieldTypes.length; i++) {
            // judge the field is or not equals null
            if (seaTunnelRow.getField(i) == null) {
                binaryWriter.setNullAt(i);
                continue;
            }
            switch (fieldTypes[i].getSqlType()) {
                case TINYINT:
                    binaryWriter.writeByte(i, (Byte) seaTunnelRow.getField(i));
                    break;
                case SMALLINT:
                    binaryWriter.writeShort(i, (Short) seaTunnelRow.getField(i));
                    break;
                case INT:
                    binaryWriter.writeInt(i, (Integer) seaTunnelRow.getField(i));
                    break;
                case BIGINT:
                    binaryWriter.writeLong(i, (Long) seaTunnelRow.getField(i));
                    break;
                case FLOAT:
                    binaryWriter.writeFloat(i, (Float) seaTunnelRow.getField(i));
                    break;
                case DOUBLE:
                    binaryWriter.writeDouble(i, (Double) seaTunnelRow.getField(i));
                    break;
                case DECIMAL:
                    DecimalType fieldType = (DecimalType) seaTunnelRowType.getFieldType(i);
                    binaryWriter.writeDecimal(
                            i,
                            Decimal.fromBigDecimal(
                                    (BigDecimal) seaTunnelRow.getField(i),
                                    fieldType.getPrecision(),
                                    fieldType.getScale()),
                            fieldType.getPrecision());
                    break;
                case STRING:
                    binaryWriter.writeString(
                            i, BinaryString.fromString((String) seaTunnelRow.getField(i)));
                    break;
                case BYTES:
                    binaryWriter.writeBinary(i, (byte[]) seaTunnelRow.getField(i));
                    break;
                case BOOLEAN:
                    binaryWriter.writeBoolean(i, (Boolean) seaTunnelRow.getField(i));
                    break;
                case DATE:
                    LocalDate date = (LocalDate) seaTunnelRow.getField(i);
                    LocalTime time = LocalTime.of(0, 0, 0);
                    binaryWriter.writeTimestamp(
                            i, Timestamp.fromLocalDateTime(date.atTime(time)), 3);
                    break;
                case TIMESTAMP:
                    LocalDateTime datetime = (LocalDateTime) seaTunnelRow.getField(i);
                    binaryWriter.writeTimestamp(i, Timestamp.fromLocalDateTime(datetime), 9);
                    break;
                case MAP:
                    MapType<?, ?> mapType = (MapType<?, ?>) seaTunnelRowType.getFieldType(i);
                    SeaTunnelDataType<?> keyType = mapType.getKeyType();
                    SeaTunnelDataType<?> valueType = mapType.getValueType();
                    DataType paimonKeyType = RowTypeConverter.convert(keyType);
                    DataType paimonValueType = RowTypeConverter.convert(valueType);
                    Map<?, ?> field = (Map<?, ?>) seaTunnelRow.getField(i);
                    Object[] keys = field.keySet().toArray(new Object[0]);
                    Object[] values = field.values().toArray(new Object[0]);
                    binaryWriter.writeMap(
                            i,
                            BinaryMap.valueOf(convert(keys, keyType), convert(values, valueType)),
                            new InternalMapSerializer(paimonKeyType, paimonValueType));
                    break;
                case ARRAY:
                    ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelRowType.getFieldType(i);
                    BinaryArray paimonArray =
                            convert(seaTunnelRow.getField(i), arrayType.getElementType());
                    binaryWriter.writeArray(
                            i,
                            paimonArray,
                            new InternalArraySerializer(
                                    RowTypeConverter.convert(arrayType.getElementType())));
                    break;
                case ROW:
                    SeaTunnelDataType<?> rowType = seaTunnelRowType.getFieldType(i);
                    Object row = seaTunnelRow.getField(i);
                    InternalRow paimonRow = convert((SeaTunnelRow) row, (SeaTunnelRowType) rowType);
                    RowType paimonRowType = RowTypeConverter.convert((SeaTunnelRowType) rowType);
                    binaryWriter.writeRow(i, paimonRow, new InternalRowSerializer(paimonRowType));
                    break;
                default:
                    throw new PaimonConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type " + seaTunnelRowType.getFieldType(i));
            }
        }
        return binaryRow;
    }
}
