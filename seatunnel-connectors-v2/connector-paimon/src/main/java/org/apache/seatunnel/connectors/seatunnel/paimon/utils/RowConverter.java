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
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.DateTimeUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    public static Object convertArrayType(
            String fieldName, InternalArray array, SeaTunnelDataType<?> dataType) {
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
                throw CommonError.unsupportedArrayGenericType(
                        PaimonConfig.CONNECTOR_IDENTITY,
                        dataType.getSqlType().toString(),
                        fieldName);
        }
    }

    /**
     * Convert SeaTunnel array to Paimon array {@link InternalArray}
     *
     * @param array SeaTunnel array object
     * @param dataType SeaTunnel array data type
     * @return Paimon array object {@link BinaryArray}
     */
    public static BinaryArray reconvert(
            String fieldName, Object array, SeaTunnelDataType<?> dataType) {
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
                throw CommonError.unsupportedArrayGenericType(
                        PaimonConfig.CONNECTOR_IDENTITY,
                        dataType.getSqlType().toString(),
                        fieldName);
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
    public static SeaTunnelRow convert(
            InternalRow rowData, SeaTunnelRowType seaTunnelRowType, TableSchema tableSchema) {
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            // judge the field is or not equals null
            if (rowData.isNullAt(i)) {
                objects[i] = null;
                continue;
            }
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            String fieldName = seaTunnelRowType.getFieldName(i);
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
                    Decimal decimal =
                            rowData.getDecimal(
                                    i,
                                    ((DecimalType) fieldType).getPrecision(),
                                    ((DecimalType) fieldType).getScale());
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
                    int dateInt = rowData.getInt(i);
                    objects[i] = DateTimeUtils.toLocalDate(dateInt);
                    break;
                case TIMESTAMP:
                    int precision = TimestampType.DEFAULT_PRECISION;
                    Optional<DataField> precisionOptional =
                            tableSchema.fields().stream()
                                    .filter(dataField -> dataField.name().equals(fieldName))
                                    .findFirst();
                    if (precisionOptional.isPresent()) {
                        precision = ((TimestampType) precisionOptional.get().type()).getPrecision();
                    }
                    Timestamp timestamp = rowData.getTimestamp(i, precision);
                    objects[i] = timestamp.toLocalDateTime();
                    break;
                case ARRAY:
                    InternalArray paimonArray = rowData.getArray(i);
                    ArrayType<?, ?> seatunnelArray = (ArrayType<?, ?>) fieldType;
                    objects[i] =
                            convertArrayType(
                                    fieldName, paimonArray, seatunnelArray.getElementType());
                    break;
                case MAP:
                    MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
                    InternalMap map = rowData.getMap(i);
                    InternalArray keyArray = map.keyArray();
                    InternalArray valueArray = map.valueArray();
                    SeaTunnelDataType<?> keyType = mapType.getKeyType();
                    SeaTunnelDataType<?> valueType = mapType.getValueType();
                    Object[] key = (Object[]) convertArrayType(fieldName, keyArray, keyType);
                    Object[] value = (Object[]) convertArrayType(fieldName, valueArray, valueType);
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
                    objects[i] = convert(row, (SeaTunnelRowType) rowType, tableSchema);
                    break;
                default:
                    throw CommonError.unsupportedDataType(
                            PaimonConfig.CONNECTOR_IDENTITY,
                            fieldType.getSqlType().toString(),
                            fieldName);
            }
        }
        return new SeaTunnelRow(objects);
    }

    /**
     * Convert SeaTunnel row {@link SeaTunnelRow} to Paimon row {@link InternalRow}
     *
     * @param seaTunnelRow SeaTunnel row object
     * @param seaTunnelRowType SeaTunnel row type
     * @param sinkTableSchema Paimon table schema
     * @return Paimon row object
     */
    public static InternalRow reconvert(
            SeaTunnelRow seaTunnelRow,
            SeaTunnelRowType seaTunnelRowType,
            TableSchema sinkTableSchema) {
        List<DataField> sinkTotalFields = sinkTableSchema.fields();
        int sourceTotalFields = seaTunnelRowType.getTotalFields();
        if (sourceTotalFields != sinkTotalFields.size()) {
            throw CommonError.writeRowErrorWithFiledsCountNotMatch(
                    "Paimon", sourceTotalFields, sinkTotalFields.size());
        }
        BinaryRow binaryRow = new BinaryRow(sourceTotalFields);
        BinaryWriter binaryWriter = new BinaryRowWriter(binaryRow);
        // Convert SeaTunnel RowKind to Paimon RowKind
        org.apache.paimon.types.RowKind rowKind =
                RowKindConverter.convertSeaTunnelRowKind2PaimonRowKind(seaTunnelRow.getRowKind());
        if (rowKind == null) {
            throw CommonError.unsupportedRowKind(
                    PaimonConfig.CONNECTOR_IDENTITY,
                    seaTunnelRow.getRowKind().shortString(),
                    seaTunnelRow.getTableId());
        }
        binaryRow.setRowKind(rowKind);
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        for (int i = 0; i < fieldTypes.length; i++) {
            // judge the field is or not equals null
            if (seaTunnelRow.getField(i) == null) {
                binaryWriter.setNullAt(i);
                continue;
            }
            checkCanWriteWithSchema(i, seaTunnelRowType, sinkTotalFields);
            String fieldName = seaTunnelRowType.getFieldName(i);
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
                    DataField decimalDataField =
                            SchemaUtil.getDataField(sinkTotalFields, fieldName);
                    org.apache.paimon.types.DecimalType decimalType =
                            (org.apache.paimon.types.DecimalType) decimalDataField.type();
                    binaryWriter.writeDecimal(
                            i,
                            Decimal.fromBigDecimal(
                                    (BigDecimal) seaTunnelRow.getField(i),
                                    decimalType.getPrecision(),
                                    decimalType.getScale()),
                            decimalType.getPrecision());
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
                    BinaryWriter.createValueSetter(DataTypes.DATE())
                            .setValue(binaryWriter, i, DateTimeUtils.toInternal(date));
                    break;
                case TIMESTAMP:
                    DataField dataField = SchemaUtil.getDataField(sinkTotalFields, fieldName);
                    int precision = ((TimestampType) dataField.type()).getPrecision();
                    LocalDateTime datetime = (LocalDateTime) seaTunnelRow.getField(i);
                    binaryWriter.writeTimestamp(
                            i, Timestamp.fromLocalDateTime(datetime), precision);
                    break;
                case MAP:
                    MapType<?, ?> mapType = (MapType<?, ?>) seaTunnelRowType.getFieldType(i);
                    SeaTunnelDataType<?> keyType = mapType.getKeyType();
                    SeaTunnelDataType<?> valueType = mapType.getValueType();
                    DataType paimonKeyType = RowTypeConverter.reconvert(fieldName, keyType);
                    DataType paimonValueType = RowTypeConverter.reconvert(fieldName, valueType);
                    Map<?, ?> field = (Map<?, ?>) seaTunnelRow.getField(i);
                    Object[] keys = field.keySet().toArray(new Object[0]);
                    Object[] values = field.values().toArray(new Object[0]);
                    binaryWriter.writeMap(
                            i,
                            BinaryMap.valueOf(
                                    reconvert(fieldName, keys, keyType),
                                    reconvert(fieldName, values, valueType)),
                            new InternalMapSerializer(paimonKeyType, paimonValueType));
                    break;
                case ARRAY:
                    ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelRowType.getFieldType(i);
                    BinaryArray paimonArray =
                            reconvert(
                                    fieldName,
                                    seaTunnelRow.getField(i),
                                    arrayType.getElementType());
                    binaryWriter.writeArray(
                            i,
                            paimonArray,
                            new InternalArraySerializer(
                                    RowTypeConverter.reconvert(
                                            fieldName, arrayType.getElementType())));
                    break;
                case ROW:
                    SeaTunnelDataType<?> rowType = seaTunnelRowType.getFieldType(i);
                    Object row = seaTunnelRow.getField(i);
                    InternalRow paimonRow =
                            reconvert(
                                    (SeaTunnelRow) row,
                                    (SeaTunnelRowType) rowType,
                                    sinkTableSchema);
                    RowType paimonRowType =
                            RowTypeConverter.reconvert((SeaTunnelRowType) rowType, sinkTableSchema);
                    binaryWriter.writeRow(i, paimonRow, new InternalRowSerializer(paimonRowType));
                    break;
                default:
                    throw CommonError.unsupportedDataType(
                            PaimonConfig.CONNECTOR_IDENTITY,
                            seaTunnelRowType.getFieldType(i).getSqlType().toString(),
                            fieldName);
            }
        }
        return binaryRow;
    }

    private static void checkCanWriteWithSchema(
            int i, SeaTunnelRowType seaTunnelRowType, List<DataField> fields) {
        String sourceFieldName = seaTunnelRowType.getFieldName(i);
        SeaTunnelDataType<?> sourceFieldType = seaTunnelRowType.getFieldType(i);
        DataField sinkDataField = fields.get(i);
        DataType exceptDataType =
                RowTypeConverter.reconvert(sourceFieldName, seaTunnelRowType.getFieldType(i));
        DataField exceptDataField = new DataField(i, sourceFieldName, exceptDataType);
        DataType sinkDataType = sinkDataField.type();
        if (!exceptDataType.getTypeRoot().equals(sinkDataType.getTypeRoot())
                || !StringUtils.equals(sourceFieldName, sinkDataField.name())) {
            throw CommonError.writeRowErrorWithSchemaIncompatibleSchema(
                    "Paimon",
                    sourceFieldName + StringUtils.SPACE + sourceFieldType.getSqlType(),
                    exceptDataField.asSQLString(),
                    sinkDataField.asSQLString());
        }
        if (sourceFieldType instanceof DecimalType
                && sinkDataType instanceof org.apache.paimon.types.DecimalType) {
            DecimalType sourceDecimalType = (DecimalType) sourceFieldType;
            org.apache.paimon.types.DecimalType sinkDecimalType =
                    (org.apache.paimon.types.DecimalType) sinkDataType;
            if (sinkDecimalType.getPrecision() < sourceDecimalType.getPrecision()
                    || sinkDecimalType.getScale() < sourceDecimalType.getScale()) {
                throw CommonError.writeRowErrorWithSchemaIncompatibleSchema(
                        "Paimon",
                        sourceFieldName + StringUtils.SPACE + sourceFieldType.getSqlType(),
                        exceptDataField.asSQLString(),
                        sinkDataField.asSQLString());
            }
        }
    }
}
