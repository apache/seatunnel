/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.avro.exception.AvroFormatErrorCode;
import org.apache.seatunnel.format.avro.exception.SeaTunnelAvroFormatException;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RowToAvroConverter implements Serializable {

    private static final long serialVersionUID = -576124379280229724L;

    private final Schema schema;
    private final SeaTunnelRowType rowType;
    private final DatumWriter<GenericRecord> writer;

    public RowToAvroConverter(SeaTunnelRowType rowType) {
        this.schema = SeaTunnelRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(rowType);
        this.rowType = rowType;
        this.writer = createWriter();
    }

    private DatumWriter<GenericRecord> createWriter() {
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        datumWriter.getData().addLogicalTypeConversion(new TimeConversions.DateConversion());
        datumWriter
                .getData()
                .addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        return datumWriter;
    }

    public Schema getSchema() {
        return schema;
    }

    public DatumWriter<GenericRecord> getWriter() {
        return writer;
    }

    public GenericRecord convertRowToGenericRecord(SeaTunnelRow element) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = rowType.getFieldName(i);
            Object value = element.getField(i);
            builder.set(fieldName.toLowerCase(), resolveObject(value, rowType.getFieldType(i)));
        }
        return builder.build();
    }

    private Object resolveObject(Object data, SeaTunnelDataType<?> seaTunnelDataType) {
        if (data == null) {
            return null;
        }
        switch (seaTunnelDataType.getSqlType()) {
            case STRING:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case DECIMAL:
            case DATE:
            case TIMESTAMP:
                return data;
            case TINYINT:
            case SMALLINT:
                Class<?> typeClass = seaTunnelDataType.getTypeClass();
                if (typeClass == Byte.class) {
                    if (data instanceof Byte) {
                        Byte aByte = (Byte) data;
                        return Byte.toUnsignedInt(aByte);
                    }
                } else if (typeClass == Short.class) {
                    if (data instanceof Short) {
                        return ((Short) data).intValue();
                    }
                }
                return data;
            case BYTES:
                return ByteBuffer.wrap((byte[]) data);
            case ARRAY:
                SeaTunnelDataType<?> basicType =
                        ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                int length = Array.getLength(data);
                ArrayList<Object> records = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    records.add(resolveObject(Array.get(data, i), basicType));
                }
                return records;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) seaTunnelDataType;
                SeaTunnelDataType<?> keyType = mapType.getKeyType();
                SeaTunnelDataType<?> valueType = mapType.getValueType();
                Map<Object, Object> mapData = new HashMap<>();
                for (Map.Entry<?, ?> entry : ((Map<Object, Object>) data).entrySet()) {
                    mapData.put(
                            resolveObject(entry.getKey(), keyType),
                            resolveObject(entry.getValue(), valueType));
                }
                return mapData;

            case ROW:
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) data;
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                Schema recordSchema =
                        SeaTunnelRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(
                                (SeaTunnelRowType) seaTunnelDataType);
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
                for (int i = 0; i < fieldNames.length; i++) {
                    recordBuilder.set(
                            fieldNames[i].toLowerCase(),
                            resolveObject(seaTunnelRow.getField(i), fieldTypes[i]));
                }
                return recordBuilder.build();
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel avro format is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new SeaTunnelAvroFormatException(
                        AvroFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}
