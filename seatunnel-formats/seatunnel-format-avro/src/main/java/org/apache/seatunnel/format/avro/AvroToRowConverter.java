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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AvroToRowConverter implements Serializable {

    private static final long serialVersionUID = 8177020083886379563L;

    private DatumReader<GenericRecord> reader = null;
    private Schema schema;

    public AvroToRowConverter(SeaTunnelRowType rowType) {
        schema = SeaTunnelRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(rowType);
    }

    public DatumReader<GenericRecord> getReader() {
        if (reader == null) {
            reader = createReader();
        }
        return reader;
    }

    private DatumReader<GenericRecord> createReader() {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema, schema);
        datumReader.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        datumReader.getData().addLogicalTypeConversion(new TimeConversions.DateConversion());
        datumReader
                .getData()
                .addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        return datumReader;
    }

    public SeaTunnelRow converter(GenericRecord record, SeaTunnelRowType rowType) {
        String[] fieldNames = rowType.getFieldNames();

        Object[] values = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (record.getSchema().getField(fieldNames[i]) == null) {
                values[i] = null;
                continue;
            }
            values[i] = convertField(rowType.getFieldType(i), record.get(fieldNames[i]));
        }
        return new SeaTunnelRow(values);
    }

    private Object convertField(SeaTunnelDataType<?> dataType, Object val) {
        if (Objects.isNull(val)) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case STRING:
                return val.toString();
            case BOOLEAN:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case NULL:
            case DATE:
            case DECIMAL:
            case TIMESTAMP:
                return val;
            case BYTES:
                return ((ByteBuffer) val).array();
            case SMALLINT:
                return ((Integer) val).shortValue();
            case TINYINT:
                Class<?> typeClass = dataType.getTypeClass();
                if (typeClass == Byte.class) {
                    Integer integer = (Integer) val;
                    return integer.byteValue();
                }
                return val;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                Map<Object, Object> res = new HashMap<>();
                Map map = (Map) val;
                for (Object o : map.entrySet()) {
                    res.put(
                            convertField(mapType.getKeyType(), ((Map.Entry) o).getKey()),
                            convertField(mapType.getValueType(), ((Map.Entry) o).getValue()));
                }
                return res;
            case ARRAY:
                SeaTunnelDataType<?> basicType = ((ArrayType<?, ?>) dataType).getElementType();
                List<Object> list = (List<Object>) val;
                return convertArray(list, basicType);
            case ROW:
                SeaTunnelRowType subRow = (SeaTunnelRowType) dataType;
                return converter((GenericRecord) val, subRow);
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel avro format is not supported for this data type [%s]",
                                dataType.getSqlType());
                throw new SeaTunnelAvroFormatException(
                        AvroFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    protected Object convertArray(List<Object> val, SeaTunnelDataType<?> dataType) {
        if (val == null) {
            return null;
        }
        int length = val.size();
        Object instance = Array.newInstance(dataType.getTypeClass(), length);
        for (int i = 0; i < val.size(); i++) {
            Array.set(instance, i, convertField(dataType, val.get(i)));
        }
        return instance;
    }
}
