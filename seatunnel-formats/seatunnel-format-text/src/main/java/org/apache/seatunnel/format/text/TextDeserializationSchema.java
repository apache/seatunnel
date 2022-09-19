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

package org.apache.seatunnel.format.text;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class TextDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final String delimiter;

    public TextDeserializationSchema(SeaTunnelRowType seaTunnelRowType, String delimiter) {
        checkNotNull(seaTunnelRowType);
        checkNotNull(delimiter);
        this.seaTunnelRowType = seaTunnelRowType;
        this.delimiter = delimiter;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        String content = new String(message);
        String[] splits = content.split(delimiter);
        if (seaTunnelRowType.getTotalFields() != splits.length) {
            throw new IndexOutOfBoundsException("The data does not match the configured schema information, please check");
        }
        Object[] objects = new Object[splits.length];
        for (int i = 0; i < splits.length; i++) {
            objects[i] = convert(splits[i], seaTunnelRowType.getFieldType(i));
        }
        return new SeaTunnelRow(objects);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    private Object convert(String field, SeaTunnelDataType<?> fieldType) {
        switch (fieldType.getSqlType()) {
            case ARRAY:
            case MAP:
            case ROW:
                throw new UnsupportedOperationException("SeaTunnel text format does not support parsing these types [array, map, row]");
            case STRING:
                return field;
            case BOOLEAN:
                return Boolean.parseBoolean(field);
            case TINYINT:
                return Byte.parseByte(field);
            case SMALLINT:
                return Short.parseShort(field);
            case INT:
                return Integer.parseInt(field);
            case BIGINT:
                return Long.parseLong(field);
            case FLOAT:
                return Float.parseFloat(field);
            case DOUBLE:
                return Double.parseDouble(field);
            case DECIMAL:
                return new BigDecimal(field);
            case NULL:
                return null;
            case BYTES:
                return field.getBytes();
            case DATE:
                // TODO: setting time formatter
                return LocalDate.parse(field);
            case TIME:
                return LocalTime.parse(field);
            case TIMESTAMP:
                return LocalDateTime.parse(field);
            default:
                // do nothing
                // never get in there
                return field;
        }
    }
}
