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

package org.apache.seatunnel.connectors.seatunnel.typesense.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@AllArgsConstructor
public class KeyExtractor implements Function<SeaTunnelRow, String>, Serializable {
    private final FieldFormatter[] fieldFormatters;
    private final String keyDelimiter;

    @Override
    public String apply(SeaTunnelRow row) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fieldFormatters.length; i++) {
            if (i > 0) {
                builder.append(keyDelimiter);
            }
            String value = fieldFormatters[i].format(row);
            builder.append(value);
        }
        return builder.toString();
    }

    public static Function<SeaTunnelRow, String> createKeyExtractor(
            SeaTunnelRowType rowType, String[] primaryKeys, String keyDelimiter) {
        if (primaryKeys == null) {
            return row -> null;
        }

        List<FieldFormatter> fieldFormatters = new ArrayList<>(primaryKeys.length);
        for (String fieldName : primaryKeys) {
            int fieldIndex = rowType.indexOf(fieldName);
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(fieldIndex);
            FieldFormatter fieldFormatter = createFieldFormatter(fieldIndex, fieldType);
            fieldFormatters.add(fieldFormatter);
        }
        return new KeyExtractor(fieldFormatters.toArray(new FieldFormatter[0]), keyDelimiter);
    }

    private static FieldFormatter createFieldFormatter(
            int fieldIndex, SeaTunnelDataType fieldType) {
        return row -> {
            switch (fieldType.getSqlType()) {
                case ROW:
                case ARRAY:
                case MAP:
                    throw new TypesenseConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            "Unsupported type: " + fieldType);
                case DATE:
                    LocalDate localDate = (LocalDate) row.getField(fieldIndex);
                    return localDate.toString();
                case TIME:
                    LocalTime localTime = (LocalTime) row.getField(fieldIndex);
                    return localTime.toString();
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) row.getField(fieldIndex);
                    return localDateTime.toString();
                default:
                    return row.getField(fieldIndex).toString();
            }
        };
    }

    private interface FieldFormatter extends Serializable {
        String format(SeaTunnelRow row);
    }
}
