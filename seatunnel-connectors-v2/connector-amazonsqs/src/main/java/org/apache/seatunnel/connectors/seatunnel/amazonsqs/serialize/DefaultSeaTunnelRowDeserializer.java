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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorException;

import lombok.AllArgsConstructor;

import java.nio.charset.StandardCharsets;

@AllArgsConstructor
public class DefaultSeaTunnelRowDeserializer implements SeaTunnelRowDeserializer {

    private final SeaTunnelRowType seaTunnelRowType;

    @Override
    public SeaTunnelRow deserialize(String messageBody) {
        String[] lines = messageBody.split(System.lineSeparator());

        Object[] fields = new Object[seaTunnelRowType.getFieldNames().length];

        for (String line : lines) {
            String[] parts = line.split(":");
            if (parts.length == 2) {
                String fieldName = parts[0].trim();
                int fieldIndex = seaTunnelRowType.indexOf(fieldName);
                if (fieldIndex != -1) {
                    String fieldValueString = parts[1].trim();
                    SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(fieldIndex);
                    fields[fieldIndex] = convertStringToField(fieldValueString, fieldType);
                }
            }
        }

        return new SeaTunnelRow(fields);
    }

    private Object convertStringToField(String value, SeaTunnelDataType<?> fieldType) {
        switch (fieldType.getSqlType()) {
            case INT:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case STRING:
                return value;
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case BYTES:
                // Implement logic to convert string to byte array
                return value.getBytes(StandardCharsets.UTF_8);
                // Handle other data types as needed
            default:
                throw new AmazonSqsConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + fieldType);
        }
    }
}
