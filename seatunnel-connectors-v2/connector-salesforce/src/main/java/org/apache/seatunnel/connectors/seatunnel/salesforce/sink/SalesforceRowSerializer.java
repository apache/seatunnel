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

package org.apache.seatunnel.connectors.seatunnel.salesforce.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSinkConfig;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

final class SalesforceRowSerializer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS'Z'");
    private final SeaTunnelRowType rowType;
    private final SalesforceSinkConfig config;

    SalesforceRowSerializer(SeaTunnelRowType rowType, SalesforceSinkConfig config) {
        validateSchema(rowType, config);
        this.rowType = rowType;
        this.config = config;
    }

    static void validateSchema(SeaTunnelRowType rowType, SalesforceSinkConfig config) {
        boolean externalIdFound = false;
        Set<String> names = new HashSet<>();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String name = rowType.getFieldName(i);
            SalesforceSinkConfig.requireIdentifier(name, "Input field");
            if (!names.add(name.toLowerCase(Locale.ROOT))
                    || "attributes".equalsIgnoreCase(name)
                    || "Id".equalsIgnoreCase(name)) {
                throw new IllegalArgumentException(
                        "Input schema contains a duplicate or reserved Salesforce field: " + name);
            }
            SqlType type = rowType.getFieldType(i).getSqlType();
            switch (type) {
                case STRING:
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TIMESTAMP_TZ:
                case BYTES:
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported Salesforce field type: " + name + " (" + type + ")");
            }
            if (name.equals(config.getExternalIdField())) {
                externalIdFound = true;
                if (type != SqlType.STRING
                        && type != SqlType.TINYINT
                        && type != SqlType.SMALLINT
                        && type != SqlType.INT
                        && type != SqlType.BIGINT
                        && type != SqlType.DECIMAL) {
                    throw new IllegalArgumentException(
                            "external_id_field must be STRING, an integer type or DECIMAL");
                }
            }
        }
        if (!externalIdFound) {
            throw new IllegalArgumentException(
                    "external_id_field must be present in the input schema");
        }
    }

    ObjectNode serialize(SeaTunnelRow row) {
        if (row.getFields().length != rowType.getTotalFields()) {
            throw new IllegalArgumentException(
                    "Input row does not match the Salesforce sink schema");
        }
        ObjectNode record = MAPPER.createObjectNode();
        record.putObject("attributes").put("type", config.getObjectName());
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String name = rowType.getFieldName(i);
            Object value = row.getField(i);
            if (value == null) {
                record.putNull(name);
                continue;
            }
            switch (rowType.getFieldType(i).getSqlType()) {
                case STRING:
                    record.put(name, (String) value);
                    break;
                case BOOLEAN:
                    record.put(name, (Boolean) value);
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    record.put(name, ((Number) value).longValue());
                    break;
                case FLOAT:
                    float floatValue = ((Number) value).floatValue();
                    if (!Float.isFinite(floatValue)) {
                        throw invalidNumber(name);
                    }
                    record.put(name, floatValue);
                    break;
                case DOUBLE:
                    double doubleValue = ((Number) value).doubleValue();
                    if (!Double.isFinite(doubleValue)) {
                        throw invalidNumber(name);
                    }
                    record.put(name, doubleValue);
                    break;
                case DECIMAL:
                    record.put(name, (BigDecimal) value);
                    break;
                case DATE:
                    record.put(name, ((LocalDate) value).toString());
                    break;
                case TIME:
                    LocalTime time = (LocalTime) value;
                    requireMilliseconds(time.getNano(), name);
                    record.put(name, time.format(TIME_FORMAT));
                    break;
                case TIMESTAMP:
                    requireMilliseconds(((LocalDateTime) value).getNano(), name);
                    record.put(
                            name,
                            ((LocalDateTime) value)
                                    .atOffset(ZoneOffset.UTC)
                                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
                    break;
                case TIMESTAMP_TZ:
                    requireMilliseconds(((OffsetDateTime) value).getNano(), name);
                    record.put(
                            name,
                            ((OffsetDateTime) value)
                                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
                    break;
                case BYTES:
                    record.put(name, ((byte[]) value).clone());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported Salesforce field: " + name);
            }
        }
        if (record.path(config.getExternalIdField()).isNull()
                || record.path(config.getExternalIdField()).asText().trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "external_id_field must be non-null and non-blank in every row");
        }
        return record;
    }

    String externalIdKey(ObjectNode record) {
        if (record.path(config.getExternalIdField()).isNumber()) {
            return record.path(config.getExternalIdField())
                    .decimalValue()
                    .stripTrailingZeros()
                    .toPlainString();
        }
        return record.path(config.getExternalIdField()).asText().toLowerCase(Locale.ROOT);
    }

    private IllegalArgumentException invalidNumber(String name) {
        return new IllegalArgumentException("Non-finite number in Salesforce field " + name);
    }

    private static void requireMilliseconds(int nanos, String name) {
        if (nanos % 1_000_000 != 0) {
            throw new IllegalArgumentException(
                    "Salesforce temporal fields support millisecond precision: " + name);
        }
    }
}
