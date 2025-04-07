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

package org.apache.seatunnel.connectors.seatunnel.file.excel;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import com.alibaba.excel.enums.CellDataTypeEnum;
import com.alibaba.excel.metadata.Cell;
import com.alibaba.excel.metadata.data.ReadCellData;
import lombok.SneakyThrows;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class ExcelCellUtils implements Serializable {

    static final long serialVersionUID = 42L;

    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter timeFormatter;

    protected Config pluginConfig;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExcelCellUtils(
            Config pluginConfig,
            String dateFormatterPattern,
            String dateTimeFormatterPattern,
            String timeFormatterPattern) {
        this.pluginConfig = pluginConfig;
        this.dateFormatter = DateTimeFormatter.ofPattern(dateFormatterPattern);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatterPattern);
        this.timeFormatter = DateTimeFormatter.ofPattern(timeFormatterPattern);
    }

    private String getCellValue(ReadCellData cellData) {

        if (cellData.getStringValue() != null) {
            return cellData.getStringValue();
        } else if (cellData.getNumberValue() != null) {
            return cellData.getNumberValue().toString();
        } else if (cellData.getOriginalNumberValue() != null) {
            return cellData.getOriginalNumberValue().toString();
        } else if (cellData.getBooleanValue() != null) {
            return cellData.getBooleanValue().toString();
        } else if (cellData.getType() == CellDataTypeEnum.EMPTY) {
            return "";
        }
        return null;
    }

    @SneakyThrows(JsonProcessingException.class)
    public Object convert(Object field, SeaTunnelDataType<?> fieldType, @Nullable Cell cellRaw) {
        if (field == null && cellRaw == null) {
            return null;
        }

        String fieldValue =
                (field instanceof String) || cellRaw == null
                        ? field.toString()
                        : getCellValue((ReadCellData) cellRaw);

        SqlType sqlType = fieldType.getSqlType();

        if (fieldValue == null || (fieldValue.equals("") && sqlType != SqlType.STRING)) {
            return null;
        }

        switch (sqlType) {
            case MAP:
            case ARRAY:
                return objectMapper.readValue(fieldValue, fieldType.getTypeClass());
            case STRING:
                if (field instanceof Double) {
                    String stringValue = field.toString();
                    if (stringValue.endsWith(".0")) {
                        return stringValue.substring(0, stringValue.length() - 2);
                    }
                    return stringValue;
                }
                return fieldValue;
            case DOUBLE:
                return Double.parseDouble(fieldValue);
            case BOOLEAN:
                return Boolean.parseBoolean(fieldValue);
            case FLOAT:
                return (float) Double.parseDouble(fieldValue);
            case BIGINT:
                return (long) Double.parseDouble(fieldValue);
            case INT:
                return (int) Double.parseDouble(fieldValue);
            case TINYINT:
                return (byte) Double.parseDouble(fieldValue);
            case SMALLINT:
                return (short) Double.parseDouble(fieldValue);
            case DECIMAL:
                return BigDecimal.valueOf(Double.parseDouble(fieldValue));
            case DATE:
                return parseDate(field, fieldType);
            case TIME:
                return parseTime(field, fieldType);
            case TIMESTAMP:
                return parseTimestamp(field, fieldType);
            case NULL:
                return null;
            case BYTES:
                return fieldValue.getBytes(StandardCharsets.UTF_8);
            case ROW:
                return parseRow(fieldValue, fieldType);
            default:
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "User defined schema validation failed");
        }
    }

    private Object parseDate(Object fieldValue, SeaTunnelDataType<?> fieldType) {
        if (fieldValue instanceof LocalDateTime) {
            return ((LocalDateTime) fieldValue).toLocalDate();
        }
        return LocalDate.parse(fieldValue.toString(), dateFormatter);
    }

    private Object parseTime(Object fieldValue, SeaTunnelDataType<?> fieldType) {
        if (fieldValue instanceof LocalDateTime) {
            return ((LocalDateTime) fieldValue).toLocalTime();
        }
        return LocalTime.parse(fieldValue.toString(), timeFormatter);
    }

    private Object parseTimestamp(Object fieldValue, SeaTunnelDataType<?> fieldType) {
        if (fieldValue instanceof LocalDateTime) {
            return fieldValue;
        }
        return LocalDateTime.parse(fieldValue.toString(), dateTimeFormatter);
    }

    private Object parseRow(String fieldValue, SeaTunnelDataType<?> fieldType) {
        String delimiter =
                ReadonlyConfig.fromConfig(pluginConfig)
                        .get(BaseSourceConfigOptions.FIELD_DELIMITER);
        String[] context = fieldValue.split(delimiter);
        SeaTunnelRowType ft = (SeaTunnelRowType) fieldType;
        int length = context.length;
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(length);
        for (int j = 0; j < length; j++) {
            seaTunnelRow.setField(j, convert(context[j], ft.getFieldType(j), null));
        }
        return seaTunnelRow;
    }
}
