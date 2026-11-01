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
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.poi.ss.util.NumberToTextConverter;

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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public class ExcelCellUtils implements Serializable {

    static final long serialVersionUID = 42L;

    /** Column types read from the stored number of a number cell, not its display text. */
    public static final Set<SqlType> NUMERIC_SQL_TYPES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            SqlType.TINYINT,
                            SqlType.SMALLINT,
                            SqlType.INT,
                            SqlType.BIGINT,
                            SqlType.FLOAT,
                            SqlType.DOUBLE,
                            SqlType.DECIMAL));

    /**
     * The largest distance, in units in the last place, between a stored number and the text Excel
     * shows for it that is taken as computation noise rather than real digits.
     */
    private static final int NOISE_ULPS = 4;

    /** The most significant digits the value Excel shows for a noisy number can have. */
    private static final int NOISE_MAX_SHOWN_DIGITS = 13;

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

    /**
     * Whether the EasyExcel cell is a number cell read into a numeric column. EasyExcel hands the
     * text formatted with the cell's display format ("General" keeps about 10 significant digits),
     * so such a cell is read from its stored number instead. The original number value is used
     * because {@link ReadCellData#getNumberValue()} is rounded to 15 significant digits.
     */
    private static boolean isNumericCell(@Nullable Cell cellRaw, SqlType sqlType) {
        return NUMERIC_SQL_TYPES.contains(sqlType)
                && cellRaw instanceof ReadCellData
                && ((ReadCellData<?>) cellRaw).getType() == CellDataTypeEnum.NUMBER
                && ((ReadCellData<?>) cellRaw).getOriginalNumberValue() != null;
    }

    /**
     * The text of a number stored in an Excel cell that is read into a numeric column. DOUBLE and
     * FLOAT columns keep the exact double, and so does an integral value in any column. Integer and
     * DECIMAL columns take the text Excel shows for the number (15 significant digits) when the
     * stored value differs from it only by the binary noise of a computation: {@code (0.1 + 0.7) *
     * 10} is stored as {@code 7.999999999999999}, which would truncate to {@code 7} in an INT
     * column and carry the noise into a DECIMAL. A value stored with 16 or 17 real digits is kept
     * exact.
     */
    public static String numberText(double value, SqlType sqlType) {
        String exact = Double.toString(value);
        if (sqlType == SqlType.DOUBLE || sqlType == SqlType.FLOAT || value == Math.rint(value)) {
            return exact;
        }
        String shown = NumberToTextConverter.toText(value);
        return isComputationNoise(value, shown) ? shown : exact;
    }

    /**
     * Whether a stored number is the value Excel shows plus the binary noise of a computation: it
     * is within {@link #NOISE_ULPS} of the shown value, and the shown value has at most {@link
     * #NOISE_MAX_SHOWN_DIGITS} significant digits, so the digits Excel dropped were a run of zeros
     * or nines. A value with 16 real digits rounds to a 15 digit shown value and is not noise.
     */
    private static boolean isComputationNoise(double value, String shown) {
        return new BigDecimal(shown).stripTrailingZeros().precision() <= NOISE_MAX_SHOWN_DIGITS
                && Math.abs(Double.parseDouble(shown) - value) <= NOISE_ULPS * Math.ulp(value);
    }

    /**
     * Gives a decimal the scale of its column when that does not change its value and the result
     * fits the column precision, so that a number cell ({@code 5000.0}) and a text cell ({@code
     * 5000.0000}) of a {@code decimal(20, 4)} column read alike. Any other value is returned as it
     * is, so no digit is rounded away; the bound also keeps a value with a large exponent, such as
     * {@code 1E+2147483647}, from being expanded.
     */
    private static BigDecimal toDeclaredScale(BigDecimal value, DecimalType decimalType) {
        int scale = decimalType.getScale();
        if (value.signum() == 0) {
            return BigDecimal.ZERO.setScale(scale);
        }
        // precision - scale does not change when trailing zeros are stripped, so check the bound
        // first: stripping the zeros of a value such as 1000E+2147483647 would underflow the scale
        long integerDigits = (long) value.precision() - value.scale();
        if (integerDigits + scale > decimalType.getPrecision()) {
            return value;
        }
        BigDecimal stripped = value.stripTrailingZeros();
        return stripped.scale() > scale ? value : stripped.setScale(scale);
    }

    @SneakyThrows(JsonProcessingException.class)
    public Object convert(Object field, SeaTunnelDataType<?> fieldType, @Nullable Cell cellRaw) {
        if (field == null && cellRaw == null) {
            return null;
        }

        SqlType sqlType = fieldType.getSqlType();

        String fieldValue;
        if (isNumericCell(cellRaw, sqlType)) {
            // The original value is the stored text, which Excel may write with 17 significant
            // digits (5.555 as 5.5549999999999997). Read the same double as the POI engine does.
            fieldValue =
                    numberText(
                            ((ReadCellData<?>) cellRaw).getOriginalNumberValue().doubleValue(),
                            sqlType);
        } else if (field instanceof String || cellRaw == null) {
            fieldValue = field.toString();
        } else {
            fieldValue = getCellValue((ReadCellData) cellRaw);
        }

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
                // Parse the text directly: going through double drops digits beyond ~15
                return toDeclaredScale(new BigDecimal(fieldValue.trim()), (DecimalType) fieldType);
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
                ReadonlyConfig.fromConfig(pluginConfig).get(FileBaseSourceOptions.FIELD_DELIMITER);
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
