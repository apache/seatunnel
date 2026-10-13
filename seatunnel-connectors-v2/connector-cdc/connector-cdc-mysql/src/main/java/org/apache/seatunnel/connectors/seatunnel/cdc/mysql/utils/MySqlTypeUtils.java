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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.DefaultValueUtils;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

/** Utilities for converting from MySQL types to SeaTunnel types. */
@Slf4j
public class MySqlTypeUtils {

    /**
     * Converter used when {@code int_type_narrowing=false}: identical to {@link
     * MySqlTypeConverter#DEFAULT_INSTANCE} except narrowing is off, so {@code tinyint(1)} stays
     * TINYINT (byte) instead of becoming BOOLEAN. Pre-built singleton (no per-column allocation).
     */
    private static final MySqlTypeConverter NO_INT_NARROWING_CONVERTER =
            new MySqlTypeConverter(MySqlVersion.V_5_7, false);

    public static SeaTunnelDataType<?> convertFromColumn(
            Column column, RelationalDatabaseConnectorConfig dbzConnectorConfig) {
        return convertToSeaTunnelColumn(column, dbzConnectorConfig).getDataType();
    }

    public static org.apache.seatunnel.api.table.catalog.Column convertToSeaTunnelColumn(
            io.debezium.relational.Column column,
            RelationalDatabaseConnectorConfig dbzConnectorConfig) {
        String bigIntUnsignedHandlingModeStr =
                dbzConnectorConfig
                        .getConfig()
                        .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        final boolean timeAdjusterEnabled =
                dbzConnectorConfig
                        .getConfig()
                        .getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
                MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(
                        bigIntUnsignedHandlingModeStr);
        MySqlValueConverters mySqlValueConverters =
                new MySqlValueConverters(
                        dbzConnectorConfig.getDecimalMode(),
                        dbzConnectorConfig.getTemporalPrecisionMode(),
                        bigIntUnsignedHandlingMode.asBigIntUnsignedMode(),
                        dbzConnectorConfig.binaryHandlingMode(),
                        timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : (x) -> x,
                        MySqlValueConverters::defaultParsingErrorHandler);
        MySqlDefaultValueConverter mySqlDefaultValueConverter =
                new MySqlDefaultValueConverter(mySqlValueConverters);

        Optional<String> defaultValueExpression = column.defaultValueExpression();
        Object defaultValue = defaultValueExpression.orElse(null);
        if (defaultValueExpression.isPresent()
                && !DefaultValueUtils.isMysqlSpecialDefaultValue(defaultValue)) {
            defaultValue =
                    mySqlDefaultValueConverter
                            .parseDefaultValue(column, defaultValueExpression.get())
                            .orElse(null);
        }
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.name())
                        .columnType(column.typeName())
                        .dataType(column.typeName())
                        .scale(column.scale().orElse(0))
                        .nullable(column.isOptional())
                        .defaultValue(defaultValue)
                        .comment(column.comment());

        if (column.length() >= 0) {
            builder.length((long) column.length()).precision((long) column.length());
        }

        switch (column.typeName().toUpperCase()) {
            case MySqlTypeConverter.MYSQL_CHAR:
            case MySqlTypeConverter.MYSQL_VARCHAR:
                if (column.length() <= 0) {
                    // set default length
                    builder.columnType(MySqlTypeConverter.MYSQL_VARCHAR);
                    builder.length(TypeDefineUtils.charTo4ByteLength(1L));
                } else {
                    // parse length from ddl sql
                    builder.columnType(
                            String.format(
                                    "%s(%s)", MySqlTypeConverter.MYSQL_VARCHAR, column.length()));
                    builder.length(TypeDefineUtils.charTo4ByteLength((long) column.length()));
                }
                break;
            case MySqlTypeConverter.MYSQL_TIME:
                if (column.length() <= 0) {
                    builder.columnType(MySqlTypeConverter.MYSQL_TIME);
                } else {
                    builder.columnType(
                            String.format(
                                    "%s(%s)", MySqlTypeConverter.MYSQL_TIME, column.length()));
                    builder.scale(column.length());
                }
                break;
            case MySqlTypeConverter.MYSQL_TIMESTAMP:
                if (column.length() <= 0) {
                    builder.columnType(MySqlTypeConverter.MYSQL_TIMESTAMP);
                } else {
                    builder.columnType(
                            String.format(
                                    "%s(%s)", MySqlTypeConverter.MYSQL_TIMESTAMP, column.length()));
                    builder.scale(column.length());
                }
                break;
            case MySqlTypeConverter.MYSQL_DATETIME:
                if (column.length() <= 0) {
                    builder.columnType(MySqlTypeConverter.MYSQL_DATETIME);
                } else {
                    builder.columnType(
                            String.format(
                                    "%s(%s)", MySqlTypeConverter.MYSQL_DATETIME, column.length()));
                    builder.scale(column.length());
                }
                break;
            case "TINYINT":
                // Debezium reports the bare type name "TINYINT", but the narrowing rule in
                // MySqlTypeConverter checks columnType.equalsIgnoreCase("tinyint(1)"). Re-append
                // the length so tinyint(1) is detectable on the CDC path; otherwise it can never
                // be narrowed (or kept) according to int_type_narrowing.
                if (column.length() > 0) {
                    builder.columnType(String.format("TINYINT(%s)", column.length()));
                }
                break;
            case MySqlTypeConverter.MYSQL_ENUM:
            case MySqlTypeConverter.MYSQL_SET:
                // Debezium reports a bookkeeping length for these types (options * 2 - 1 for SET,
                // always 1 for ENUM) rather than the length of the stored value. Forwarding it
                // makes MySqlTypeConverter produce a column length of 1 for an ENUM and options *
                // 2 - 1 for a SET, which sinks that rebuild the type from the column length (any
                // non-MySQL sink, via JdbcDialect#applySchemaChange -> reconvert) turn into an
                // undersized column. Derive the real length from the option list instead; it is
                // available on the DDL path and, for completeness, whenever the column carries it.
                long optionListLength = maxOptionListLength(column);
                if (optionListLength > 0) {
                    builder.length(optionListLength).precision(optionListLength);
                }
                break;
            default:
                break;
        }
        // Honor the int_type_narrowing source option (carried via the Debezium properties).
        // Default true keeps the original DEFAULT_INSTANCE behavior byte-for-byte.
        boolean intTypeNarrowing =
                dbzConnectorConfig.getConfig().getBoolean("int_type_narrowing", true);
        return (intTypeNarrowing ? MySqlTypeConverter.DEFAULT_INSTANCE : NO_INT_NARROWING_CONVERTER)
                .convert(builder.build());
    }

    /**
     * Computes the longest value a {@code SET} / {@code ENUM} column can store, in characters: the
     * longest member for an {@code ENUM}, and the commas plus the sum of all members for a {@code
     * SET}.
     *
     * @param column Debezium column, whose option list is populated on the DDL-parsing path
     * @return the derived length, or {@code -1} when no option list is available
     */
    private static long maxOptionListLength(Column column) {
        List<String> enumValues = column.enumValues();
        if (enumValues == null || enumValues.isEmpty()) {
            return -1L;
        }
        boolean isSet = MySqlTypeConverter.MYSQL_SET.equalsIgnoreCase(column.typeName());
        long totalLength = 0L;
        long maxLength = 0L;
        for (String enumValue : enumValues) {
            long length = unquotedValueLength(enumValue);
            totalLength += length;
            maxLength = Math.max(maxLength, length);
        }
        return isSet ? totalLength + enumValues.size() - 1L : maxLength;
    }

    /**
     * Returns the number of characters of a member of an option list as it appears in the DDL, so
     * the surrounding quotes and any doubled quote escape are removed before measuring.
     *
     * @param enumValue raw option text collected by the DDL parser
     * @return the stored length of that member
     */
    private static long unquotedValueLength(String enumValue) {
        if (enumValue == null) {
            return 0L;
        }
        if (enumValue.length() >= 2
                && enumValue.charAt(0) == '\''
                && enumValue.charAt(enumValue.length() - 1) == '\'') {
            return enumValue.substring(1, enumValue.length() - 1).replace("''", "'").length();
        }
        return enumValue.length();
    }
}
