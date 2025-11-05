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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.shade.org.apache.commons.lang3.math.NumberUtils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import org.postgresql.util.PGobject;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_CIDR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_INET;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_MAC_ADDR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_MAC_ADDR8;

@Slf4j
public class PostgresJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String PG_GEOMETRY = "GEOMETRY";
    private static final String PG_GEOGRAPHY = "GEOGRAPHY";

    @Override
    public String converterName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
            OffsetDateTime offsetDateTime = (OffsetDateTime) value;
            try {
                statement.setObject(statementIndex, offsetDateTime);
                return;
            } catch (AbstractMethodError | SQLException e) {
                try {
                    PGobject timestampTzObject = new PGobject();
                    timestampTzObject.setType("timestamptz");
                    timestampTzObject.setValue(offsetDateTime.toString());
                    statement.setObject(statementIndex, timestampTzObject);
                    return;
                } catch (SQLException pge) {
                    try {
                        statement.setTimestamp(
                                statementIndex, Timestamp.from(offsetDateTime.toInstant()));
                        return;
                    } catch (SQLException se) {
                        throw new SQLException(
                                "Failed to set TIMESTAMP_TZ value for PostgreSQL using all methods",
                                se);
                    }
                }
            }
        }
        super.setValueToStatementByDataType(
                value, statement, seaTunnelDataType, statementIndex, sourceType);
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            String metaDataColumnType =
                    rs.getMetaData().getColumnTypeName(resultSetIndex).toUpperCase(Locale.ROOT);
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    if (metaDataColumnType.equals(PG_GEOMETRY)
                            || metaDataColumnType.equals(PG_GEOGRAPHY)) {
                        Object geoObj = rs.getObject(resultSetIndex);
                        fields[fieldIndex] = geoObj == null ? null : geoObj.toString();
                    } else {
                        fields[fieldIndex] = JdbcFieldTypeUtils.getString(rs, resultSetIndex);
                    }
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBoolean(rs, resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getByte(rs, resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getShort(rs, resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getInt(rs, resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getLong(rs, resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getFloat(rs, resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getDouble(rs, resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBigDecimal(rs, resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = JdbcFieldTypeUtils.getDate(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = JdbcFieldTypeUtils.getTime(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case TIMESTAMP_TZ:
                    // Enhanced PostgreSQL TIMESTAMP_TZ handling
                    fields[fieldIndex] = getPostgresOffsetDateTime(rs, resultSetIndex);
                    break;
                case BYTES:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBytes(rs, resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case ARRAY:
                    Array jdbcArray = rs.getArray(resultSetIndex);
                    if (jdbcArray == null) {
                        fields[fieldIndex] = null;
                        break;
                    }

                    Object arrayObject = jdbcArray.getArray();
                    if (((ArrayType) seaTunnelDataType)
                            .getTypeClass()
                            .equals(arrayObject.getClass())) {
                        fields[fieldIndex] = arrayObject;
                    } else {
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                                "Unexpected value: " + seaTunnelDataType.getTypeClass());
                    }
                    break;
                case MAP:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }

    @Override
    public PreparedStatement toExternal(
            TableSchema tableSchema,
            @Nullable TableSchema databaseTableSchema,
            SeaTunnelRow row,
            PreparedStatement statement)
            throws SQLException {
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        String[] sourceTypes =
                tableSchema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(Column::getSourceType)
                        .toArray(String[]::new);
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            try {
                SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
                int statementIndex = fieldIndex + 1;
                Object fieldValue = row.getField(fieldIndex);
                if (fieldValue == null) {
                    statement.setObject(statementIndex, null);
                    continue;
                }

                switch (seaTunnelDataType.getSqlType()) {
                    case STRING:
                        String sourceType = sourceTypes[fieldIndex];
                        if (PG_INET.equalsIgnoreCase(sourceType)
                                || PG_CIDR.equalsIgnoreCase(sourceType)
                                || PG_MAC_ADDR.equalsIgnoreCase(sourceType)
                                || PG_MAC_ADDR8.equalsIgnoreCase(sourceType)) {
                            // handle network address types of postgres
                            PGobject networkTypeObject = new PGobject();
                            networkTypeObject.setType(sourceType);
                            networkTypeObject.setValue(String.valueOf(row.getField(fieldIndex)));
                            statement.setObject(statementIndex, networkTypeObject);
                        } else if (PG_INTERVAL.equalsIgnoreCase(sourceType)) {
                            PGobject intervalObject = new PGobject();
                            intervalObject.setType(PG_INTERVAL);
                            String intervalVal = String.valueOf(row.getField(fieldIndex));
                            if (NumberUtils.isCreatable(intervalVal)) {
                                // postgres interval types are converted to microseconds (long) in
                                // Debezium, so if it is a number,
                                // it is formatted as a postgres interval value.
                                intervalVal = microsecondsToIntervalFormatVal(intervalVal);
                            }
                            intervalObject.setValue(intervalVal);
                            statement.setObject(statementIndex, intervalObject);
                        } else {
                            statement.setString(statementIndex, (String) row.getField(fieldIndex));
                        }
                        break;
                    case BOOLEAN:
                        statement.setBoolean(statementIndex, (Boolean) row.getField(fieldIndex));
                        break;
                    case TINYINT:
                        statement.setByte(statementIndex, (Byte) row.getField(fieldIndex));
                        break;
                    case SMALLINT:
                        statement.setShort(statementIndex, (Short) row.getField(fieldIndex));
                        break;
                    case INT:
                        statement.setInt(statementIndex, (Integer) row.getField(fieldIndex));
                        break;
                    case BIGINT:
                        statement.setLong(statementIndex, (Long) row.getField(fieldIndex));
                        break;
                    case FLOAT:
                        statement.setFloat(statementIndex, (Float) row.getField(fieldIndex));
                        break;
                    case DOUBLE:
                        statement.setDouble(statementIndex, (Double) row.getField(fieldIndex));
                        break;
                    case DECIMAL:
                        statement.setBigDecimal(
                                statementIndex, (BigDecimal) row.getField(fieldIndex));
                        break;
                    case DATE:
                        LocalDate localDate = (LocalDate) row.getField(fieldIndex);
                        statement.setDate(statementIndex, java.sql.Date.valueOf(localDate));
                        break;
                    case TIME:
                        writeTime(statement, statementIndex, (LocalTime) row.getField(fieldIndex));
                        break;
                    case TIMESTAMP:
                        LocalDateTime localDateTime = (LocalDateTime) row.getField(fieldIndex);
                        statement.setTimestamp(
                                statementIndex, java.sql.Timestamp.valueOf(localDateTime));
                        break;
                    case TIMESTAMP_TZ:
                        setValueToStatementByDataType(
                                row.getField(fieldIndex),
                                statement,
                                seaTunnelDataType,
                                statementIndex,
                                sourceTypes[fieldIndex]);
                        break;
                    case BYTES:
                        statement.setBytes(statementIndex, (byte[]) row.getField(fieldIndex));
                        break;
                    case NULL:
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                        break;
                    case ARRAY:
                        SeaTunnelDataType elementType =
                                ((ArrayType) seaTunnelDataType).getElementType();
                        Object[] array = (Object[]) row.getField(fieldIndex);
                        if (array == null) {
                            statement.setNull(statementIndex, java.sql.Types.ARRAY);
                            break;
                        }
                        if (SqlType.TINYINT.equals(elementType.getSqlType())) {
                            Short[] shortArray = new Short[array.length];
                            for (int i = 0; i < array.length; i++) {
                                shortArray[i] = Short.valueOf(array[i].toString());
                            }
                            statement.setObject(statementIndex, shortArray);
                        } else {
                            statement.setObject(statementIndex, array);
                        }
                        break;
                    case MAP:
                    case ROW:
                    default:
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                                "Unexpected value: " + seaTunnelDataType);
                }
            } catch (Exception e) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.DATA_TYPE_CAST_FAILED,
                        "error field:" + rowType.getFieldNames()[fieldIndex],
                        e);
            }
        }
        return statement;
    }

    public String microsecondsToIntervalFormatVal(String intervalVal) {
        Duration duration = Duration.ofNanos(Long.parseLong(intervalVal) * 1000);
        int days = (int) duration.toDays();
        duration = duration.minusDays(days);
        int hours = (int) duration.toHours();
        duration = duration.minusHours(hours);
        int minutes = (int) duration.toMinutes();
        duration = duration.minusMinutes(minutes);
        int seconds = (int) duration.getSeconds();
        StringBuilder sb = new StringBuilder();
        if (days > 0) sb.append(days).append(" days ");
        if (hours > 0) sb.append(hours).append(" hours ");
        if (minutes > 0) sb.append(minutes).append(" minutes ");
        if (seconds > 0) sb.append(seconds).append(" seconds");
        return sb.toString().trim();
    }

    private OffsetDateTime getPostgresOffsetDateTime(ResultSet rs, int columnIndex)
            throws SQLException {
        Object obj = null;
        try {
            obj = rs.getObject(columnIndex);
        } catch (SQLException e) {
            log.debug("Failed to get object from ResultSet at column {}", columnIndex, e);
            try {
                String str = rs.getString(columnIndex);
                if (str != null && !str.trim().isEmpty()) {
                    return parsePostgresTimestampTz(str);
                }
            } catch (SQLException se) {
                log.debug("Failed to get string from ResultSet at column {}", columnIndex, se);
            }
            return null;
        }

        if (obj == null) {
            return null;
        }

        if (obj.getClass().getName().startsWith("org.postgresql.")) {
            try {
                String str = obj.toString();
                if (str != null && !str.isEmpty()) {
                    return parsePostgresTimestampTz(str);
                }
            } catch (Exception e) {
                log.debug(
                        "Failed to parse PostgreSQL timestamp object from string representation",
                        e);
            }
        }

        return JdbcFieldTypeUtils.getOffsetDateTime(rs, columnIndex);
    }

    private OffsetDateTime parsePostgresTimestampTz(String str) {
        if (str == null || str.trim().isEmpty()) {
            return null;
        }

        try {
            String s = str.trim();
            if (s.endsWith(" UTC")) {
                s = s.substring(0, s.length() - 4) + "Z";
            }
            String iso = s.replace(' ', 'T');
            if (iso.matches(".*[+-]\\d{2}$")) {
                iso = iso + ":00";
            } else if (iso.matches(".*[+-]\\d{4}$")) {
                iso = iso.substring(0, iso.length() - 2) + ":" + iso.substring(iso.length() - 2);
            }

            return OffsetDateTime.parse(iso);
        } catch (Exception e) {
            log.debug("Failed to parse PostgreSQL timestamptz string: {}", str, e);
            try {
                String withoutOffset =
                        str.replaceFirst("([+-]\\d{2}:?\\d{2}|\\s+UTC|Z)$", "").trim();
                Timestamp ts = Timestamp.valueOf(withoutOffset);
                return ts.toInstant().atOffset(ZoneOffset.UTC);
            } catch (Exception e2) {
                log.debug("Failed to parse PostgreSQL timestamptz as UTC timestamp: {}", str, e2);
                return null;
            }
        }
    }
}
