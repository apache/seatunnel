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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
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
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_CIDR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_GEOGRAPHY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_GEOMETRY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_INET;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_MAC_ADDR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_MAC_ADDR8;

@Slf4j
public class PostgresJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final Set<String> BUILTIN_TYPE_NAMES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "text",
                                    "varchar",
                                    "bpchar",
                                    "char",
                                    "name",
                                    "json",
                                    "jsonb",
                                    "uuid",
                                    "xml",
                                    "citext",
                                    "bool",
                                    "boolean",
                                    "int2",
                                    "int4",
                                    "int8",
                                    "integer",
                                    "bigint",
                                    "smallint",
                                    "float4",
                                    "float8",
                                    "numeric",
                                    "decimal",
                                    "bytea",
                                    "date",
                                    "time",
                                    "timetz",
                                    "timestamp",
                                    "timestamptz",
                                    "interval",
                                    "money")));

    private final boolean numericSpecialValueAsNull;

    /**
     * Columns for which a numeric special value has already been logged. The warning is logged once
     * per column, so that a large table with many special values cannot flood the log.
     */
    private final Set<Integer> loggedNumericSpecialValueColumns = ConcurrentHashMap.newKeySet();

    public PostgresJdbcRowConverter() {
        this(false);
    }

    public PostgresJdbcRowConverter(boolean numericSpecialValueAsNull) {
        this.numericSpecialValueAsNull = numericSpecialValueAsNull;
    }

    @Override
    public String converterName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    protected void writeTime(PreparedStatement statement, int index, LocalTime time)
            throws SQLException {
        statement.setObject(index, time);
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
            if (value == null) {
                statement.setNull(statementIndex, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                PGobject timestampTzObject = new PGobject();
                timestampTzObject.setType("timestamptz");
                timestampTzObject.setValue(((OffsetDateTime) value).toString());
                statement.setObject(statementIndex, timestampTzObject);
            }
            return;
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
                    if (PG_GEOMETRY.equalsIgnoreCase(metaDataColumnType)
                            || PG_GEOGRAPHY.equalsIgnoreCase(metaDataColumnType)) {
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
                    fields[fieldIndex] = getDecimalValue(rs, resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = JdbcFieldTypeUtils.getDate(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getLocalTime(rs, resultSetIndex);
                    break;
                case TIMESTAMP:
                    // Use getLocalDateTime() which avoids JVM-default-timezone influence.
                    // See JdbcFieldTypeUtils.getLocalDateTime() for full strategy details.
                    fields[fieldIndex] = JdbcFieldTypeUtils.getLocalDateTime(rs, resultSetIndex);
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

    /**
     * Reads a DECIMAL/NUMERIC column value as {@link BigDecimal}.
     *
     * <p>PostgreSQL allows the special numeric values {@code NaN}, {@code Infinity} and {@code
     * -Infinity} (supported since PostgreSQL 14), which have no {@code BigDecimal} representation.
     * The pgjdbc driver fails to convert them with {@code PSQLException: Bad value for type
     * BigDecimal : NaN} (text protocol) or with a {@code ClassCastException} (binary protocol,
     * where the driver returns a {@code Double}). When {@code numeric_special_value_as_null} is
     * enabled, such values are read as {@code null} and a warning is logged for the first
     * occurrence per column, so that a single special value does not abort the whole job. Otherwise
     * the original error is rethrown, which keeps the default behavior unchanged.
     */
    private BigDecimal getDecimalValue(ResultSet rs, int columnIndex) throws SQLException {
        BigDecimal value =
                JdbcFieldTypeUtils.getBigDecimal(rs, columnIndex, numericSpecialValueAsNull);
        if (value == null
                && numericSpecialValueAsNull
                && JdbcFieldTypeUtils.isNumericSpecialValue(rs, columnIndex)
                && shouldLogNumericSpecialValue(columnIndex)) {
            log.warn(
                    "The numeric special value at column '{}' cannot be represented as BigDecimal, reading it as null. Further special values in this column will not be logged",
                    getColumnLabel(rs, columnIndex));
        }
        return value;
    }

    /**
     * Decides whether the numeric special value at the given column should be logged: the first
     * occurrence per column is logged, the following ones are skipped silently, so that the log
     * cannot be flooded when many rows contain special values.
     */
    boolean shouldLogNumericSpecialValue(int columnIndex) {
        return loggedNumericSpecialValueColumns.add(columnIndex);
    }

    private static String getColumnLabel(ResultSet rs, int columnIndex) {
        try {
            return rs.getMetaData().getColumnLabel(columnIndex);
        } catch (SQLException | RuntimeException e) {
            // The column label is only used to make the warning actionable, the index is enough
            return String.valueOf(columnIndex);
        }
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
                        String sourceType =
                                resolveSourceType(
                                        rowType, fieldIndex, databaseTableSchema, sourceTypes);
                        if (sourceType != null
                                && (PG_GEOMETRY.equalsIgnoreCase(sourceType)
                                        || PG_GEOGRAPHY.equalsIgnoreCase(sourceType))) {
                            // handle PostGIS geometry/geography when represented as string
                            PGobject geometryObject = new PGobject();
                            geometryObject.setType(sourceType.toLowerCase(Locale.ROOT));
                            geometryObject.setValue((String) row.getField(fieldIndex));
                            statement.setObject(statementIndex, geometryObject);
                        } else if (PG_INET.equalsIgnoreCase(sourceType)
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
                            String enumType = postgresUserTypeName(sourceType);
                            if (enumType != null) {
                                PGobject enumObject = new PGobject();
                                enumObject.setType(enumType);
                                enumObject.setValue((String) row.getField(fieldIndex));
                                statement.setObject(statementIndex, enumObject);
                            } else {
                                statement.setString(
                                        statementIndex, (String) row.getField(fieldIndex));
                            }
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
                                resolveSourceType(
                                        rowType, fieldIndex, databaseTableSchema, sourceTypes));
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
                        String arraySourceType =
                                resolveSourceType(
                                        rowType, fieldIndex, databaseTableSchema, sourceTypes);
                        String enumElementType = postgresEnumArrayElementType(arraySourceType);
                        if (enumElementType != null
                                && SqlType.STRING.equals(elementType.getSqlType())) {
                            statement.setArray(
                                    statementIndex,
                                    statement.getConnection()
                                            .createArrayOf(enumElementType, array));
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

    @Nullable private String resolveSourceType(
            SeaTunnelRowType rowType,
            int fieldIndex,
            @Nullable TableSchema databaseTableSchema,
            String[] sourceTypes) {
        if (databaseTableSchema != null) {
            String fieldName = rowType.getFieldName(fieldIndex);
            if (databaseTableSchema.contains(fieldName)) {
                return databaseTableSchema.getColumn(fieldName).getSourceType();
            }
        }
        if (fieldIndex < sourceTypes.length) {
            return sourceTypes[fieldIndex];
        }
        return null;
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
        // Read the value once to avoid drivers returning null on subsequent reads
        final Object obj = rs.getObject(columnIndex);

        if (obj == null) {
            return null;
        }

        // Direct types
        if (obj instanceof OffsetDateTime) {
            return (OffsetDateTime) obj;
        }
        if (obj instanceof java.time.ZonedDateTime) {
            return ((java.time.ZonedDateTime) obj).toOffsetDateTime();
        }
        if (obj instanceof java.util.Date) {
            return ((java.util.Date) obj).toInstant().atOffset(ZoneOffset.UTC);
        }

        // Handle java.sql.Timestamp: avoid using toInstant() directly because the Timestamp
        // was constructed with JVM-default-timezone semantics, which would shift the value.
        // Instead, re-read as string and parse the timezone info explicitly.
        if (obj instanceof Timestamp) {
            String strVal = rs.getString(columnIndex);
            if (strVal == null) {
                return null;
            }
            try {
                return JdbcFieldTypeUtils.parseOffsetDateTimeFromString(strVal);
            } catch (Exception e) {
                // Last resort: fall back to instant-based conversion
                return ((Timestamp) obj).toInstant().atOffset(ZoneOffset.UTC);
            }
        }

        // Remaining PostgreSQL-specific or driver types: fall back to string representation
        return parseTimestampFromObjectString(obj);
    }

    private OffsetDateTime parsePostgresTimestampTz(String str) throws SQLException {
        String normalized = normalizeIsoTimestamp(str);
        if (normalized == null) {
            return null;
        }

        try {
            return OffsetDateTime.parse(normalized);
        } catch (Exception primary) {
            log.debug("Failed to parse PostgreSQL timestamptz as ISO-8601: {}", str, primary);
            try {
                String withoutOffset =
                        normalized.replaceFirst("([+-]\\d{2}:?\\d{2}|\\s+UTC|[zZ])$", "");
                String fallback = withoutOffset.replace('T', ' ').trim();
                Timestamp ts = Timestamp.valueOf(fallback);
                return ts.toInstant().atOffset(ZoneOffset.UTC);
            } catch (Exception secondary) {
                log.debug(
                        "Failed to parse PostgreSQL timestamptz as UTC timestamp: {}",
                        str,
                        secondary);
                throw new SQLException(
                        "Failed to parse PostgreSQL timestamptz string: " + str, secondary);
            }
        }
    }

    @Nullable private OffsetDateTime parseTimestampFromObjectString(Object obj) throws SQLException {
        final String str;
        try {
            str = String.valueOf(obj);
        } catch (Throwable e) {
            log.debug(
                    "Failed to get PostgreSQL timestamp object string representation from class: {}",
                    obj.getClass().getName(),
                    e);
            return null;
        }
        return parsePostgresTimestampTz(str);
    }

    private String normalizeIsoTimestamp(String value) {
        // PostgreSQL timestamptz format examples:
        // "2023-12-25 10:30:45.123456+08:00"
        // "2023-12-25 10:30:45+08"
        // "2023-12-25 10:30:45.123456 UTC"
        String normalized = StringUtils.trimToNull(value);
        if (normalized == null) {
            return null;
        }
        // Handle UTC timezone
        if (normalized.endsWith(" UTC")) {
            normalized = normalized.substring(0, normalized.length() - 4) + "Z";
        }
        // Normalize to ISO-8601 format examples:
        // "2024-01-01T10:15:30+08:00"
        // "2024-01-01T10:15:30Z"
        normalized = normalized.replace(' ', 'T');
        if (!normalized.isEmpty()) {
            char lastChar = normalized.charAt(normalized.length() - 1);
            if (lastChar == 'z' || lastChar == 'Z') {
                normalized = normalized.substring(0, normalized.length() - 1) + "Z";
            }
        }
        // Add colon to offsets like +HH -> +HH:00
        if (normalized.matches(".*[+-]\\d{2}$")) {
            return normalized + ":00";
        }
        if (normalized.matches(".*[+-]\\d{4}$")) {
            // Add colon to offsets like +HHMM -> +HH:MM
            return normalized.substring(0, normalized.length() - 2)
                    + ":"
                    + normalized.substring(normalized.length() - 2);
        }
        return normalized;
    }

    /**
     * Type name for {@code PGobject.setType}, or null when the value stays on {@code setString}.
     *
     * <p>Catalog metadata is {@code format_type}: quotes, spaces, and the schema are part of the
     * identity. pgjdbc lowercases an unquoted name and keeps case only inside quotes, and it
     * resolves {@code schema."type"} on that same string. Stripping the schema makes {@code
     * tenant."text"} look like builtin {@code text}, and it binds the wrong type when two schemas
     * define the same enum name.
     */
    @Nullable
    private static String postgresUserTypeName(String sourceType) {
        if (sourceType == null) {
            return null;
        }
        String trimmed = sourceType.trim();
        String[] parts = splitPgTypeName(trimmed);
        if (parts == null) {
            return null;
        }
        boolean builtinSchema = parts[0] == null || "pg_catalog".equals(parts[0]);
        if (builtinSchema && BUILTIN_TYPE_NAMES.contains(parts[1])) {
            return null;
        }
        return trimmed;
    }

    /**
     * Element type for {@code Connection.createArrayOf}, or null when the array is not an enum.
     * The {@code []} suffix is removed only when it sits outside the quoted name. pgjdbc appends
     * {@code []} again while resolving the array type.
     */
    @Nullable
    private static String postgresEnumArrayElementType(String sourceType) {
        if (sourceType == null) {
            return null;
        }
        String trimmed = sourceType.trim();
        if (!trimmed.endsWith("[]")) {
            return null;
        }
        return postgresUserTypeName(trimmed.substring(0, trimmed.length() - 2).trim());
    }

    /**
     * Schema and type with quotes removed, or null when {@code sourceType} is not one identifier
     * or {@code schema.type}. Index 0 is null when the name has no schema. An unquoted name is
     * folded to lowercase, the way PostgreSQL folds it. A quoted name keeps its case, so {@code
     * "TEXT"} is not builtin {@code text}. A doubled quote inside a quoted identifier is one quote
     * character.
     */
    @Nullable
    private static String[] splitPgTypeName(String sourceType) {
        PgIdentifier first = readPgIdentifier(sourceType, 0);
        if (first == null || first.comparable.isEmpty()) {
            return null;
        }
        if (first.next == sourceType.length()) {
            return new String[] {null, first.comparable};
        }
        if (sourceType.charAt(first.next) != '.') {
            return null;
        }
        PgIdentifier second = readPgIdentifier(sourceType, first.next + 1);
        if (second == null || second.comparable.isEmpty() || second.next != sourceType.length()) {
            return null;
        }
        return new String[] {first.comparable, second.comparable};
    }

    @Nullable
    private static PgIdentifier readPgIdentifier(String value, int start) {
        if (start >= value.length()) {
            return null;
        }
        if (value.charAt(start) == '"') {
            StringBuilder raw = new StringBuilder();
            int index = start + 1;
            while (index < value.length()) {
                char current = value.charAt(index);
                if (current == '"') {
                    if (index + 1 < value.length() && value.charAt(index + 1) == '"') {
                        raw.append('"');
                        index += 2;
                        continue;
                    }
                    return new PgIdentifier(raw.toString(), index + 1);
                }
                raw.append(current);
                index++;
            }
            return null;
        }
        char startChar = value.charAt(start);
        if (!isPgIdentifierStart(startChar)) {
            return null;
        }
        int index = start + 1;
        while (index < value.length() && isPgIdentifierPart(value.charAt(index))) {
            index++;
        }
        return new PgIdentifier(
                value.substring(start, index).toLowerCase(Locale.ROOT), index);
    }

    private static boolean isPgIdentifierStart(char current) {
        return (current >= 'A' && current <= 'Z')
                || (current >= 'a' && current <= 'z')
                || current == '_';
    }

    private static boolean isPgIdentifierPart(char current) {
        return isPgIdentifierStart(current)
                || (current >= '0' && current <= '9')
                || current == '$';
    }

    private static final class PgIdentifier {
        private final String comparable;
        private final int next;

        private PgIdentifier(String comparable, int next) {
            this.comparable = comparable;
            this.next = next;
        }
    }
}
