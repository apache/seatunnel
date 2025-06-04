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

package org.apache.seatunnel.connectors.seatunnel.databend.util;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class DatabendUtil {

    public static final String DRIVER_NAME = "com.databend.jdbc.DatabendDriver";

    static {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to load Databend JDBC driver: " + e.getMessage(),
                    e);
        }
    }

    /** Create a JDBC connection using the provided config */
    public static Connection createConnection(DatabendSourceConfig config) throws SQLException {
        try {
            return DriverManager.getConnection(config.getUrl(), config.getProperties());
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to create connection to Databend: " + e.getMessage(),
                    e);
        }
    }

    /** Create a JDBC connection using the provided config */
    public static Connection createConnection(DatabendSinkConfig config) throws SQLException {
        try {
            return DriverManager.getConnection(config.getUrl(), config.getProperties());
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to create connection to Databend: " + e.getMessage(),
                    e);
        }
    }

    /** Create a JDBC connection using the provided ReadonlyConfig */
    public static Connection createConnection(ReadonlyConfig config) throws SQLException {
        String url = config.get(DatabendOptions.URL);
        Boolean ssl = config.getOptional(DatabendOptions.SSL).orElse(null);
        String username = config.get(DatabendOptions.USERNAME);
        String password = config.get(DatabendOptions.PASSWORD);

        Properties properties = new Properties();
        if (config.getOptional(DatabendOptions.JDBC_CONFIG).isPresent()) {
            Map<String, String> jdbcConfig = config.get(DatabendOptions.JDBC_CONFIG);
            jdbcConfig.forEach(properties::setProperty);
        }

        if (!properties.containsKey("user")) {
            properties.setProperty("user", username);
        }
        if (!properties.containsKey("password")) {
            properties.setProperty("password", password);
        }
        if (ssl != null) {
            properties.setProperty("ssl", ssl.toString());
        }

        try {
            return DriverManager.getConnection(url, properties);
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to create connection to Databend: " + e.getMessage(),
                    e);
        }
    }

    /** Convert a ResultSet row to SeaTunnelRow */
    public static SeaTunnelRow convertToSeaTunnelRow(ResultSet resultSet, SeaTunnelRowType rowType)
            throws SQLException {
        if (resultSet == null) {
            throw new IllegalArgumentException("ResultSet cannot be null");
        }
        if (rowType == null) {
            throw new IllegalArgumentException("RowType cannot be null");
        }

        int arity = rowType.getFieldNames().length;
        Object[] fields = new Object[arity];
        log.info("Converting ResultSet to SeaTunnelRow with {} fields", arity);

        try {
            for (int i = 0; i < arity; i++) {
                int columnIndex = i + 1;
                String fieldName = rowType.getFieldName(i);
                SeaTunnelDataType<?> fieldType = rowType.getFieldType(i);

                try {
                    Object value = getFieldValue(resultSet, columnIndex, fieldType);
                    fields[i] = value;

                    if (value == null) {
                        log.info("Field {} ({}) [{}]: null", i, fieldName, fieldType.getSqlType());
                    } else {
                        log.info(
                                "Field {} ({}) [{}]: {} ({})",
                                i,
                                fieldName,
                                fieldType.getSqlType(),
                                value,
                                value.getClass().getSimpleName());
                    }
                } catch (SQLException e) {
                    log.error("Error getting field {} ({}): {}", i, fieldName, e.getMessage());
                    fields[i] = null;
                }
            }

            SeaTunnelRow row = new SeaTunnelRow(fields);
            return row;
        } catch (Exception e) {
            log.error("Failed to convert ResultSet to SeaTunnelRow: {}", e.getMessage());
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to convert ResultSet to SeaTunnelRow: " + e.getMessage(),
                    e);
        }
    }

    private static Object getFieldValue(
            ResultSet resultSet, int columnIndex, SeaTunnelDataType<?> fieldType)
            throws SQLException {
        try {
            if (fieldType instanceof BasicType) {
                BasicType basicType = (BasicType) fieldType;
                switch (basicType.getSqlType()) {
                    case STRING:
                        return resultSet.getString(columnIndex);
                    case INT:
                        int intValue = resultSet.getInt(columnIndex);
                        return resultSet.wasNull() ? null : intValue;
                    case BIGINT:
                        long longValue = resultSet.getLong(columnIndex);
                        return resultSet.wasNull() ? null : longValue;
                    case FLOAT:
                        float floatValue = resultSet.getFloat(columnIndex);
                        return resultSet.wasNull() ? null : floatValue;
                    case DOUBLE:
                        double doubleValue = resultSet.getDouble(columnIndex);
                        return resultSet.wasNull() ? null : doubleValue;
                    case BOOLEAN:
                        boolean boolValue = resultSet.getBoolean(columnIndex);
                        return resultSet.wasNull() ? null : boolValue;
                    case BYTES:
                        return resultSet.getBytes(columnIndex);
                    default:
                        return resultSet.getObject(columnIndex);
                }
            } else if (fieldType instanceof LocalTimeType) {
                LocalTimeType localTimeType = (LocalTimeType) fieldType;
                switch (localTimeType.getSqlType()) {
                    case DATE:
                        java.sql.Date date = resultSet.getDate(columnIndex);
                        return date == null ? null : date.toLocalDate();
                    case TIME:
                        java.sql.Time time = resultSet.getTime(columnIndex);
                        return time == null ? null : time.toLocalTime();
                    case TIMESTAMP:
                        java.sql.Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                        return timestamp == null ? null : timestamp.toLocalDateTime();
                    default:
                        return resultSet.getObject(columnIndex);
                }
            } else if (fieldType instanceof DecimalType) {
                return resultSet.getBigDecimal(columnIndex);
            } else {
                return resultSet.getObject(columnIndex);
            }
        } catch (SQLException e) {
            log.error(
                    "Error getting field value at index {}, type {}: {}",
                    columnIndex,
                    fieldType.getClass().getSimpleName(),
                    e.getMessage());
            throw e;
        }
    }

    /** Convert a value from Databend type to SeaTunnel type */
    private static Object convertFromDatabendType(
            ResultSet resultSet, int index, SeaTunnelDataType<?> fieldType) throws SQLException {
        switch (fieldType.getSqlType()) {
            case STRING:
                return resultSet.getString(index);
            case BOOLEAN:
                return resultSet.getBoolean(index);
            case TINYINT:
                return resultSet.getByte(index);
            case SMALLINT:
                return resultSet.getShort(index);
            case INT:
                return resultSet.getInt(index);
            case BIGINT:
                return resultSet.getLong(index);
            case FLOAT:
                return resultSet.getFloat(index);
            case DOUBLE:
                return resultSet.getDouble(index);
            case DECIMAL:
                return resultSet.getBigDecimal(index);
            case DATE:
                return resultSet.getDate(index);
            case TIME:
                return resultSet.getTime(index);
            case TIMESTAMP:
                return resultSet.getTimestamp(index);
            case BYTES:
                return resultSet.getBytes(index);
            default:
                return resultSet.getObject(index);
        }
    }

    /** Generate a table exists query */
    public static String generateTableExistsQuery(String database, String table) {
        StringBuilder sql = new StringBuilder("SELECT 1 FROM information_schema.tables WHERE ");
        if (database != null && !database.isEmpty()) {
            sql.append("table_schema = '").append(database).append("' AND ");
        }
        sql.append("table_name = '").append(table).append("' LIMIT 1");
        return sql.toString();
    }

    /** Check if a table exists in Databend */
    public static boolean tableExists(Connection connection, String database, String table)
            throws SQLException {
        String sql = generateTableExistsQuery(database, table);
        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next();
        }
    }

    /** Generate an INSERT SQL for a table */
    public static String generateInsertSql(
            String database, String table, CatalogTable catalogTable) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        String[] fieldNames = rowType.getFieldNames();

        String columns =
                Arrays.stream(fieldNames)
                        .map(name -> "`" + name + "`")
                        .collect(Collectors.joining(", "));

        String placeholders =
                Arrays.stream(fieldNames).map(field -> "?").collect(Collectors.joining(", "));

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        if (database != null && !database.isEmpty()) {
            sqlBuilder.append(database).append(".");
        }
        sqlBuilder.append(table);
        sqlBuilder.append(" (").append(columns).append(") ");
        sqlBuilder.append("VALUES (").append(placeholders).append(")");

        return sqlBuilder.toString();
    }

    /** Get table schema from Databend */
    public static List<String> getTableColumns(Connection connection, String database, String table)
            throws SQLException {
        StringBuilder sql =
                new StringBuilder("SELECT column_name FROM information_schema.columns WHERE ");
        if (database != null && !database.isEmpty()) {
            sql.append("table_schema = '").append(database).append("' AND ");
        }
        sql.append("table_name = '").append(table).append("' ORDER BY ordinal_position");

        List<String> columns = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql.toString());
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                columns.add(resultSet.getString("column_name"));
            }
        }
        return columns;
    }

    /** Close resources quietly */
    public static void closeQuietly(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    log.warn("Error while closing resource: {}", e.getMessage());
                }
            }
        }
    }
}
