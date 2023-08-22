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

package org.apache.seatunnel.connectors.seatunnel.access.util;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.access.client.AccessClient;
import org.apache.seatunnel.connectors.seatunnel.access.config.AccessParameters;
import org.apache.seatunnel.connectors.seatunnel.access.exception.AccessConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.access.exception.AccessConnectorException;

import org.apache.commons.lang.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.TABLE;

public class TypeConvertUtil {
    public static SeaTunnelDataType<?> convert(String type) {
        switch (type) {
            case "INTEGER":
            case "BIGINT":
                return BasicType.INT_TYPE;
            case "VARCHAR":
                return BasicType.STRING_TYPE;
            case "DECIMAL":
                return BasicType.DOUBLE_TYPE;
            case "TIMESTAMP":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            default:
                throw new AccessConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported this data type: " + type);
        }
    }

    public static Object convertToObject(ResultSet result, String columnName, String columnType) {
        Object value;
        if ("INTEGER".equals(columnType) || "BIGINT".equals(columnType)) {
            try {
                value = result.getInt(columnName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if ("VARCHAR".equals(columnType)) {
            try {
                value = result.getString(columnName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if ("DECIMAL".equals(columnType)) {
            try {
                value = result.getDouble(columnName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if ("TIMESTAMP".equals(columnType)) {
            try {
                value = result.getString(columnName);
                if (null == value) {
                } else {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long timestamp = dateFormat.parse((String) value).getTime();
                    Instant instant = Instant.ofEpochMilli(timestamp);
                    value = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                }
            } catch (SQLException | ParseException e) {
                throw new RuntimeException(e);
            }
        } else if ("BOOLEAN".equals(columnType)) {
            try {
                value = result.getBoolean(columnName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                value = result.getObject(columnName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return value;
    }

    public static void reconvertAndInject(
            PreparedStatement statement, int index, String type, Object fieldValue) {
        switch (type) {
            case "INT":
            case "INTEGER":
                try {
                    statement.setInt(
                            index + 1,
                            null == fieldValue ? 0 : Integer.parseInt(fieldValue.toString()));
                    return;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            case "STRING":
                try {
                    statement.setString(index + 1, null == fieldValue ? "" : fieldValue.toString());
                    return;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            case "DOUBLE":
                try {
                    statement.setDouble(
                            index + 1,
                            null == fieldValue ? 0D : Double.parseDouble(fieldValue.toString()));
                    return;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            case "TIMESTAMP":
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    if (null == fieldValue) {
                        statement.setTimestamp(index + 1, Timestamp.valueOf(LocalDateTime.MIN));
                    } else {
                        Date date = dateFormat.parse(fieldValue.toString().substring(0, 10));
                        statement.setTimestamp(index + 1, new Timestamp(date.getTime()));
                    }
                    return;
                } catch (ParseException | SQLException e) {
                    throw new RuntimeException(e);
                }
            case "BOOLEAN":
                try {
                    statement.setBoolean(
                            index + 1,
                            null == fieldValue ? null : new Boolean(fieldValue.toString()));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
        }
    }

    public static SeaTunnelDataType<?>[] getSeaTunnelDataTypes(
            AccessParameters accessParameters,
            AccessClient accessClient,
            Config pluginConfig,
            String pluginName) {
        String[] sqlColumns;
        List<String> fields = accessParameters.getFields();
        SeaTunnelDataType<?>[] seaTunnelDataTypes;
        try {
            ResultSetMetaData metaData =
                    accessClient.getTableSchema(pluginConfig.getString(TABLE.key()));
            int columnCount = metaData.getColumnCount();
            String columnString =
                    StringUtils.substringBetween(pluginConfig.getString(QUERY.key()), "(", ")");
            sqlColumns = columnString.split(",");
            String[] tableColumns = new String[metaData.getColumnCount()];
            for (int i = 1; i <= columnCount; i++) {
                tableColumns[i - 1] = metaData.getColumnName(i).toUpperCase();
            }
            seaTunnelDataTypes = new SeaTunnelDataType[sqlColumns.length];
            Set<String> tableColumnSet = new HashSet<>(Arrays.asList(tableColumns));
            int tableColumnSetSize = tableColumnSet.size();
            if (fields == null || fields.isEmpty()) {
                for (int j = 0; j < sqlColumns.length; j++) {
                    tableColumnSet.add(sqlColumns[j].trim().toUpperCase());
                    if (tableColumnSetSize == tableColumnSet.size()) {
                        accessParameters.setFields(Arrays.asList(sqlColumns));
                        for (int k = 1; k <= columnCount; k++) {
                            if (metaData.getColumnName(k)
                                    .toUpperCase()
                                    .equals(sqlColumns[j].trim().toUpperCase())) {
                                seaTunnelDataTypes[j] =
                                        TypeConvertUtil.convert(metaData.getColumnTypeName(j + 1));
                            }
                        }
                    } else {
                        throw new AccessConnectorException(
                                AccessConnectorErrorCode.FIELD_NOT_IN_TABLE,
                                "Field "
                                        + sqlColumns[j]
                                        + " does not exist in table "
                                        + pluginConfig.getString(TABLE.key()));
                    }
                }
            }
        } catch (Exception e) {
            throw new AccessConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            pluginName, PluginType.SINK, e));
        }
        return seaTunnelDataTypes;
    }
}
