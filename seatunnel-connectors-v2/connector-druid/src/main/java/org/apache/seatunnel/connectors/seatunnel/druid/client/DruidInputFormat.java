/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.druid.client;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.druid.config.DruidSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.druid.config.DruidTypeMapper;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

@Data
public class DruidInputFormat implements Serializable {
    protected static final String COLUMNS_DEFAULT = "*";
    protected static final String QUERY_TEMPLATE = "SELECT %s FROM %s WHERE 1=1";
    private static final Logger LOGGER = LoggerFactory.getLogger(DruidInputFormat.class);
    protected transient Connection connection;
    protected transient PreparedStatement statement;
    protected transient ResultSet resultSet;
    protected SeaTunnelRowType rowTypeInfo;
    protected DruidSourceOptions druidSourceOptions;
    protected String quarySQL;
    protected boolean hasNext;

    public DruidInputFormat(DruidSourceOptions druidSourceOptions) {
        this.druidSourceOptions = druidSourceOptions;
        this.rowTypeInfo = initTableField();
    }

    public ResultSetMetaData getResultSetMetaData() throws SQLException {
        try {
            quarySQL = getSQL();
            connection = DriverManager.getConnection(druidSourceOptions.getUrl());
            statement = connection.prepareStatement(quarySQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return statement.getMetaData();
        } catch (SQLException se) {
            throw new SQLException("ResultSetMetaData() failed." + se.getMessage(), se);
        }
    }

    public void openInputFormat() {
        try {
            connection = DriverManager.getConnection(druidSourceOptions.getUrl());
            statement = connection.prepareStatement(quarySQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("openInputFormat() failed." + se.getMessage(), se);
        }
    }

    private String getSQL() throws SQLException {
        String columns = COLUMNS_DEFAULT;
        String startTimestamp = druidSourceOptions.getStartTimestamp();
        String endTimestamp = druidSourceOptions.getEndTimestamp();
        String dataSource = druidSourceOptions.getDatasource();
        if (druidSourceOptions.getColumns() != null && druidSourceOptions.getColumns().size() > 0) {
            columns = String.join(",", druidSourceOptions.getColumns());
        }
        String sql = String.format(QUERY_TEMPLATE, columns, dataSource);
        if (startTimestamp != null) {
            sql += " AND __time >=  '" + startTimestamp + "'";
        }
        if (endTimestamp != null) {
            sql += " AND __time <  '" + endTimestamp + "'";
        }
        return sql;
    }

    public void closeInputFormat() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException se) {
            LOGGER.error("DruidInputFormat Statement couldn't be closed", se);
        } finally {
            statement = null;
            resultSet = null;
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException se) {
                LOGGER.error("DruidInputFormat Connection couldn't be closed", se);
            } finally {
                connection = null;
            }
        }
    }

    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    public SeaTunnelRow nextRecord() throws IOException {
        try {
            if (!hasNext) {
                return null;
            }
            SeaTunnelRow seaTunnelRow = toInternal(resultSet, rowTypeInfo);
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return seaTunnelRow;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    public SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType rowTypeInfo) throws SQLException {
        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = rowTypeInfo.getFieldTypes();

        for (int i = 1; i <= seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i - 1];
            if (null == rs.getObject(i)) {
                seatunnelField = null;
            } else if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBoolean(i);
            } else if (BasicType.BYTE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getByte(i);
            } else if (BasicType.SHORT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getShort(i);
            } else if (BasicType.INT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getInt(i);
            } else if (BasicType.LONG_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getLong(i);
            } else if (BasicType.FLOAT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getFloat(i);
            } else if (BasicType.DOUBLE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDouble(i);
            } else if (BasicType.STRING_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getString(i);
            } else if (LocalTimeType.LOCAL_DATE_TIME_TYPE.equals(seaTunnelDataType)) {
                Timestamp ts = rs.getTimestamp(i, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                LocalDateTime localDateTime = LocalDateTime.ofInstant(ts.toInstant(), ZoneId.of("UTC"));  // good
                seatunnelField = localDateTime;
            } else if (LocalTimeType.LOCAL_TIME_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDate(i);
            } else if (LocalTimeType.LOCAL_DATE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDate(i);
            } else {
                throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
            }

            fields.add(seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }

    private SeaTunnelRowType initTableField() {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();

        try {
            ResultSetMetaData resultSetMetaData = getResultSetMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnName(i));
                seaTunnelDataTypes.add(DruidTypeMapper.DRUID_TYPE_MAPPS.get(resultSetMetaData.getColumnTypeName(i)));
            }
        } catch (SQLException e) {
            LOGGER.warn("get row type info exception", e);
        }
        rowTypeInfo = new SeaTunnelRowType(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));

        return rowTypeInfo;
    }
}
