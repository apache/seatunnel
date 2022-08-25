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

package org.apache.seatunnel.connectors.seatunnel.phoenix.client;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.phoenix.config.PhoenixSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.phoenix.constant.Constant;
import org.apache.seatunnel.connectors.seatunnel.phoenix.constant.NullModeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PhoenixStatementExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixStatementExecutor.class);
    private final PhoenixSinkConfig phoenixWriteConfig;
    private final List<SeaTunnelRow> batch;
    private transient PreparedStatement st;
    private int[] columnTypes;

    public PhoenixStatementExecutor(
            PhoenixSinkConfig phoenixWriteConfig, int[] columnTypes) {
        this.phoenixWriteConfig = phoenixWriteConfig;
        this.batch = new ArrayList<>();
        this.columnTypes = columnTypes;
    }

    public void prepareStatements(Connection connection) throws SQLException {
        this.st = createPreparedStatement(connection);
    }

    public void addToBatch(SeaTunnelRow record) {
        batch.add(record);
    }

    public void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            for (SeaTunnelRow record : batch) {
                setupStatement(st, record);
                st.addBatch();
            }
            st.executeBatch();
            // cache commit to phoenix
            st.getConnection().commit();
            st.clearParameters();
            st.clearBatch();

            batch.clear();
        }
    }

    public void closeStatements() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
    }

    private PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
        StringBuilder columnNamesBuilder = new StringBuilder();
        if (phoenixWriteConfig.isThinClient()) {
            for (String col : phoenixWriteConfig.getColumns()) {
                // thin client does not use double quotes
                columnNamesBuilder.append(col);
                columnNamesBuilder.append(",");
            }
        } else {
            for (String col : phoenixWriteConfig.getColumns()) {
                // Column names use double quotes
                columnNamesBuilder.append("\"");
                columnNamesBuilder.append(col);
                columnNamesBuilder.append("\"");
                columnNamesBuilder.append(",");
            }
        }
        // The table name uses double quotation marks
        columnNamesBuilder.setLength(columnNamesBuilder.length() - 1);
        String columnNames = columnNamesBuilder.toString();
        // Generate upsert template
        String tableName = phoenixWriteConfig.getTableName();
        StringBuilder upsertBuilder = null;
        if (phoenixWriteConfig.isThinClient()) {
            upsertBuilder = new StringBuilder("upsert into " + tableName + " (" + columnNames + " ) values (");
        } else {
            // The table name uses double quotation marks
            upsertBuilder = new StringBuilder("upsert into \"" + tableName + "\" (" + columnNames + " ) values (");
        }
        for (int i = 0; i < phoenixWriteConfig.getColumns().size(); i++) {
            upsertBuilder.append("?,");
        }
        //Remove the extra comma at the end
        upsertBuilder.setLength(upsertBuilder.length() - 1);
        upsertBuilder.append(")");

        String sql = upsertBuilder.toString();
        PreparedStatement ps = connection.prepareStatement(sql);
        LOG.debug("SQL template generated: " + sql);
        return ps;
    }

    private void setupStatement(PreparedStatement upload, SeaTunnelRow row) throws SQLException {
        if (columnTypes != null && columnTypes.length > 0 && columnTypes.length != row.getFields().length) {
            LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
        }
        List<Integer> sinkColumnsIndex = phoenixWriteConfig.getSinkColumnsIndexInRow();
        if (columnTypes == null) {
            // no types provided
            for (int index = 0; index < sinkColumnsIndex.size(); index++) {
                upload.setObject(index + 1, row.getFields()[index]);
            }
        } else {
            // types provided
            for (int i = 0; i < sinkColumnsIndex.size(); i++) {
                int sqlType = columnTypes[i];
                setupColumn(upload, i + 1, sqlType, row.getFields()[i]);
            }
        }
    }

    private void setupColumn(PreparedStatement ps, int pos, int sqlType, Object col) throws SQLException {
        if (col != null) {
            // base on phoenix support datatype, transform seatunnel data
            //refer to https://phoenix.apache.org/language/datatypes.html
            switch (sqlType) {
                case Types.CHAR:
                case Types.VARCHAR:
                    ps.setString(pos, (String) col);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                    ps.setBytes(pos, (byte[]) col);
                    break;

                case Types.BOOLEAN:
                    ps.setBoolean(pos, (Boolean) col);
                    break;

                case Types.TINYINT:
                case Constant.TYPE_UNSIGNED_TINYINT:
                    ps.setByte(pos, ((Long) col).byteValue());
                    break;

                case Types.SMALLINT:
                case Constant.TYPE_UNSIGNED_SMALLINT:
                    ps.setShort(pos, ((Long) col).shortValue());
                    break;

                case Types.INTEGER:
                case Constant.TYPE_UNSIGNED_INTEGER:
                    ps.setInt(pos, ((Long) col).intValue());
                    break;

                case Types.BIGINT:
                case Constant.TYPE_UNSIGNED_LONG:
                    ps.setLong(pos, (Long) col);
                    break;

                case Types.FLOAT:
                case Constant.TYPE_UNSIGNED_FLOAT:
                    ps.setFloat(pos, ((Double) col).floatValue());
                    break;

                case Types.DOUBLE:
                case Constant.TYPE_UNSIGNED_DOUBLE:
                    ps.setDouble(pos, (Double) col);
                    break;

                case Types.DECIMAL:
                    ps.setBigDecimal(pos, (BigDecimal) col);
                    break;
                //Seatunnel LocalTimeType.LocalDate to java.sql.Date
                case Types.DATE:
                case Constant.TYPE_UNSIGNED_DATE:
                    ps.setDate(pos, Date.valueOf((LocalDate) col));
                    break;
                //Seatunnel LocalTimeType.LocalTime to java.sql.Time
                case Types.TIME:
                case Constant.TYPE_UNSIGNED_TIME:
                    ps.setTime(pos, Time.valueOf((LocalTime) col));
                    break;
                //Seatunnel LocalTimeType.LocalDateTime to java.sql.Timestamp
                case Types.TIMESTAMP:
                case Constant.TYPE_UNSIGNED_TIMESTAMP:
                    ps.setTimestamp(pos, Timestamp.valueOf((LocalDateTime) col));
                    break;

                default:
                    throw new RuntimeException("The column type you configured is not supported: " + sqlType);

            }
        } else {
            // If there is no value, it will be handled according to the configuration of null value
            switch (phoenixWriteConfig.getNullMode()){
                case Skip:
                    // Skipping a null value does not insert the column
                    ps.setNull(pos, sqlType);
                    break;

                case Empty:
                    ps.setObject(pos, getEmptyValue(sqlType));
                    break;

                default:
                    throw new RuntimeException(
                            "phoenix sink not this nullMode type: " + phoenixWriteConfig.getNullMode() +
                                    ", currently supported nullMode types are:" + Arrays.asList(NullModeType.values()));
            }
        }
    }

    private static Object getEmptyValue(int sqlType) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
                return "";

            case Types.BOOLEAN:
                return false;

            case Types.TINYINT:
            case Constant.TYPE_UNSIGNED_TINYINT:
                return (byte) 0;

            case Types.SMALLINT:
            case Constant.TYPE_UNSIGNED_SMALLINT:
                return (short) 0;

            case Types.INTEGER:
            case Constant.TYPE_UNSIGNED_INTEGER:
                return 0;

            case Types.BIGINT:
            case Constant.TYPE_UNSIGNED_LONG:
                return (long) 0;

            case Types.FLOAT:
            case Constant.TYPE_UNSIGNED_FLOAT:
                return (float) 0.0;

            case Types.DOUBLE:
            case Constant.TYPE_UNSIGNED_DOUBLE:
                return 0.0d;

            case Types.DECIMAL:
                return new BigDecimal(0);

            case Types.DATE:
            case Constant.TYPE_UNSIGNED_DATE:
                return new java.sql.Date(0);

            case Types.TIME:
            case Constant.TYPE_UNSIGNED_TIME:
                return new java.sql.Time(0);

            case Types.TIMESTAMP:
            case Constant.TYPE_UNSIGNED_TIMESTAMP:
                return new java.sql.Timestamp(0);

            case Types.BINARY:
            case Types.VARBINARY:
                return new byte[0];

            default:
                throw new RuntimeException("The column type you configured is not supported: "
                        + sqlType);
        }
    }
}
