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

package org.apache.seatunnel.connectors.seatunnel.databend.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;
import org.apache.seatunnel.connectors.seatunnel.databend.util.DatabendUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

@Slf4j
public class DatabendSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final DatabendSourceConfig sourceConfig;
    private final String sql;
    private SeaTunnelRowType rowType;
    private final SingleSplitReaderContext readerContext;
    private Connection connection;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private boolean hasNext;
    private SeaTunnelRow firstRow = null;
    private boolean reachEnd;

    public DatabendSourceReader(
            SingleSplitReaderContext context,
            DatabendSourceConfig sourceConfig,
            String sql,
            SeaTunnelRowType rowType) {
        this.readerContext = context;
        this.sourceConfig = sourceConfig;
        this.sql = sql;
        this.rowType = rowType;
        log.info("DatabendSourceReader constructor - rowType: {}", rowType);
    }

    @Override
    public void open() throws Exception {
        log.info("Starting to open DatabendSourceReader");
        try {
            log.info("Loading Databend JDBC driver");
            Class.forName("com.databend.jdbc.DatabendDriver");

            log.info("Connecting to Databend with URL: {}", sourceConfig.getUrl());
            Properties properties = sourceConfig.getProperties();
            connection = DriverManager.getConnection(sourceConfig.getUrl(), properties);
            log.info("Connection to Databend established successfully");

            log.info("Preparing SQL statement: {}", sql);
            statement = connection.prepareStatement(sql);

            Integer fetchSize = sourceConfig.getFetchSize();
            if (fetchSize != null && fetchSize > 0) {
                log.info("Setting fetch size to: {}", fetchSize);
                statement.setFetchSize(fetchSize);
                statement.setFetchDirection(java.sql.ResultSet.FETCH_FORWARD);
            } else {
                log.info("Using default fetch size");
            }

            log.info("Executing query");
            resultSet = statement.executeQuery();
            log.info("Query executed successfully");

            // if rowType is null or empty, infer it from ResultSet metadata
            if (rowType == null || rowType.getFieldNames().length == 0) {
                log.info("Row type is null or empty, inferring from ResultSet metadata");
                rowType = inferRowTypeFromResultSet(resultSet.getMetaData());
                log.info("Inferred row type: {}", rowType);
            } else {
                log.info("Using provided row type: {}", rowType);
            }

            hasNext = resultSet.next();
            log.info("Initial resultSet.next() returned: {}", hasNext);
            if (!hasNext) {
                log.info("No data found in result set");
                reachEnd = true;
            }

        } catch (Exception e) {
            log.error("Error while opening Databend source reader", e);
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to open Databend source reader: " + e.getMessage(),
                    e);
        }
        log.info("DatabendSourceReader opened successfully");
    }

    public SeaTunnelRowType getRowType() {
        return this.rowType;
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (reachEnd) {
            return;
        }

        log.info("Starting to poll data from Databend");
        int rowCount = 0;
        try {
            while (hasNext) {
                SeaTunnelRow row = DatabendUtil.convertToSeaTunnelRow(resultSet, rowType);
                log.info("Converting ResultSet to SeaTunnelRow: {}", row);
                output.collect(row);
                rowCount++;
                log.info("Collected row {}: {}", rowCount, row);
                hasNext = resultSet.next();
                if (!hasNext) {
                    log.info("Reached end of ResultSet after reading {} rows", rowCount);
                    reachEnd = true;
                    // inform the flink reader context that no more elements will be emitted
                    readerContext.signalNoMoreElement();
                    break;
                }
            }
        } catch (Exception e) {
            log.error("Error while polling data from Databend", e);
            throw e;
        }
        log.info("Finished polling data from Databend, total rows: {}", rowCount);
    }

    /** from ResultSetMetaData get SeaTunnelRowType */
    private SeaTunnelRowType inferRowTypeFromResultSet(ResultSetMetaData metaData)
            throws SQLException {
        int columnCount = metaData.getColumnCount();
        String[] fieldNames = new String[columnCount];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[columnCount];

        for (int i = 0; i < columnCount; i++) {
            int columnIndex = i + 1;
            fieldNames[i] = metaData.getColumnLabel(columnIndex);
            fieldTypes[i] =
                    convertDatabendTypeToSeaTunnelType(
                            metaData.getColumnType(columnIndex),
                            metaData.getColumnTypeName(columnIndex),
                            metaData.getPrecision(columnIndex),
                            metaData.getScale(columnIndex));
        }

        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    /** ref: Databend doc: https://docs.databend.com/sql/sql-reference/data-types/ */
    private SeaTunnelDataType<?> convertDatabendTypeToSeaTunnelType(
            int sqlType, String typeName, int precision, int scale) {
        if (typeName != null) {
            typeName = typeName.toUpperCase();

            if (typeName.contains("VARCHAR")
                    || typeName.contains("STRING")
                    || typeName.contains("TEXT")
                    || typeName.contains("CHAR")) {
                return BasicType.STRING_TYPE;
            }

            if (typeName.contains("BOOLEAN") || typeName.equals("BOOL")) {
                return BasicType.BOOLEAN_TYPE;
            }

            if (typeName.equals("TINYINT") || typeName.equals("UINT8") || typeName.equals("INT8")) {
                return BasicType.BYTE_TYPE;
            }
            if (typeName.equals("SMALLINT")
                    || typeName.equals("UINT16")
                    || typeName.equals("INT16")) {
                return BasicType.SHORT_TYPE;
            }
            if (typeName.equals("INT")
                    || typeName.equals("INTEGER")
                    || typeName.equals("UINT32")
                    || typeName.equals("INT32")) {
                return BasicType.INT_TYPE;
            }
            if (typeName.equals("BIGINT")
                    || typeName.equals("UINT64")
                    || typeName.equals("INT64")) {
                return BasicType.LONG_TYPE;
            }

            if (typeName.equals("FLOAT") || typeName.contains("FLOAT32")) {
                return BasicType.FLOAT_TYPE;
            }
            if (typeName.equals("DOUBLE") || typeName.contains("FLOAT64")) {
                return BasicType.DOUBLE_TYPE;
            }

            if (typeName.contains("DECIMAL")) {
                return new DecimalType(precision, scale);
            }

            if (typeName.equals("DATE")) {
                return LocalTimeType.LOCAL_DATE_TYPE;
            }
            if (typeName.equals("TIMESTAMP") || typeName.equals("DATETIME")) {
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            }

            if (typeName.contains("BINARY") || typeName.contains("BLOB")) {
                return PrimitiveByteArrayType.INSTANCE;
            }
        }

        switch (sqlType) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.LONGNVARCHAR:
                return BasicType.STRING_TYPE;

            case Types.TINYINT:
                return BasicType.BYTE_TYPE;

            case Types.SMALLINT:
                return BasicType.SHORT_TYPE;

            case Types.INTEGER:
                return BasicType.INT_TYPE;

            case Types.BIGINT:
                return BasicType.LONG_TYPE;

            case Types.FLOAT:
            case Types.REAL:
                return BasicType.FLOAT_TYPE;

            case Types.DOUBLE:
                return BasicType.DOUBLE_TYPE;

            case Types.BOOLEAN:
            case Types.BIT:
                return BasicType.BOOLEAN_TYPE;

            case Types.DECIMAL:
            case Types.NUMERIC:
                return new DecimalType(precision > 0 ? precision : 38, scale >= 0 ? scale : 18);

            case Types.DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;

            case Types.TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;

            case Types.TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return PrimitiveByteArrayType.INSTANCE;

            default:
                log.warn(
                        "Unsupported SQL type: {}, type name: {}, using STRING_TYPE as fallback",
                        sqlType,
                        typeName);
                return BasicType.STRING_TYPE;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new IOException("Error while closing Databend source reader", e);
        }
    }
}
