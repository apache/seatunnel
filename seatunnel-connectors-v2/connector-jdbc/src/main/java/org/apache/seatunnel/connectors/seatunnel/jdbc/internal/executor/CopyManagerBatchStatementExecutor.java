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
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

public class CopyManagerBatchStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {

    private final String copySql;
    private final TableSchema tableSchema;
    CopyManagerProxy copyManagerProxy;
    CSVFormat csvFormat = CSVFormat.POSTGRESQL_CSV;
    CSVPrinter csvPrinter;

    public CopyManagerBatchStatementExecutor(String copySql, TableSchema tableSchema) {
        this.copySql = copySql;
        this.tableSchema = tableSchema;
    }

    public static void copyManagerProxyChecked(JdbcConnectionProvider connectionProvider) {
        try (Connection connection = connectionProvider.getConnection()) {
            new CopyManagerProxy(connection);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUPPORT_OPERATION_FAILED,
                    "unable to open CopyManager Operation in this JDBC writer. Please configure option use_copy_statement = false.",
                    e);
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CREATE_DRIVER_FAILED, "unable to open JDBC writer", e);
        }
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        try {
            this.copyManagerProxy = new CopyManagerProxy(connection);
            this.csvPrinter = new CSVPrinter(new StringBuilder(), csvFormat);
        } catch (NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException
                | IOException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUPPORT_OPERATION_FAILED,
                    "unable to open CopyManager Operation in this JDBC writer. Please configure option use_copy_statement = false.",
                    e);
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CREATE_DRIVER_FAILED, "unable to open JDBC writer", e);
        }
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        try {
            this.csvPrinter.printRecord(toExtract(record));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Object> toExtract(SeaTunnelRow record) {
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        List<Object> csvRecord = new ArrayList<>();
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
            Object fieldValue = record.getField(fieldIndex);
            if (fieldValue == null) {
                csvRecord.add(null);
                continue;
            }
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    csvRecord.add((String) record.getField(fieldIndex));
                    break;
                case BOOLEAN:
                    csvRecord.add((Boolean) record.getField(fieldIndex));
                    break;
                case TINYINT:
                    csvRecord.add((Byte) record.getField(fieldIndex));
                    break;
                case SMALLINT:
                    csvRecord.add((Short) record.getField(fieldIndex));
                    break;
                case INT:
                    csvRecord.add((Integer) record.getField(fieldIndex));
                    break;
                case BIGINT:
                    csvRecord.add((Long) record.getField(fieldIndex));
                    break;
                case FLOAT:
                    csvRecord.add((Float) record.getField(fieldIndex));
                    break;
                case DOUBLE:
                    csvRecord.add((Double) record.getField(fieldIndex));
                    break;
                case DECIMAL:
                    csvRecord.add((BigDecimal) record.getField(fieldIndex));
                    break;
                case DATE:
                    LocalDate localDate = (LocalDate) record.getField(fieldIndex);
                    csvRecord.add((java.sql.Date) java.sql.Date.valueOf(localDate));
                    break;
                case TIME:
                    LocalTime localTime = (LocalTime) record.getField(fieldIndex);
                    csvRecord.add((java.sql.Time) java.sql.Time.valueOf(localTime));
                    break;
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) record.getField(fieldIndex);
                    csvRecord.add((java.sql.Timestamp) java.sql.Timestamp.valueOf(localDateTime));
                    break;
                case BYTES:
                    csvRecord.add(
                            org.apache.commons.codec.binary.Base64.encodeBase64String(
                                    (byte[]) record.getField(fieldIndex)));
                    break;
                case NULL:
                    csvRecord.add(null);
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return csvRecord;
    }

    @Override
    public void executeBatch() throws SQLException {
        try {
            this.csvPrinter.flush();
            this.copyManagerProxy.doCopy(
                    copySql, new StringReader(this.csvPrinter.getOut().toString()));
        } catch (InvocationTargetException | IllegalAccessException | IOException e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED, "Sql command: " + copySql);
        } finally {
            try {
                this.csvPrinter.close();
                this.csvPrinter = new CSVPrinter(new StringBuilder(), csvFormat);
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        this.copyManagerProxy = null;
        try {
            this.csvPrinter.close();
            this.csvPrinter = null;
        } catch (Exception ignore) {
        }
    }
}
