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

package org.apache.seatunnel.benchmark.jdbc;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import java.lang.reflect.Proxy;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

/** Provides a positioned result set and a parameter recorder for the common JDBC converter. */
public final class JdbcRowConverterFixture implements AutoCloseable {

    private static final BasicType<?>[] FIELD_TYPES = {
        BasicType.INT_TYPE,
        BasicType.LONG_TYPE,
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.DOUBLE_TYPE
    };
    private static final JDBCType[] COLUMN_TYPES = {
        JDBCType.INTEGER, JDBCType.BIGINT, JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.DOUBLE
    };
    private static final Object[] FIELD_VALUES = {
        1009, 10_000_007_123L, "customer_0291", null, 2.5D
    };

    private final JdbcRowConverter converter =
            new AbstractJdbcRowConverter() {
                @Override
                public String converterName() {
                    return "benchmark";
                }
            };
    private final TableSchema tableSchema;
    private final TableSchema targetTableSchema;
    private final SeaTunnelRow inputRow;
    private final PreparedStatement statement;
    private final CachedRowSet resultSet;

    public JdbcRowConverterFixture(int columns) throws SQLException {
        if (columns < 1) {
            throw new IllegalArgumentException("columns must be positive");
        }

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(columns);
        Object[] fields = new Object[columns];
        for (int column = 0; column < columns; column++) {
            int type = column % FIELD_TYPES.length;
            String name = "c" + column;
            JDBCType columnType = COLUMN_TYPES[type];
            schemaBuilder.column(
                    PhysicalColumn.of(
                            name,
                            FIELD_TYPES[type],
                            64L,
                            true,
                            null,
                            "",
                            columnType.getName(),
                            Collections.emptyMap()));
            metadata.setColumnName(column + 1, name);
            metadata.setColumnType(column + 1, columnType.getVendorTypeNumber());
            fields[column] = FIELD_VALUES[type];
        }
        tableSchema = schemaBuilder.build();
        targetTableSchema = tableSchema.copy();
        inputRow = new SeaTunnelRow(fields);
        resultSet = createResultSet(metadata, fields);

        Object[] parameters = new Object[columns];
        Arrays.fill(parameters, new Object());
        statement = recordingStatement(parameters);
        bindRow();
        if (!Arrays.equals(fields, parameters) || !Arrays.equals(fields, readRow().getFields())) {
            throw new IllegalStateException("JDBC conversion did not preserve the input fields");
        }
    }

    public SeaTunnelRow readRow() throws SQLException {
        return converter.toInternal(resultSet, tableSchema);
    }

    public PreparedStatement bindRow() throws SQLException {
        return converter.toExternal(tableSchema, targetTableSchema, inputRow, statement);
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
    }

    private static CachedRowSet createResultSet(RowSetMetaDataImpl metadata, Object[] fields)
            throws SQLException {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        rowSet.setMetaData(metadata);
        rowSet.moveToInsertRow();
        for (int column = 0; column < fields.length; column++) {
            rowSet.updateObject(column + 1, fields[column]);
        }
        rowSet.insertRow();
        rowSet.moveToCurrentRow();
        rowSet.beforeFirst();
        if (!rowSet.next()) {
            throw new IllegalStateException("No row in the JDBC converter fixture");
        }
        return rowSet;
    }

    private static PreparedStatement recordingStatement(Object[] parameters) {
        return (PreparedStatement)
                Proxy.newProxyInstance(
                        JdbcRowConverterFixture.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "setInt":
                                case "setLong":
                                case "setString":
                                case "setDouble":
                                case "setObject":
                                    parameters[(Integer) args[0] - 1] = args[1];
                                    return null;
                                case "setNull":
                                    parameters[(Integer) args[0] - 1] = null;
                                    return null;
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });
    }
}
