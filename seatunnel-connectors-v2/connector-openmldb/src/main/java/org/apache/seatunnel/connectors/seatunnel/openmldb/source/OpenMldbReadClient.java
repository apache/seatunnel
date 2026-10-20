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

package org.apache.seatunnel.connectors.seatunnel.openmldb.source;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbSqlExecutor;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.Date;
import com._4paradigm.openmldb.ResultSet;
import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.Schema;
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.sdk.SqlException;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/** Owns the native resources needed to read nullable query results with SDK 0.6.3. */
class OpenMldbReadClient implements AutoCloseable {
    private SQLRouter router;

    OpenMldbReadClient(OpenMldbParameters parameters) throws SqlException {
        router = OpenMldbSqlExecutor.createReader(parameters);
    }

    synchronized Query execute(
            String database, String sql, SeaTunnelRowType rowType, boolean matchByName)
            throws SQLException {
        if (router == null) {
            throw new SQLException("OpenMldb reader is closed");
        }
        Status status = new Status();
        ResultSet result = null;
        try {
            // Query the online tables directly; ExecuteSQL can submit an offline job in cluster
            // mode.
            result = router.ExecuteSQLParameterized(database, sql, null, status);
            if (status.getCode() != 0 || result == null) {
                throw new SQLException("OpenMldb query failed: " + status.getMsg());
            }
            Query query = new Query(result, rowType, matchByName);
            result = null; // Ownership transfers only after schema validation succeeds.
            return query;
        } finally {
            if (result != null) {
                result.delete();
            }
            status.delete();
        }
    }

    @Override
    public synchronized void close() {
        if (router != null) {
            router.delete();
            router = null;
        }
    }

    static class Query implements AutoCloseable {
        private ResultSet result;
        private final SeaTunnelRowType rowType;
        private final int[] columnIndexes;

        private Query(ResultSet result, SeaTunnelRowType rowType, boolean matchByName)
                throws SQLException {
            this.result = result;
            this.rowType = rowType;
            this.columnIndexes = new int[rowType.getTotalFields()];
            Schema schema = result.GetSchema();
            if (schema == null) {
                throw new SQLException("OpenMldb query did not return a schema");
            }
            try {
                if (schema.GetColumnCnt() != rowType.getTotalFields()) {
                    throw new SQLException("Query column count does not match source schema");
                }
                Map<String, Integer> columns = new HashMap<>();
                if (matchByName) {
                    for (int i = 0; i < schema.GetColumnCnt(); i++) {
                        String name = schema.GetColumnName(i);
                        if (columns.put(name, i) != null) {
                            throw new SQLException("Ambiguous query column: " + name);
                        }
                    }
                }
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    Integer index =
                            matchByName ? columns.get(rowType.getFieldName(i)) : Integer.valueOf(i);
                    if (index == null) {
                        throw new SQLException(
                                "Query is missing configured field: " + rowType.getFieldName(i));
                    }
                    columnIndexes[i] = index;
                    if (schema.GetColumnType(columnIndexes[i])
                            != nativeType(rowType.getFieldType(i).getSqlType())) {
                        throw new SQLException(
                                "Query column "
                                        + (i + 1)
                                        + " does not match source schema type "
                                        + rowType.getFieldType(i).getSqlType());
                    }
                }
            } finally {
                schema.delete();
            }
        }

        boolean next() {
            return result.Next();
        }

        SeaTunnelRow readRow() {
            Object[] fields = new Object[rowType.getTotalFields()];
            for (int i = 0; i < fields.length; i++) {
                // JDBC primitive getters return zero/false for NULL, and wasNull is unsupported.
                if (!result.IsNULL(columnIndexes[i])) {
                    fields[i] = readValue(columnIndexes[i], rowType.getFieldType(i).getSqlType());
                }
            }
            return new SeaTunnelRow(fields);
        }

        private Object readValue(int index, SqlType type) {
            switch (type) {
                case BOOLEAN:
                    return result.GetBoolUnsafe(index);
                case SMALLINT:
                    return result.GetInt16Unsafe(index);
                case INT:
                    return result.GetInt32Unsafe(index);
                case BIGINT:
                    return result.GetInt64Unsafe(index);
                case FLOAT:
                    return result.GetFloatUnsafe(index);
                case DOUBLE:
                    return result.GetDoubleUnsafe(index);
                case STRING:
                    return result.GetStringUnsafe(index);
                case DATE:
                    Date date = result.GetStructDateUnsafe(index);
                    try {
                        return LocalDate.of(date.getYear(), date.getMonth(), date.getDay());
                    } finally {
                        date.delete();
                    }
                case TIMESTAMP:
                    return new Timestamp(result.GetTimeUnsafe(index)).toLocalDateTime();
                default:
                    throw new IllegalArgumentException("Unsupported OpenMldb type: " + type);
            }
        }

        @Override
        public void close() {
            if (result != null) {
                result.delete();
                result = null;
            }
        }
    }

    private static DataType nativeType(SqlType type) throws SQLException {
        switch (type) {
            case BOOLEAN:
                return DataType.kTypeBool;
            case SMALLINT:
                return DataType.kTypeInt16;
            case INT:
                return DataType.kTypeInt32;
            case BIGINT:
                return DataType.kTypeInt64;
            case FLOAT:
                return DataType.kTypeFloat;
            case DOUBLE:
                return DataType.kTypeDouble;
            case STRING:
                return DataType.kTypeString;
            case DATE:
                return DataType.kTypeDate;
            case TIMESTAMP:
                return DataType.kTypeTimestamp;
            default:
                throw new SQLException("Unsupported OpenMldb type: " + type);
        }
    }
}
