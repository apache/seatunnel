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

package org.apache.seatunnel.connectors.cdc.dameng.utils;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DamengConncetionUtils {

    public static Object[] queryMinMax(JdbcConnection jdbc,
                                       TableId tableId,
                                       String columnName) throws SQLException {
        String minMaxQuery = String.format("SELECT MIN(%s), MAX(%s) FROM %s",
            quote(columnName), quote(columnName), quote(tableId));
        JdbcConnection.ResultSetMapper<Object[]> mapper = rs -> {
            rs.next();
            return SourceRecordUtils.rowToArray(rs, 2);
        };
        return jdbc.queryAndMap(minMaxQuery, mapper);
    }

    public static Object queryMin(JdbcConnection jdbc,
                                  TableId tableId,
                                  String columnName,
                                  Object excludedLowerBound) throws SQLException {
        String minQuery = String.format("SELECT MIN(%s) FROM %s WHERE %s > ?",
            quote(columnName), quote(tableId), quote(columnName));
        JdbcConnection.ResultSetMapper<Object> mapper = rs -> {
            rs.next();
            return rs.getObject(1);
        };
        return jdbc.prepareQueryAndMap(minQuery,
            ps -> ps.setObject(1, excludedLowerBound),
            mapper);
    }

    public static Object queryNextChunkMax(JdbcConnection jdbc,
                                           TableId tableId,
                                           String splitColumnName,
                                           int chunkSize,
                                           Object includedLowerBound) throws SQLException {
        String quotedColumn = quote(splitColumnName);
        String query = String.format("SELECT MAX(%s) FROM ("
                + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                + ") AS T",
                quotedColumn,
                quotedColumn,
                quote(tableId),
                quotedColumn,
                quotedColumn,
                chunkSize);
        JdbcConnection.ResultSetMapper<Object> mapper = rs -> {
            rs.next();
            return rs.getObject(1);
        };

        return jdbc.prepareQueryAndMap(query,
            ps -> ps.setObject(1, includedLowerBound),
            mapper);
    }

    public static Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
        String rowCountQuery = String.format("SELECT COUNT(*) FROM %s", quote(tableId));
        return jdbc.queryAndMap(rowCountQuery,
            rs -> {
                rs.next();
                return rs.getLong(1);
            });
    }

    public static String buildSplitQuery(TableId tableId,
                                         SeaTunnelRowType rowType,
                                         boolean isFirstSplit,
                                         boolean isLastSplit) {
        final String condition;
        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            String filterCondition = Arrays.stream(rowType.getFieldNames())
                .map(field -> field + "  <= ? ")
                .collect(Collectors.joining(" AND "));
            String notCondition = Arrays.stream(rowType.getFieldNames())
                .map(field -> field + "  = ? ")
                .collect(Collectors.joining(" AND "));
            condition = String.format("%s AND NOT (%s)", filterCondition, notCondition);
        } else if (isLastSplit) {
            condition = Arrays.stream(rowType.getFieldNames())
                .map(field -> field + "  >= ? ")
                .collect(Collectors.joining(" AND "));
        } else {
            String filterCondition = Stream.concat(
                Arrays.stream(rowType.getFieldNames()).map(field -> field + "  <= ? "),
                Arrays.stream(rowType.getFieldNames()).map(field -> field + "  >= ? "))
                .collect(Collectors.joining(" AND "));
            String notCondition = Arrays.stream(rowType.getFieldNames())
                .map(field -> field + "  = ? ")
                .collect(Collectors.joining(" AND "));
            condition = String.format("%s AND NOT (%s)", filterCondition, notCondition);
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(quote(tableId));
        if (condition != null) {
            sql.append(" WHERE ").append(condition);
        }
        return sql.toString();
    }

    public static PreparedStatement createTableSplitDataStatement(JdbcConnection jdbc,
                                                                  String sql,
                                                                  boolean isFirstSplit,
                                                                  boolean isLastSplit,
                                                                  Object[] splitStart,
                                                                  Object[] splitEnd,
                                                                  int primaryKeyNum,
                                                                  int fetchSize) {
        try {
            Connection connection = jdbc.connection();
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setFetchSize(fetchSize);
            if (isFirstSplit && isLastSplit) {
                return statement;
            }
            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitEnd[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                    statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    @SuppressWarnings("MagicNumber")
    public static List<Column> queryColumns(JdbcConnection jdbc, TableId tableId) throws SQLException {
        String selectColumnSQL = String.format("SELECT " +
                "COLUMN_NAME, " +
                "DATA_TYPE, " +
                "DATA_LENGTH, " +
                "DATA_PRECISION, " +
                "DATA_SCALE, " +
                "NULLABLE, " +
                "COLUMN_ID " +
                "FROM ALL_TAB_COLUMNS WHERE OWNER = ? AND TABLE_NAME = ?",
            tableId.schema(), tableId.table());
        JdbcConnection.StatementPreparer preparer = statement -> {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.table());
        };
        JdbcConnection.ResultSetMapper<List<Column>> mapper = rs -> {
            List<Column> tmp = new ArrayList<>();
            while (rs.next()) {
                Column column = Column.editor()
                    .name(rs.getString(1))
                    .type(rs.getString(2))
                    .length(rs.getInt(3))
                    .scale(rs.getInt(5))
                    .optional("Y".equalsIgnoreCase(rs.getString(6)))
                    .position(rs.getInt(7))
                    .create();
                tmp.add(column);
            }
            return tmp;
        };
        List<Column> columns = jdbc.prepareQueryAndMap(selectColumnSQL, preparer, mapper);

        String selectMetadataSQL = columns.stream()
            .map(c -> c.name())
            .collect(Collectors.joining(
                ",", "SELECT ", " FROM" + quote(tableId) + " LIMIT 1"));
        jdbc.query(selectMetadataSQL, rs -> {
            ResultSetMetaData metadata = rs.getMetaData();
            for (int i = 0; i < columns.size(); i++) {
                Column newColumn = columns.get(i).edit()
                    .jdbcType(metadata.getColumnType(i + 1))
                    .create();
                columns.set(i, newColumn);
            }
        });
        return columns;
    }

    @SuppressWarnings("MagicNumber")
    public static List<String> queryPrimaryKeyNames(JdbcConnection jdbc, TableId tableId) throws SQLException {
        String selectColumnIndexSQL = "SELECT " +
                "C.COLUMN_NAME, " +
                "I.INDEX_NAME, " +
                "I.UNIQUENESS, " +
                "C.DESCEND, " +
                "(CASE WHEN EXISTS (SELECT * FROM ALL_CONSTRAINTS WHERE OWNER = ? AND INDEX_NAME = I.INDEX_NAME AND CONSTRAINT_TYPE = 'P') THEN 1 ELSE 0 END) IS_PK " +
                "FROM ALL_INDEXES I INNER JOIN ALL_IND_COLUMNS C " +
                "ON I.INDEX_NAME = C.INDEX_NAME " +
                "AND C.TABLE_OWNER = ? " +
                "WHERE I.TABLE_OWNER = ? " +
                "AND I.TABLE_NAME = ? " +
                "ORDER BY I.TABLE_NAME, I.INDEX_NAME, C.COLUMN_POSITION";
        JdbcConnection.StatementPreparer preparer = statement -> {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.schema());
            statement.setString(3, tableId.schema());
            statement.setString(4, tableId.table());
        };
        JdbcConnection.ResultSetMapper<List<String>> mapper = rs -> {
            List<String> keys = new ArrayList<>();
            while (rs.next()) {
                String column = rs.getString(1);
                boolean isPK = rs.getBoolean(5);
                if (isPK) {
                    keys.add(column);
                }
            }
            return keys;
        };
        return jdbc.prepareQueryAndMap(selectColumnIndexSQL, preparer, mapper);
    }

    public static String quote(TableId tableId) {
        return tableId.toQuotedString('"');
    }

    public static String quote(String dbOrTableName) {
        return dbOrTableName;
    }
}
