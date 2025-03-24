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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.SQLUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class MysqlDialect implements JdbcDialect {

    private static final List NOT_SUPPORTED_DEFAULT_VALUES =
            Arrays.asList(MysqlType.BLOB, MysqlType.TEXT, MysqlType.JSON, MysqlType.GEOMETRY);

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public MysqlDialect() {}

    public MysqlDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.MYSQL;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new MysqlJdbcRowConverter();
    }

    @Override
    public TypeConverter<BasicTypeDefine> getTypeConverter() {
        TypeConverter typeConverter = MySqlTypeConverter.DEFAULT_INSTANCE;
        return typeConverter;
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new MySqlTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + getFieldIde(identifier, fieldIde) + "`";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tableIdentifier(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                                + "=VALUES("
                                                + quoteIdentifier(fieldName)
                                                + ")")
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                getInsertIntoStatement(database, tableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause;
        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE);
        return statement;
    }

    @Override
    public String extractTableName(TablePath tablePath) {
        return tablePath.getTableName();
    }

    @Override
    public Map<String, String> defaultParameter() {
        HashMap<String, String> map = new HashMap<>();
        map.put("rewriteBatchedStatements", "true");
        return map;
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, false);
    }

    @Override
    public Object[] sampleDataFromColumn(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int samplingRate,
            int fetchSize)
            throws Exception {
        String sampleQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sampleQuery =
                    String.format(
                            "SELECT %s FROM (%s) AS T",
                            quoteIdentifier(columnName), table.getQuery());
        } else {
            sampleQuery =
                    String.format(
                            "SELECT %s FROM %s",
                            quoteIdentifier(columnName), tableIdentifier(table.getTablePath()));
        }

        try (Statement stmt =
                connection.createStatement(
                        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rs = stmt.executeQuery(sampleQuery)) {
                int count = 0;
                List<Object> results = new ArrayList<>();

                while (rs.next()) {
                    count++;
                    if (count % samplingRate == 0) {
                        results.add(rs.getObject(1));
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Thread interrupted");
                    }
                }
                Object[] resultsArray = results.toArray();
                Arrays.sort(resultsArray);
                return resultsArray;
            }
        }
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {

        // 1. If no query is configured, use TABLE STATUS.
        // 2. If a query is configured but does not contain a WHERE clause and tablePath is
        // configured , use TABLE STATUS.
        // 3. If a query is configured with a WHERE clause, or a query statement is configured but
        // tablePath is TablePath.DEFAULT, use COUNT(*).

        boolean useTableStats =
                StringUtils.isBlank(table.getQuery())
                        || (!table.getQuery().toLowerCase().contains("where")
                                && table.getTablePath() != null
                                && !TablePath.DEFAULT
                                        .getFullName()
                                        .equals(table.getTablePath().getFullName()));

        if (useTableStats) {
            // The statement used to get approximate row count which is less
            // accurate than COUNT(*), but is more efficient for large table.
            TablePath tablePath = table.getTablePath();
            String useDatabaseStatement =
                    String.format("USE %s;", quoteDatabaseIdentifier(tablePath.getDatabaseName()));
            String rowCountQuery =
                    String.format("SHOW TABLE STATUS LIKE '%s';", tablePath.getTableName());

            try (Statement stmt = connection.createStatement()) {
                log.info("Split Chunk, approximateRowCntStatement: {}", useDatabaseStatement);
                stmt.execute(useDatabaseStatement);
                log.info("Split Chunk, approximateRowCntStatement: {}", rowCountQuery);
                try (ResultSet rs = stmt.executeQuery(rowCountQuery)) {
                    if (!rs.next() || rs.getMetaData().getColumnCount() < 5) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(5);
                }
            }
        }

        return SQLUtils.countForSubquery(connection, table.getQuery());
    }

    @Override
    public boolean supportDefaultValue(BasicTypeDefine typeBasicTypeDefine) {
        MysqlType nativeType = (MysqlType) typeBasicTypeDefine.getNativeType();
        return !(NOT_SUPPORTED_DEFAULT_VALUES.contains(nativeType));
    }

    @Override
    public boolean needsQuotesWithDefaultValue(BasicTypeDefine columnDefine) {
        MysqlType mysqlType = MysqlType.getByName(columnDefine.getColumnType());
        switch (mysqlType) {
            case CHAR:
            case VARCHAR:
            case TEXT:
            case TINYTEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case ENUM:
            case SET:
            case BLOB:
            case TINYBLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case DATE:
            case DATETIME:
            case TIMESTAMP:
            case TIME:
            case YEAR:
                return true;
            default:
                return false;
        }
    }
}
