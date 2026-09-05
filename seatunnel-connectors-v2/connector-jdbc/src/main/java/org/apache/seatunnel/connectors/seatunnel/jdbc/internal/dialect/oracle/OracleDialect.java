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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.SQLUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.StringRangeSplitDecision;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class OracleDialect implements JdbcDialect {

    private static final int DEFAULT_ORACLE_FETCH_SIZE = 128;

    private static final String NLS_SORT = "NLS_SORT";

    private static final String NLS_COMP = "NLS_COMP";

    private static final String BINARY_NLS_VALUE = "BINARY";

    private static final int FIRST_PRINTABLE_ASCII = 32;

    private static final int LAST_PRINTABLE_ASCII = 126;

    private static final String BINARY_COLLATION = "BINARY";

    private static final String USING_NLS_COMP_COLLATION = "USING_NLS_COMP";

    /** Oracle PCTFREE legal range (inclusive). */
    private static final int PCTFREE_MIN = 0;

    private static final int PCTFREE_MAX = 99;

    private static final Set<String> SUPPORTED_TABLE_OPTIONS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(
                            Arrays.asList(
                                    OracleCatalog.TABLE_OPTION_TABLESPACE,
                                    OracleCatalog.TABLE_OPTION_PCTFREE)));

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();
    private final boolean handleBlobAsString;

    public OracleDialect(String fieldIde) {
        this(fieldIde, JdbcCommonOptions.HANDLE_BLOB_AS_STRING.defaultValue());
    }

    public OracleDialect() {
        this(
                FieldIdeEnum.ORIGINAL.getValue(),
                JdbcCommonOptions.HANDLE_BLOB_AS_STRING.defaultValue());
    }

    public OracleDialect(String fieldIde, boolean handleBlobAsString) {
        this.fieldIde = fieldIde;
        this.handleBlobAsString = handleBlobAsString;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new OracleJdbcRowConverter();
    }

    @Override
    public TypeConverter<BasicTypeDefine> getTypeConverter() {
        return new OracleTypeConverter(true, handleBlobAsString);
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "MOD(ORA_HASH(" + quoteIdentifier(fieldName) + ")," + mod + ")";
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new OracleTypeMapper(true, handleBlobAsString);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("\"").append(parts[i]).append("\"").append(".");
            }
            return sb.append("\"")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("\"")
                    .toString();
        }
        return "\"" + getFieldIde(identifier, fieldIde) + "\"";
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(tableName);
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] pkNames) {
        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(pkNames).contains(fieldName))
                        .collect(Collectors.toList());
        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));

        String usingClause = String.format("SELECT %s FROM DUAL", valuesBinding);
        String onConditions =
                Arrays.stream(pkNames)
                        .map(
                                fieldName ->
                                        String.format(
                                                "TARGET.%s=SOURCE.%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "TARGET.%s=SOURCE.%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));
        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "SOURCE." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));

        String upsertSQL =
                String.format(
                        " MERGE INTO %s TARGET"
                                + " USING (%s) SOURCE"
                                + " ON (%s) "
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s)",
                        tableIdentifier(database, tableName),
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_ORACLE_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return quoteIdentifier(tablePath.getSchemaAndTableName());
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {

        // 1. Use select count
        // 2. If no query is configured, use TABLE STATUS.
        // 3. If a query is configured but does not contain a WHERE clause and tablePath is
        // configured, use TABLE STATUS.
        // 4. If a query is configured with a WHERE clause, or a query statement is configured but
        // tablePath is TablePath.DEFAULT, use COUNT(*).

        String query = table.getQuery();

        boolean useTableStats =
                StringUtils.isBlank(query)
                        || (!query.toLowerCase().contains("where")
                                && table.getTablePath() != null
                                && !TablePath.DEFAULT
                                        .getFullName()
                                        .equals(table.getTablePath().getFullName()));

        if (table.getUseSelectCount()) {
            useTableStats = false;
            if (StringUtils.isBlank(query)) {
                query = "SELECT * FROM " + tableIdentifier(table.getTablePath());
            }
        }

        if (useTableStats) {
            TablePath tablePath = table.getTablePath();
            String rowCountQuery =
                    String.format(
                            "select NUM_ROWS from all_tables where OWNER = '%s' AND TABLE_NAME = '%s' ",
                            tablePath.getSchemaName(), tablePath.getTableName());
            try (Statement stmt = connection.createStatement()) {
                String analyzeTable =
                        String.format(
                                "analyze table %s compute statistics for table",
                                tableIdentifier(tablePath));
                if (!table.getSkipAnalyze()) {
                    log.info("Split Chunk, approximateRowCntStatement: {}", analyzeTable);
                    stmt.execute(analyzeTable);
                } else {
                    log.warn("Skip analyze, approximateRowCntStatement: {}", analyzeTable);
                }
                log.info("Split Chunk, approximateRowCntStatement: {}", rowCountQuery);
                try (ResultSet rs = stmt.executeQuery(rowCountQuery)) {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(1);
                }
            }
        }
        return SQLUtils.countForSubquery(connection, query);
    }

    @Override
    public StringRangeSplitDecision validateStringRangeSplit(
            Connection connection, JdbcSourceTable table, String columnName, int sampleSize)
            throws SQLException {
        if (getClass() != OracleDialect.class) {
            return StringRangeSplitDecision.unsafe(
                    "ASCII string range splitting is only validated for the Oracle dialect");
        }
        if (table.getTablePath() == null
                || TablePath.DEFAULT.getFullName().equals(table.getTablePath().getFullName())
                || StringUtils.isBlank(table.getTablePath().getDatabaseName())) {
            return StringRangeSplitDecision.unsafe(
                    "missing physical table path for Oracle string range split validation");
        }
        if (sampleSize <= 0) {
            return StringRangeSplitDecision.unsafe("sample size must be greater than zero");
        }
        StringRangeSplitDecision sessionDecision = validateStringRangeSplitSession(connection);
        if (!sessionDecision.isSafe()) {
            return sessionDecision;
        }
        StringRangeSplitDecision collationDecision =
                validateStringRangeSplitColumnCollation(connection, table, columnName);
        if (!collationDecision.isSafe()) {
            return collationDecision;
        }

        List<String> samples = sampleStringValues(connection, table, columnName, sampleSize);
        if (samples.isEmpty()) {
            return StringRangeSplitDecision.unsafe("no non-null sample values found");
        }
        Integer sampleLength = null;
        for (String sample : samples) {
            int nonPrintableAsciiIndex = findNonPrintableAsciiIndex(sample);
            if (nonPrintableAsciiIndex >= 0) {
                return StringRangeSplitDecision.unsafe(
                        String.format(
                                "sample value of length %s contains a non-printable ASCII character at index %s",
                                sample.length(), nonPrintableAsciiIndex));
            }
            if (sampleLength == null) {
                sampleLength = sample.length();
            } else if (sample.length() != sampleLength) {
                return StringRangeSplitDecision.unsafe(
                        "sample values have variable lengths and cannot preserve string range order");
            }
        }
        return StringRangeSplitDecision.safe(
                String.format(
                        "session %s and %s are binary and %s sampled values are fixed-length printable ASCII",
                        NLS_SORT, NLS_COMP, samples.size()));
    }

    @Override
    public StringRangeSplitDecision validateStringRangeSplitSession(Connection connection)
            throws SQLException {
        if (getClass() != OracleDialect.class) {
            return StringRangeSplitDecision.unsafe(
                    "ASCII string range splitting is only validated for the Oracle dialect");
        }
        Map<String, String> sessionNlsParameters = querySessionNlsParameters(connection);
        String nlsSort = sessionNlsParameters.get(NLS_SORT);
        if (!BINARY_NLS_VALUE.equalsIgnoreCase(nlsSort)) {
            return StringRangeSplitDecision.unsafe(
                    String.format(
                            "session %s must be %s but was %s",
                            NLS_SORT, BINARY_NLS_VALUE, nlsSort));
        }
        String nlsComp = sessionNlsParameters.get(NLS_COMP);
        if (!BINARY_NLS_VALUE.equalsIgnoreCase(nlsComp)) {
            return StringRangeSplitDecision.unsafe(
                    String.format(
                            "session %s must be %s but was %s",
                            NLS_COMP, BINARY_NLS_VALUE, nlsComp));
        }
        StringRangeSplitDecision encodingDecision = validatePrintableAsciiEncoding(connection);
        if (!encodingDecision.isSafe()) {
            return encodingDecision;
        }
        return StringRangeSplitDecision.safe(
                String.format(
                        "session %s and %s are binary and the database preserves printable ASCII binary ordering",
                        NLS_SORT, NLS_COMP));
    }

    @Override
    public boolean supportStringRangeSplit() {
        return getClass() == OracleDialect.class;
    }

    private Map<String, String> querySessionNlsParameters(Connection connection)
            throws SQLException {
        String sql =
                "SELECT PARAMETER, VALUE FROM NLS_SESSION_PARAMETERS "
                        + "WHERE PARAMETER IN ('NLS_SORT', 'NLS_COMP')";
        Map<String, String> parameters = new HashMap<>();
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                parameters.put(resultSet.getString(1), resultSet.getString(2));
            }
        }
        return parameters;
    }

    /**
     * Binary comparison follows database encoding. Verify the complete alphabet used by the Java
     * boundary arithmetic so ASCII-compatible encodings work and EBCDIC encodings fail closed.
     */
    private StringRangeSplitDecision validatePrintableAsciiEncoding(Connection connection)
            throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int asciiCode = FIRST_PRINTABLE_ASCII;
                asciiCode <= LAST_PRINTABLE_ASCII;
                asciiCode++) {
            if (asciiCode > FIRST_PRINTABLE_ASCII) {
                sql.append(", ");
            }
            sql.append("ASCII(?)");
        }
        sql.append(" FROM DUAL");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (int asciiCode = FIRST_PRINTABLE_ASCII;
                    asciiCode <= LAST_PRINTABLE_ASCII;
                    asciiCode++) {
                statement.setString(
                        asciiCode - FIRST_PRINTABLE_ASCII + 1, String.valueOf((char) asciiCode));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return StringRangeSplitDecision.unsafe(
                            "database character set validation returned no result");
                }
                for (int asciiCode = FIRST_PRINTABLE_ASCII;
                        asciiCode <= LAST_PRINTABLE_ASCII;
                        asciiCode++) {
                    if (resultSet.getInt(asciiCode - FIRST_PRINTABLE_ASCII + 1) != asciiCode) {
                        return StringRangeSplitDecision.unsafe(
                                "database character set does not preserve printable ASCII binary ordering");
                    }
                }
            }
        }
        return StringRangeSplitDecision.safe("database character set preserves printable ASCII");
    }

    /**
     * Oracle 12.2+ can attach a data-bound collation to a column, which overrides the session order
     * used by range predicates. Accept only a binary collation or one governed by NLS_COMP.
     */
    private StringRangeSplitDecision validateStringRangeSplitColumnCollation(
            Connection connection, JdbcSourceTable table, String columnName) throws SQLException {
        TablePath tablePath = table.getTablePath();
        String sql =
                "SELECT COLLATION FROM ALL_TAB_COLUMNS "
                        + "WHERE OWNER = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tablePath.getDatabaseName().toUpperCase(Locale.ROOT));
            statement.setString(2, tablePath.getTableName().toUpperCase(Locale.ROOT));
            statement.setString(3, columnName.toUpperCase(Locale.ROOT));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return StringRangeSplitDecision.unsafe(
                            "column collation is unavailable for Oracle string range split validation");
                }
                String collation = resultSet.getString(1);
                if (!BINARY_COLLATION.equalsIgnoreCase(collation)
                        && !USING_NLS_COMP_COLLATION.equalsIgnoreCase(collation)) {
                    return StringRangeSplitDecision.unsafe(
                            String.format("column collation %s is not binary", collation));
                }
            }
        }
        return StringRangeSplitDecision.safe("column collation preserves binary comparison");
    }

    private List<String> sampleStringValues(
            Connection connection, JdbcSourceTable table, String columnName, int sampleSize)
            throws SQLException {
        String quotedColumn = quoteIdentifier(columnName);
        String sql;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sql =
                    String.format(
                            "SELECT %s FROM (SELECT %s FROM (%s) tmp WHERE %s IS NOT NULL ORDER BY %s ASC) WHERE ROWNUM <= %s",
                            quotedColumn,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            quotedColumn,
                            sampleSize);
        } else {
            sql =
                    String.format(
                            "SELECT %s FROM (SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s ASC) WHERE ROWNUM <= %s",
                            quotedColumn,
                            quotedColumn,
                            tableIdentifier(table.getTablePath()),
                            quotedColumn,
                            quotedColumn,
                            sampleSize);
        }

        List<String> samples = new ArrayList<>(sampleSize);
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                String value = resultSet.getString(1);
                if (value != null) {
                    samples.add(value);
                }
            }
        }
        return samples;
    }

    private int findNonPrintableAsciiIndex(String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch < FIRST_PRINTABLE_ASCII || ch > LAST_PRINTABLE_ASCII) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public Object queryNextChunkMax(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quoteIdentifier(columnName);
        String sqlQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM (%s) WHERE %s >= ? ORDER BY %s ASC "
                                    + ") WHERE ROWNUM <= %s",
                            quotedColumn,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            quotedColumn,
                            chunkSize);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC "
                                    + ") WHERE ROWNUM <= %s",
                            quotedColumn,
                            quotedColumn,
                            tableIdentifier(table.getTablePath()),
                            quotedColumn,
                            quotedColumn,
                            chunkSize);
        }

        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    // this should never happen
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", sqlQuery));
                }
                return rs.getObject(1);
            }
        }
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
                            "SELECT %s FROM (%s) T", quoteIdentifier(columnName), table.getQuery());
        } else {
            sampleQuery =
                    String.format(
                            "SELECT %s FROM %s",
                            quoteIdentifier(columnName), tableIdentifier(table.getTablePath()));
        }

        try (PreparedStatement stmt = creatPreparedStatement(connection, sampleQuery, fetchSize)) {
            try (ResultSet rs = stmt.executeQuery()) {
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
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableAddColumnEvent event)
            throws SQLException {
        List<String> ddlSQL = new ArrayList<>();
        ddlSQL.add(buildUpdateColumnSQL(connection, tablePath, event));

        if (event.getColumn().getComment() != null) {
            ddlSQL.add(buildUpdateColumnCommentSQL(tablePath, event.getColumn()));
        }

        try (Statement statement = connection.createStatement()) {
            for (String sql : ddlSQL) {
                log.info("Executing add column SQL: {}", sql);
                statement.execute(sql);
            }
        }
    }

    @Override
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableChangeColumnEvent event)
            throws SQLException {
        List<String> ddlSQL = new ArrayList<>();
        if (event.getOldColumn() != null
                && !(event.getColumn().getName().equals(event.getOldColumn()))) {
            StringBuilder sqlBuilder =
                    new StringBuilder()
                            .append("ALTER TABLE ")
                            .append(tableIdentifier(tablePath))
                            .append(" RENAME COLUMN ")
                            .append(quoteIdentifier(event.getOldColumn()))
                            .append(" TO ")
                            .append(quoteIdentifier(event.getColumn().getName()));
            ddlSQL.add(sqlBuilder.toString());
        }

        try (Statement statement = connection.createStatement()) {
            for (String sql : ddlSQL) {
                log.info("Executing change column SQL: {}", sql);
                statement.execute(sql);
            }
        }

        if (event.getColumn().getDataType() != null) {
            applySchemaChange(
                    connection,
                    tablePath,
                    AlterTableModifyColumnEvent.modify(event.tableIdentifier(), event.getColumn()));
        }
    }

    @Override
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableModifyColumnEvent event)
            throws SQLException {
        List<String> ddlSQL = new ArrayList<>();
        ddlSQL.add(buildUpdateColumnSQL(connection, tablePath, event));

        if (event.getColumn().getComment() != null) {
            ddlSQL.add(buildUpdateColumnCommentSQL(tablePath, event.getColumn()));
        }

        try (Statement statement = connection.createStatement()) {
            for (String sql : ddlSQL) {
                log.info("Executing modify column SQL: {}", sql);
                statement.execute(sql);
            }
        }
    }

    private String buildUpdateColumnSQL(
            Connection connection, TablePath tablePath, AlterTableColumnEvent event)
            throws SQLException {
        String actionType;
        Column column;
        if (event instanceof AlterTableModifyColumnEvent) {
            actionType = "MODIFY";
            column = ((AlterTableModifyColumnEvent) event).getColumn();
        } else if (event instanceof AlterTableAddColumnEvent) {
            actionType = "ADD";
            column = ((AlterTableAddColumnEvent) event).getColumn();
        } else {
            throw new IllegalArgumentException("Unsupported AlterTableColumnEvent: " + event);
        }
        String sourceDialectName = event.getSourceDialectName();
        boolean sameCatalog = StringUtils.equals(dialectName(), sourceDialectName);
        BasicTypeDefine typeDefine = getTypeConverter().reconvert(column);
        String columnType = sameCatalog ? column.getSourceType() : typeDefine.getColumnType();
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE  ")
                        .append(tableIdentifier(tablePath))
                        .append(" ")
                        .append(actionType)
                        .append(" ")
                        .append(quoteIdentifier(column.getName()))
                        .append(" ")
                        .append(columnType);
        // Only decorate with default value when source dialect is same as sink dialect
        // Todo Support for cross-database default values for ddl statements
        if (column.getDefaultValue() != null && sameCatalog) {
            sqlBuilder.append(" ").append(sqlClauseWithDefaultValue(typeDefine, sourceDialectName));
        }
        if (event instanceof AlterTableModifyColumnEvent) {
            boolean targetColumnNullable =
                    columnIsNullable(connection, tablePath, column.getName());
            if (column.isNullable() != targetColumnNullable) {
                sqlBuilder.append(" ").append(column.isNullable() ? "NULL" : "NOT NULL");
            }
        } else {
            sqlBuilder.append(" ").append(column.isNullable() ? "NULL" : "NOT NULL");
        }
        return sqlBuilder.toString();
    }

    private String buildUpdateColumnCommentSQL(TablePath tablePath, Column column) {
        return String.format(
                "COMMENT ON COLUMN %s.%s IS '%s'",
                tableIdentifier(tablePath), quoteIdentifier(column.getName()), column.getComment());
    }

    private boolean columnIsNullable(Connection connection, TablePath tablePath, String column)
            throws SQLException {
        String selectColumnSQL =
                "SELECT"
                        + "        NULLABLE FROM"
                        + "        ALL_TAB_COLUMNS c"
                        + "        WHERE c.owner = '"
                        + tablePath.getSchemaName()
                        + "'"
                        + "        AND c.table_name = '"
                        + tablePath.getTableName()
                        + "'"
                        + "        AND c.column_name = '"
                        + column
                        + "'";
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(selectColumnSQL);
            rs.next();
            return rs.getString("NULLABLE").equals("Y");
        }
    }

    @Override
    public String dualTable() {
        return " FROM dual ";
    }

    @Override
    public String getCollateSql(String collate) {
        if (StringUtils.isNotBlank(collate)) {
            StringBuilder sql = new StringBuilder();
            sql.append("NLSSORT(")
                    .append("char_val")
                    .append(", 'NLS_SORT=")
                    .append(collate)
                    .append("')");
            return sql.toString();
        } else {
            return "char_val";
        }
    }

    @Override
    public void validateTableOptions(Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return;
        }

        Set<String> unsupportedOptions = new LinkedHashSet<>(tableOptions.keySet());
        unsupportedOptions.removeAll(SUPPORTED_TABLE_OPTIONS);
        if (!unsupportedOptions.isEmpty()) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Unsupported JDBC table_options for dialect '%s': %s. Supported keys: %s",
                            dialectName(),
                            String.join(", ", unsupportedOptions),
                            String.join(", ", SUPPORTED_TABLE_OPTIONS)));
        }

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isBlank(value)) {
                throw new JdbcConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "Invalid JDBC table_options for dialect '%s': key '%s' must not be blank",
                                dialectName(), key));
            }
            String trimmed = value.trim();
            if (OracleCatalog.TABLE_OPTION_PCTFREE.equals(key)) {
                validatePctfree(trimmed);
            } else if (OracleCatalog.TABLE_OPTION_TABLESPACE.equals(key)) {
                validateTablespace(trimmed);
            }
        }
    }

    private void validatePctfree(String value) {
        int pctfree;
        try {
            pctfree = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Invalid JDBC table_options for dialect '%s': key '%s' must be an integer between %d and %d, but got '%s'",
                            dialectName(),
                            OracleCatalog.TABLE_OPTION_PCTFREE,
                            PCTFREE_MIN,
                            PCTFREE_MAX,
                            value));
        }
        if (pctfree < PCTFREE_MIN || pctfree > PCTFREE_MAX) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Invalid JDBC table_options for dialect '%s': key '%s' must be an integer between %d and %d, but got '%s'",
                            dialectName(),
                            OracleCatalog.TABLE_OPTION_PCTFREE,
                            PCTFREE_MIN,
                            PCTFREE_MAX,
                            value));
        }
    }

    private void validateTablespace(String value) {
        // Always emitted as "TABLESPACE \"...\"", so reject quote / control chars that break DDL.
        if (value.indexOf('"') >= 0
                || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0
                || value.indexOf(';') >= 0) {
            throw new JdbcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Invalid JDBC table_options for dialect '%s': key '%s' contains illegal characters: '%s'",
                            dialectName(), OracleCatalog.TABLE_OPTION_TABLESPACE, value));
        }
    }
}
