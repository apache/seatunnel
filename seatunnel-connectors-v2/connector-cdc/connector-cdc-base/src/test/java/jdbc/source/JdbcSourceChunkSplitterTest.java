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

package jdbc.source;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.AbstractJdbcSourceChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JdbcSourceChunkSplitterTest {

    @Test
    void splitColumnTest() throws SQLException {
        TestJdbcSourceChunkSplitter testJdbcSourceChunkSplitter =
                new TestJdbcSourceChunkSplitter(null, new TestSourceDialect());
        Column splitColumn =
                testJdbcSourceChunkSplitter.getSplitColumn(
                        null, new TestSourceDialect(), new TableId("", "", ""));
        Assertions.assertEquals("varchar", splitColumn.typeName());
    }

    @Test
    void splitColumnTestWithUniqueKey() throws SQLException {
        TestJdbcSourceChunkSplitter testJdbcSourceChunkSplitter =
                new TestJdbcSourceChunkSplitter(null, new TestSourceDialectWithUniqueKey());
        Column splitColumn =
                testJdbcSourceChunkSplitter.getSplitColumn(
                        null, new TestSourceDialectWithUniqueKey(), new TableId("", "", ""));
        Assertions.assertEquals("bigint", splitColumn.typeName());
    }

    @Test
    void splitColumnTestWithUniqueKey_2() throws SQLException {
        TestJdbcSourceChunkSplitter testJdbcSourceChunkSplitter =
                new TestJdbcSourceChunkSplitter(null, new TestSourceDialectWithUniqueKey_2());
        Column splitColumn =
                testJdbcSourceChunkSplitter.getSplitColumn(
                        null, new TestSourceDialectWithUniqueKey_2(), new TableId("", "", ""));
        Assertions.assertEquals("int", splitColumn.typeName());
    }

    @Test
    void splitColumnTestWithConfiguredPrimaryKey() throws SQLException {
        JdbcSourceConfig sourceConfig = mock(JdbcSourceConfig.class);
        when(sourceConfig.getSplitColumn()).thenReturn(Collections.singletonMap(".", "bigint_col"));
        TestSourceDialect dialect = new TestSourceDialect();
        TestJdbcSourceChunkSplitter testJdbcSourceChunkSplitter =
                new TestJdbcSourceChunkSplitter(sourceConfig, dialect);

        Column splitColumn =
                testJdbcSourceChunkSplitter.getSplitColumn(null, dialect, new TableId("", "", ""));

        Assertions.assertEquals("bigint", splitColumn.typeName());
    }

    @Test
    void splitColumnSkipsNullableUniqueKey() throws SQLException {
        TestSourceDialectWithNullableUniqueKey dialect =
                new TestSourceDialectWithNullableUniqueKey();
        TestJdbcSourceChunkSplitter testJdbcSourceChunkSplitter =
                new TestJdbcSourceChunkSplitter(null, dialect);

        // NULLs in a unique key never match a chunk range, so no split column is safe here and
        // the table falls back to a single full-scan split.
        Assertions.assertNull(
                testJdbcSourceChunkSplitter.getSplitColumn(null, dialect, new TableId("", "", "")));
    }

    @Test
    void splitColumnPrefersNotNullUniqueKeyOverNullableOne() throws SQLException {
        TestSourceDialectWithNullableAndNotNullUniqueKeys dialect =
                new TestSourceDialectWithNullableAndNotNullUniqueKeys();
        TestJdbcSourceChunkSplitter testJdbcSourceChunkSplitter =
                new TestJdbcSourceChunkSplitter(null, dialect);

        Column splitColumn =
                testJdbcSourceChunkSplitter.getSplitColumn(null, dialect, new TableId("", "", ""));

        Assertions.assertEquals("int", splitColumn.name());
    }

    @Test
    void splitColumnIgnoresConfiguredNullableColumn() throws SQLException {
        JdbcSourceConfig sourceConfig = mock(JdbcSourceConfig.class);
        when(sourceConfig.getSplitColumn())
                .thenReturn(Collections.singletonMap(".", "nullable_int"));
        TestSourceDialectWithNullableAndNotNullUniqueKeys dialect =
                new TestSourceDialectWithNullableAndNotNullUniqueKeys();
        TestJdbcSourceChunkSplitter testJdbcSourceChunkSplitter =
                new TestJdbcSourceChunkSplitter(sourceConfig, dialect);

        Column splitColumn =
                testJdbcSourceChunkSplitter.getSplitColumn(null, dialect, new TableId("", "", ""));

        Assertions.assertEquals("int", splitColumn.name());
    }

    @Test
    void isColumnNullableReadsDatabaseMetadata() throws SQLException {
        TestSourceDialect dialect = new TestSourceDialect();
        TableId tableId = new TableId("db", null, "no_pk");
        // the parsed column says NOT NULL, the database metadata decides
        Column code = intColumn("code", false);

        Assertions.assertTrue(
                dialect.isColumnNullableFromMetadata(
                        jdbcWithColumns(tableId, "code", row("no_pk", "code", "YES")),
                        tableId,
                        code));
        Assertions.assertFalse(
                dialect.isColumnNullableFromMetadata(
                        jdbcWithColumns(tableId, "code", row("no_pk", "code", "NO")),
                        tableId,
                        intColumn("code", true)));
    }

    @Test
    void isColumnNullableFallsBackToParsedColumnWithoutMetadataRow() throws SQLException {
        TestSourceDialect dialect = new TestSourceDialect();
        TableId tableId = new TableId("db", null, "no_pk");

        Assertions.assertTrue(
                dialect.isColumnNullableFromMetadata(
                        jdbcWithColumns(tableId, "code"), tableId, intColumn("code", true)));
        Assertions.assertFalse(
                dialect.isColumnNullableFromMetadata(
                        jdbcWithColumns(tableId, "code"), tableId, intColumn("code", false)));
    }

    @Test
    void isColumnNullableIgnoresRowsMatchedOnlyByNamePattern() throws SQLException {
        TestSourceDialect dialect = new TestSourceDialect();
        TableId tableId = new TableId("db", null, "no_pk");
        // getColumns takes LIKE patterns: `_` also matches the column of table `noXpk`
        JdbcConnection jdbc = jdbcWithColumns(tableId, "code", row("noXpk", "code", "YES"));

        Assertions.assertFalse(
                dialect.isColumnNullableFromMetadata(jdbc, tableId, intColumn("code", false)));
    }

    private static Column intColumn(String name, boolean optional) {
        return Column.editor()
                .name(name)
                .jdbcType(Types.INTEGER)
                .type("int")
                .optional(optional)
                .create();
    }

    private static String[] row(String tableName, String columnName, String isNullable) {
        return new String[] {tableName, columnName, isNullable};
    }

    /** A connection whose metadata returns the given TABLE_NAME, COLUMN_NAME, IS_NULLABLE rows. */
    private static JdbcConnection jdbcWithColumns(
            TableId tableId, String columnName, String[]... rows) throws SQLException {
        Iterator<String[]> iterator = Arrays.asList(rows).iterator();
        AtomicReference<String[]> current = new AtomicReference<>();
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next())
                .thenAnswer(
                        invocation -> {
                            if (!iterator.hasNext()) {
                                return false;
                            }
                            current.set(iterator.next());
                            return true;
                        });
        when(resultSet.getString("TABLE_NAME")).thenAnswer(invocation -> current.get()[0]);
        when(resultSet.getString("COLUMN_NAME")).thenAnswer(invocation -> current.get()[1]);
        when(resultSet.getString("IS_NULLABLE")).thenAnswer(invocation -> current.get()[2]);

        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(metaData.getColumns(tableId.catalog(), tableId.schema(), tableId.table(), columnName))
                .thenReturn(resultSet);
        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(metaData);
        JdbcConnection jdbc = mock(JdbcConnection.class);
        when(jdbc.connection()).thenReturn(connection);
        return jdbc;
    }

    private class TestJdbcSourceChunkSplitter extends AbstractJdbcSourceChunkSplitter {

        public TestJdbcSourceChunkSplitter(
                JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
            super(sourceConfig, dialect);
        }

        @Override
        public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
                throws SQLException {
            return new Object[0];
        }

        @Override
        public Object queryMin(
                JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        public Object[] sampleDataFromColumn(
                JdbcConnection jdbc, TableId tableId, String columnName, int samplingRate)
                throws Exception {
            return new Object[0];
        }

        @Override
        public Object queryNextChunkMax(
                JdbcConnection jdbc,
                TableId tableId,
                String columnName,
                int chunkSize,
                Object includedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
                throws SQLException {
            return null;
        }

        @Override
        public String buildSplitScanQuery(
                Table table,
                SeaTunnelRowType splitKeyType,
                boolean isFirstSplit,
                boolean isLastSplit) {
            return null;
        }

        @Override
        public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
            String typeName = splitColumn.typeName();
            switch (typeName) {
                case "varchar":
                    return BasicType.STRING_TYPE;
                case "tinyint":
                    return BasicType.BYTE_TYPE;
                case "smallint":
                    return BasicType.SHORT_TYPE;
                case "int":
                    return BasicType.INT_TYPE;
                case "bigint":
                    return BasicType.LONG_TYPE;
                case "decimal":
                    return new DecimalType(20, 0);
                default:
                    return BasicType.STRING_TYPE;
            }
        }

        @Override
        public Column getSplitColumn(
                JdbcConnection jdbc, JdbcDataSourceDialect dialect, TableId tableId)
                throws SQLException {
            return super.getSplitColumn(jdbc, dialect, tableId);
        }
    }

    private class TestSourceDialect implements JdbcDataSourceDialect {

        @Override
        public String getName() {
            return null;
        }

        @Override
        public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
            return false;
        }

        @Override
        public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
            return null;
        }

        @Override
        public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
            return null;
        }

        @Override
        public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
            return null;
        }

        @Override
        public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
            return null;
        }

        @Override
        public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {

            Table table =
                    Table.editor()
                            .tableId(tableId)
                            .addColumns(
                                    Column.editor()
                                            .name("string_col")
                                            .jdbcType(Types.VARCHAR)
                                            .type("varchar")
                                            .optional(false)
                                            .create(),
                                    Column.editor()
                                            .name("smallint")
                                            .jdbcType(Types.SMALLINT)
                                            .type("smallint")
                                            .optional(false)
                                            .create(),
                                    Column.editor()
                                            .name("int")
                                            .jdbcType(Types.INTEGER)
                                            .type("int")
                                            .optional(false)
                                            .create(),
                                    Column.editor()
                                            .name("decimal")
                                            .jdbcType(Types.DECIMAL)
                                            .type("decimal")
                                            .optional(false)
                                            .create(),
                                    Column.editor()
                                            .name("tinyint_col")
                                            .jdbcType(Types.TINYINT)
                                            .type("tinyint")
                                            .optional(false)
                                            .create(),
                                    Column.editor()
                                            .name("bigint_col")
                                            .jdbcType(Types.BIGINT)
                                            .type("bigint")
                                            .optional(false)
                                            .create(),
                                    Column.editor()
                                            .name("nullable_int")
                                            .jdbcType(Types.INTEGER)
                                            .type("int")
                                            .optional(true)
                                            .create())
                            .create();
            return new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
        }

        @Override
        public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
            return null;
        }

        @Override
        public JdbcSourceFetchTaskContext createFetchTaskContext(
                SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
            return null;
        }

        @Override
        public Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            return Optional.of(
                    PrimaryKey.of(
                            "pkName",
                            Arrays.asList(
                                    "string_col",
                                    "smallint",
                                    "int",
                                    "decimal",
                                    "tinyint_col",
                                    "bigint_col")));
        }

        @Override
        public List<ConstraintKey> getUniqueKeys(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            return new ArrayList<ConstraintKey>();
        }

        @Override
        public boolean isColumnNullable(
                JdbcConnection jdbcConnection, TableId tableId, Column column) {
            // no database here, the fixture columns carry the nullability
            return column.isOptional();
        }

        /** Runs the default, metadata based implementation of the dialect. */
        boolean isColumnNullableFromMetadata(
                JdbcConnection jdbcConnection, TableId tableId, Column column) throws SQLException {
            return JdbcDataSourceDialect.super.isColumnNullable(jdbcConnection, tableId, column);
        }
    }

    private class TestSourceDialectWithUniqueKey extends TestSourceDialect {

        @Override
        public Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            return Optional.of(PrimaryKey.of("pkName", Arrays.asList("bigint_col")));
        }

        @Override
        public List<ConstraintKey> getUniqueKeys(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            List<ConstraintKey> keys = new ArrayList<>();

            keys.add(
                    ConstraintKey.of(
                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                            "uk_1",
                            Arrays.asList(
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            "string_col", ConstraintKey.ColumnSortType.ASC),
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            "int", ConstraintKey.ColumnSortType.ASC))));

            return keys;
        }
    }

    private class TestSourceDialectWithUniqueKey_2 extends TestSourceDialect {

        @Override
        public Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            return Optional.of(PrimaryKey.of("pkName", Arrays.asList("bigint_col")));
        }

        @Override
        public List<ConstraintKey> getUniqueKeys(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            List<ConstraintKey> keys = new ArrayList<>();

            keys.add(
                    ConstraintKey.of(
                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                            "uk_1",
                            Arrays.asList(
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            "string_col", ConstraintKey.ColumnSortType.ASC))));

            keys.add(
                    ConstraintKey.of(
                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                            "uk_2",
                            Arrays.asList(
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            "int", ConstraintKey.ColumnSortType.ASC),
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            "smallint", ConstraintKey.ColumnSortType.ASC))));

            return keys;
        }
    }

    private class TestSourceDialectWithNullableUniqueKey extends TestSourceDialect {

        @Override
        public Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            return Optional.empty();
        }

        @Override
        public List<ConstraintKey> getUniqueKeys(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            return Collections.singletonList(
                    ConstraintKey.of(
                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                            "uk_nullable",
                            Collections.singletonList(
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            "nullable_int", ConstraintKey.ColumnSortType.ASC))));
        }
    }

    private class TestSourceDialectWithNullableAndNotNullUniqueKeys
            extends TestSourceDialectWithNullableUniqueKey {

        @Override
        public List<ConstraintKey> getUniqueKeys(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            List<ConstraintKey> keys =
                    new ArrayList<>(super.getUniqueKeys(jdbcConnection, tableId));
            keys.add(
                    ConstraintKey.of(
                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                            "uk_not_null",
                            Collections.singletonList(
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            "int", ConstraintKey.ColumnSortType.ASC))));
            return keys;
        }
    }
}
