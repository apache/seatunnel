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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/** Tests for composite primary key (multi-column) chunk splitting in the JDBC source. */
public class CompositeKeyChunkSplitterTest {

    private static JdbcSourceConfig config() {
        // Use JdbcSourceConfig.of(...) so useDynamicSplitter defaults to true (the builder
        // default is false unless set explicitly).
        return JdbcSourceConfig.of(
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put("url", "jdbc:mysql://localhost:3306/test");
                                put("driver", "com.mysql.cj.jdbc.Driver");
                            }
                        }));
    }

    private static CatalogTable catalogTable(List<Column> columns, PrimaryKey primaryKey) {
        TableSchema schema = TableSchema.builder().columns(columns).primaryKey(primaryKey).build();
        return CatalogTable.of(
                TableIdentifier.of("db", "schema", "table"),
                schema,
                new HashMap<>(),
                Collections.emptyList(),
                null);
    }

    private static JdbcSourceTable table(CatalogTable catalogTable) {
        return JdbcSourceTable.builder()
                .tablePath(TablePath.of("db", "schema", "table"))
                .catalogTable(catalogTable)
                .build();
    }

    private static List<Column> compositePkColumns() {
        return Arrays.asList(
                PhysicalColumn.builder()
                        .name("order_id")
                        .sourceType("bigint")
                        .dataType(BasicType.LONG_TYPE)
                        .build(),
                PhysicalColumn.builder()
                        .name("line_no")
                        .sourceType("int")
                        .dataType(BasicType.INT_TYPE)
                        .build());
    }

    private static List<Column> singlePkColumn() {
        return Collections.singletonList(
                PhysicalColumn.builder()
                        .name("id")
                        .sourceType("bigint")
                        .dataType(BasicType.LONG_TYPE)
                        .build());
    }

    private static DatabaseMetaData databaseMetaData(int majorVersion) throws SQLException {
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(metaData.getDatabaseMajorVersion()).thenReturn(majorVersion);
        return metaData;
    }

    private static Connection connectionWithMetadata(DatabaseMetaData metaData)
            throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(metaData);
        return connection;
    }

    private static DynamicChunkSplitter splitterWithConnection(
            JdbcSourceConfig config, Connection connection) {
        // The composite-key gate reads the database metadata from the splitter's own connection
        // provider; stub the connection so unit tests never hit a real database.
        return new DynamicChunkSplitter(config) {
            @Override
            protected Connection getOrEstablishConnection() {
                return connection;
            }
        };
    }

    @Test
    public void testFindSplitKeyReturnsAllCompositeKeyColumns() throws SQLException {
        JdbcSourceConfig config = config();
        Assertions.assertTrue(config.isUseDynamicSplitter());

        CatalogTable ct =
                catalogTable(
                        compositePkColumns(),
                        new PrimaryKey("pk", Arrays.asList("order_id", "line_no")));
        JdbcSourceTable table = table(ct);

        DynamicChunkSplitter splitter =
                splitterWithConnection(config, connectionWithMetadata(databaseMetaData(8)));
        Optional<SeaTunnelRowType> splitKey = splitter.findSplitKey(table);

        Assertions.assertTrue(splitKey.isPresent());
        SeaTunnelRowType rowType = splitKey.get();
        Assertions.assertEquals(2, rowType.getTotalFields());
        Assertions.assertArrayEquals(new String[] {"order_id", "line_no"}, rowType.getFieldNames());
        Assertions.assertEquals(BasicType.LONG_TYPE, rowType.getFieldType(0));
        Assertions.assertEquals(BasicType.INT_TYPE, rowType.getFieldType(1));
    }

    @Test
    public void testFindSplitKeyKeepsSingleColumnBehavior() {
        JdbcSourceConfig config = config();
        CatalogTable ct =
                catalogTable(
                        singlePkColumn(), new PrimaryKey("pk", Collections.singletonList("id")));
        JdbcSourceTable table = table(ct);

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        Optional<SeaTunnelRowType> splitKey = splitter.findSplitKey(table);

        Assertions.assertTrue(splitKey.isPresent());
        SeaTunnelRowType rowType = splitKey.get();
        Assertions.assertEquals(1, rowType.getTotalFields());
        Assertions.assertEquals("id", rowType.getFieldName(0));
    }

    @Test
    public void testFindSplitKeyFallsBackToSingleColumnForUnsupportedType() throws SQLException {
        // A composite PK containing a non-splittable type (BOOLEAN here, standing for e.g.
        // BINARY/VARBINARY) must not reach compareArrays; findSplitKey falls back to the
        // first supported PK column.
        JdbcSourceConfig config = config();
        CatalogTable ct =
                catalogTable(
                        Arrays.asList(
                                PhysicalColumn.builder()
                                        .name("order_id")
                                        .sourceType("bigint")
                                        .dataType(BasicType.LONG_TYPE)
                                        .build(),
                                PhysicalColumn.builder()
                                        .name("flag")
                                        .sourceType("boolean")
                                        .dataType(BasicType.BOOLEAN_TYPE)
                                        .build()),
                        new PrimaryKey("pk", Arrays.asList("order_id", "flag")));
        JdbcSourceTable table = table(ct);

        DynamicChunkSplitter splitter =
                splitterWithConnection(config, connectionWithMetadata(databaseMetaData(8)));
        Optional<SeaTunnelRowType> splitKey = splitter.findSplitKey(table);

        Assertions.assertTrue(splitKey.isPresent());
        SeaTunnelRowType rowType = splitKey.get();
        Assertions.assertEquals(1, rowType.getTotalFields());
        Assertions.assertEquals("order_id", rowType.getFieldName(0));
    }

    @Test
    public void testFindSplitKeyFallsBackToSingleColumnForDialectNotOptedIn() throws SQLException {
        // A dialect that has not opted in via supportCompositeKeySplit() (DB2 default false)
        // must keep the pre-PR single-column behavior even for an all-supported composite PK.
        JdbcSourceConfig config =
                JdbcSourceConfig.of(
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("url", "jdbc:db2://localhost:50000/test");
                                        put("driver", "com.ibm.db2.jcc.DB2Driver");
                                    }
                                }));
        CatalogTable ct =
                catalogTable(
                        compositePkColumns(),
                        new PrimaryKey("pk", Arrays.asList("order_id", "line_no")));
        JdbcSourceTable table = table(ct);

        DynamicChunkSplitter splitter =
                splitterWithConnection(config, connectionWithMetadata(databaseMetaData(11)));
        Optional<SeaTunnelRowType> splitKey = splitter.findSplitKey(table);

        Assertions.assertTrue(splitKey.isPresent());
        SeaTunnelRowType rowType = splitKey.get();
        Assertions.assertEquals(1, rowType.getTotalFields());
        Assertions.assertEquals("order_id", rowType.getFieldName(0));
    }

    @Test
    public void testCompositeSplitQuerySQLUsesExpandedTupleConditions() {
        JdbcSourceConfig config = config();
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        TableSchema schema = TableSchema.builder().columns(compositePkColumns()).build();
        String keyName = "order_id,line_no";
        SeaTunnelRowType keyType =
                new SeaTunnelRowType(
                        new String[] {"order_id", "line_no"},
                        new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE, BasicType.INT_TYPE});

        // middle split: (a > ? OR (a = ? AND b > ?)) AND (a < ? OR (a = ? AND b <= ?))
        // AND (a IS NOT NULL AND b IS NOT NULL)
        JdbcSourceSplit middle =
                new JdbcSourceSplit(
                        TablePath.of("db", "schema", "table"),
                        "split-1",
                        null,
                        keyName,
                        keyType,
                        new Object[] {100L, 5},
                        new Object[] {200L, 9});
        String sql = splitter.createDynamicSplitQuerySQL(middle, schema);
        Assertions.assertEquals(
                "SELECT * FROM `db`.`table` "
                        + "WHERE ((`order_id` > ?) OR (`order_id` = ? AND `line_no` > ?)) "
                        + "AND ((`order_id` < ?) OR (`order_id` = ? AND `line_no` <= ?)) "
                        + "AND (`order_id` IS NOT NULL AND `line_no` IS NOT NULL)",
                sql);

        // first split: ((a < ? OR (a = ? AND b <= ?)) OR (a IS NULL OR b IS NULL))
        JdbcSourceSplit first =
                new JdbcSourceSplit(
                        TablePath.of("db", "schema", "table"),
                        "split-0",
                        null,
                        keyName,
                        keyType,
                        null,
                        new Object[] {100L, 5});
        String firstSql = splitter.createDynamicSplitQuerySQL(first, schema);
        Assertions.assertEquals(
                "SELECT * FROM `db`.`table` "
                        + "WHERE (((`order_id` < ?) OR (`order_id` = ? AND `line_no` <= ?)) "
                        + "OR (`order_id` IS NULL OR `line_no` IS NULL))",
                firstSql);

        // last split: a > ? OR (a = ? AND b > ?), plus the IS NOT NULL guard
        JdbcSourceSplit last =
                new JdbcSourceSplit(
                        TablePath.of("db", "schema", "table"),
                        "split-9",
                        null,
                        keyName,
                        keyType,
                        new Object[] {200L, 9},
                        null);
        String lastSql = splitter.createDynamicSplitQuerySQL(last, schema);
        Assertions.assertEquals(
                "SELECT * FROM `db`.`table` "
                        + "WHERE ((`order_id` > ?) OR (`order_id` = ? AND `line_no` > ?)) "
                        + "AND (`order_id` IS NOT NULL AND `line_no` IS NOT NULL)",
                lastSql);
    }

    @Test
    public void testCompositeSplitQuerySQLWithUserQuery() {
        JdbcSourceConfig config = config();
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        TableSchema schema = TableSchema.builder().columns(compositePkColumns()).build();
        SeaTunnelRowType keyType =
                new SeaTunnelRowType(
                        new String[] {"order_id", "line_no"},
                        new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE, BasicType.INT_TYPE});

        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("db", "schema", "table"),
                        "split-1",
                        "select * from src_table",
                        "order_id,line_no",
                        keyType,
                        new Object[] {100L, 5},
                        new Object[] {200L, 9});
        String sql = splitter.createDynamicSplitQuerySQL(split, schema);
        Assertions.assertEquals(
                "SELECT * FROM (select * from src_table) tmp "
                        + "WHERE ((`order_id` > ?) OR (`order_id` = ? AND `line_no` > ?)) "
                        + "AND ((`order_id` < ?) OR (`order_id` = ? AND `line_no` <= ?)) "
                        + "AND (`order_id` IS NOT NULL AND `line_no` IS NOT NULL)",
                sql);
    }

    @Test
    public void testConfigParsingKeepsDynamicSplitterDefault() {
        ReadonlyConfig readonly =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap("url", "jdbc:mysql://localhost:3306/test"));
        JdbcSourceConfig cfg = JdbcSourceConfig.of(readonly);
        Assertions.assertTrue(cfg.isUseDynamicSplitter());
    }

    @Test
    public void testCompositeKeySplitDialectSupport() throws SQLException {
        // Composite split SQL is emitted in portable expanded OR/AND form (no row-value
        // constructor). Each dialect opts in via supportCompositeKeySplit(DatabaseMetaData) only
        // after its composite-PK path is validated by an official E2E; currently MySQL,
        // PostgreSQL, SQLite, SQL Server and Oracle are covered.
        JdbcDialect mysql = JdbcDialectLoader.load("jdbc:mysql://localhost:3306/test", null, null);
        Assertions.assertTrue(mysql.supportCompositeKeySplit(databaseMetaData(8)));
        Assertions.assertEquals(" LIMIT 10", mysql.getLimitClause(10));
        Assertions.assertEquals(" LIMIT 1 OFFSET 9", mysql.getOffsetLimitClause(9, 1));

        JdbcDialect postgres =
                JdbcDialectLoader.load("jdbc:postgresql://localhost:5432/test", null, null);
        Assertions.assertTrue(postgres.supportCompositeKeySplit(databaseMetaData(15)));
        Assertions.assertEquals(" LIMIT 10", postgres.getLimitClause(10));
        Assertions.assertEquals(" LIMIT 1 OFFSET 9", postgres.getOffsetLimitClause(9, 1));

        JdbcDialect sqlite =
                JdbcDialectLoader.load("jdbc:sqlite:/tmp/seatunnel_split_e2e.db", null, null);
        Assertions.assertTrue(sqlite.supportCompositeKeySplit(databaseMetaData(3)));
        Assertions.assertEquals(" LIMIT 10", sqlite.getLimitClause(10));
        Assertions.assertEquals(" LIMIT 1 OFFSET 9", sqlite.getOffsetLimitClause(9, 1));

        JdbcDialect sqlserver =
                JdbcDialectLoader.load("jdbc:sqlserver://localhost:1433", null, null);
        Assertions.assertTrue(sqlserver.supportCompositeKeySplit(databaseMetaData(15)));
        Assertions.assertEquals(
                " OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", sqlserver.getLimitClause(10));
        Assertions.assertEquals(
                " OFFSET 9 ROWS FETCH NEXT 1 ROWS ONLY", sqlserver.getOffsetLimitClause(9, 1));

        JdbcDialect oracle =
                JdbcDialectLoader.load("jdbc:oracle:thin:@localhost:1521:xe", null, null);
        Assertions.assertTrue(oracle.supportCompositeKeySplit(databaseMetaData(23)));
        Assertions.assertEquals(" FETCH FIRST 10 ROWS ONLY", oracle.getLimitClause(10));
        Assertions.assertEquals(
                " OFFSET 9 ROWS FETCH NEXT 1 ROWS ONLY", oracle.getOffsetLimitClause(9, 1));
    }

    @Test
    public void testOracleCompositeSplitGatedOnDatabaseVersion() throws SQLException {
        // The composite boundary queries use FETCH FIRST / OFFSET ... FETCH NEXT, which Oracle
        // only parses from 12c onwards; on older releases (e.g. 11g) the dialect must decline so
        // the splitter falls back to the single-column path instead of failing job startup.
        JdbcDialect oracle =
                JdbcDialectLoader.load("jdbc:oracle:thin:@localhost:1521:xe", null, null);
        Assertions.assertFalse(
                oracle.supportCompositeKeySplit(databaseMetaData(11)),
                "Oracle 11g must fall back to single-column split");
        Assertions.assertTrue(
                oracle.supportCompositeKeySplit(databaseMetaData(12)),
                "Oracle 12c supports the composite boundary SQL");
        Assertions.assertTrue(
                oracle.supportCompositeKeySplit(databaseMetaData(23)),
                "Oracle 23ai (used by JdbcOracleSplitIT) supports the composite boundary SQL");
    }

    @Test
    public void testVersionIndependentDialectsIgnoreMetadata() throws SQLException {
        // MySQL, PostgreSQL, SQLite and SQL Server have no version-gated composite SQL; their
        // opt-in must be unchanged regardless of the reported database version.
        JdbcDialect mysql = JdbcDialectLoader.load("jdbc:mysql://localhost:3306/test", null, null);
        Assertions.assertTrue(mysql.supportCompositeKeySplit(databaseMetaData(1)));
        Assertions.assertTrue(mysql.supportCompositeKeySplit(databaseMetaData(99)));

        JdbcDialect postgres =
                JdbcDialectLoader.load("jdbc:postgresql://localhost:5432/test", null, null);
        Assertions.assertTrue(postgres.supportCompositeKeySplit(databaseMetaData(1)));
        Assertions.assertTrue(postgres.supportCompositeKeySplit(databaseMetaData(99)));

        JdbcDialect sqlite =
                JdbcDialectLoader.load("jdbc:sqlite:/tmp/seatunnel_split_e2e.db", null, null);
        Assertions.assertTrue(sqlite.supportCompositeKeySplit(databaseMetaData(1)));
        Assertions.assertTrue(sqlite.supportCompositeKeySplit(databaseMetaData(99)));

        JdbcDialect sqlserver =
                JdbcDialectLoader.load("jdbc:sqlserver://localhost:1433", null, null);
        Assertions.assertTrue(sqlserver.supportCompositeKeySplit(databaseMetaData(1)));
        Assertions.assertTrue(sqlserver.supportCompositeKeySplit(databaseMetaData(99)));
    }

    @Test
    public void testFindSplitKeyFallsBackWhenMetadataUnavailable() throws SQLException {
        // If reading the database metadata fails, the composite gate must degrade to "not
        // supported" (single-column split) instead of failing job startup.
        JdbcSourceConfig config = config();
        CatalogTable ct =
                catalogTable(
                        compositePkColumns(),
                        new PrimaryKey("pk", Arrays.asList("order_id", "line_no")));
        JdbcSourceTable table = table(ct);

        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenThrow(new SQLException("metadata unavailable"));
        DynamicChunkSplitter splitter = splitterWithConnection(config, connection);
        Optional<SeaTunnelRowType> splitKey = splitter.findSplitKey(table);

        Assertions.assertTrue(splitKey.isPresent());
        SeaTunnelRowType rowType = splitKey.get();
        Assertions.assertEquals(1, rowType.getTotalFields());
        Assertions.assertEquals("order_id", rowType.getFieldName(0));
    }

    @Test
    public void testFindSplitKeyFallsBackWhenConnectionAcquisitionFails() {
        // Same as above, but failing one step earlier: the connection needed for the metadata
        // check cannot be established, so the splitter must still fall back to one column.
        JdbcSourceConfig config = config();
        CatalogTable ct =
                catalogTable(
                        compositePkColumns(),
                        new PrimaryKey("pk", Arrays.asList("order_id", "line_no")));
        JdbcSourceTable table = table(ct);

        DynamicChunkSplitter splitter =
                new DynamicChunkSplitter(config) {
                    @Override
                    protected Connection getOrEstablishConnection() throws SQLException {
                        throw new SQLException("connection failed");
                    }
                };
        Optional<SeaTunnelRowType> splitKey = splitter.findSplitKey(table);

        Assertions.assertTrue(splitKey.isPresent());
        SeaTunnelRowType rowType = splitKey.get();
        Assertions.assertEquals(1, rowType.getTotalFields());
        Assertions.assertEquals("order_id", rowType.getFieldName(0));
    }

    @Test
    public void testCompareCompositeElementMixedNumericTypes() {
        // SQLite JDBC (and others) may return Integer for small values and Long for large values
        // of the same INTEGER column; raw Comparable.compareTo would throw ClassCastException.
        Assertions.assertTrue(DynamicChunkSplitter.compareCompositeElement(5, 7L) < 0);
        Assertions.assertTrue(DynamicChunkSplitter.compareCompositeElement(5L, 7) < 0);
        Assertions.assertEquals(0, DynamicChunkSplitter.compareCompositeElement(300, 300L));
        Assertions.assertEquals(0, DynamicChunkSplitter.compareCompositeElement(1, 1.0d));
        Assertions.assertEquals(0, DynamicChunkSplitter.compareCompositeElement(0.5f, 0.5d));
        Assertions.assertTrue(
                DynamicChunkSplitter.compareCompositeElement(Integer.MAX_VALUE, Long.MAX_VALUE)
                        < 0);
        Assertions.assertTrue(
                DynamicChunkSplitter.compareCompositeElement(new java.math.BigDecimal("1.5"), 2)
                        < 0);
        // BigDecimal of different scales but equal values compare equal
        Assertions.assertEquals(
                0,
                DynamicChunkSplitter.compareCompositeElement(
                        new java.math.BigDecimal("1.0"), new java.math.BigDecimal("1.00")));
        // non-numeric types keep the plain Comparable behavior
        Assertions.assertTrue(DynamicChunkSplitter.compareCompositeElement("abc", "abd") < 0);
        Assertions.assertEquals(0, DynamicChunkSplitter.compareCompositeElement("abc", "abc"));
    }

    @Test
    public void testCompareCompositeElementNullSortsFirst() {
        Assertions.assertEquals(0, DynamicChunkSplitter.compareCompositeElement(null, null));
        Assertions.assertTrue(DynamicChunkSplitter.compareCompositeElement(null, 1) < 0);
        Assertions.assertTrue(DynamicChunkSplitter.compareCompositeElement(1, null) > 0);
    }

    @Test
    public void testCompareNumericNonFiniteFloatingPoint() {
        // Double.compare semantics: NaN is greater than everything (including Infinity).
        Assertions.assertTrue(DynamicChunkSplitter.compareNumeric(Double.NaN, 1.0d) > 0);
        Assertions.assertTrue(DynamicChunkSplitter.compareNumeric(1.0d, Double.NaN) < 0);
        Assertions.assertEquals(0, DynamicChunkSplitter.compareNumeric(Double.NaN, Double.NaN));
        Assertions.assertTrue(
                DynamicChunkSplitter.compareNumeric(Double.POSITIVE_INFINITY, Double.MAX_VALUE)
                        > 0);
        Assertions.assertEquals(
                0,
                DynamicChunkSplitter.compareNumeric(
                        Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        Assertions.assertEquals(0, DynamicChunkSplitter.compareNumeric(3, 3.0d));
        Assertions.assertTrue(DynamicChunkSplitter.compareNumeric(2, 10L) < 0);
    }

    @Test
    public void testMiddleAndLastSplitsExcludeNullKeyComponents() {
        // The first chunk's read predicate must capture rows whose composite key contains a NULL
        // component (which the tuple comparisons alone would silently drop) via an explicit
        // (col IS NULL OR ...) disjunct, while middle and last chunk predicates must EXPLICITLY
        // exclude those rows with an (col IS NOT NULL AND ...) guard: without the guard, a row
        // with a NULL non-leading key component whose leading-column value falls strictly inside
        // the chunk's boundary range satisfies the expanded condition on the leading column alone
        // (e.g. `col1 > ?` is TRUE regardless of col2 IS NULL) and would be read twice — once by
        // the first chunk's NULL disjunct and once by this chunk (data duplication).
        JdbcSourceConfig config = config();
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        TableSchema schema = TableSchema.builder().columns(compositePkColumns()).build();
        SeaTunnelRowType keyType =
                new SeaTunnelRowType(
                        new String[] {"order_id", "line_no"},
                        new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE, BasicType.INT_TYPE});
        String notNullGuard = "(`order_id` IS NOT NULL AND `line_no` IS NOT NULL)";

        JdbcSourceSplit first =
                new JdbcSourceSplit(
                        TablePath.of("db", "schema", "table"),
                        "split-0",
                        null,
                        "order_id,line_no",
                        keyType,
                        null,
                        new Object[] {100L, 5});
        String firstSql =
                splitter.createDynamicSplitQuerySQL(first, schema)
                        .replace("SELECT * FROM `db`.`table` WHERE ", "");
        // The first split must capture NULL key components via the IS NULL disjunct...
        Assertions.assertTrue(
                firstSql.contains("(`order_id` IS NULL OR `line_no` IS NULL)"),
                "First split must capture NULL key components, got: " + firstSql);
        // ...and must NOT carry the IS NOT NULL guard (it would exclude the very rows the NULL
        // disjunct is supposed to capture, silently dropping them again).
        Assertions.assertFalse(
                firstSql.contains("IS NOT NULL"),
                "First split must not exclude NULL key components, got: " + firstSql);

        JdbcSourceSplit middle =
                new JdbcSourceSplit(
                        TablePath.of("db", "schema", "table"),
                        "split-1",
                        null,
                        "order_id,line_no",
                        keyType,
                        new Object[] {100L, 5},
                        new Object[] {200L, 9});
        String middleSql =
                splitter.createDynamicSplitQuerySQL(middle, schema)
                        .replace("SELECT * FROM `db`.`table` WHERE ", "");
        // The guard must name every composite key column, not just the leading one.
        Assertions.assertTrue(
                middleSql.contains(notNullGuard),
                "Middle split must exclude NULL key components with an IS NOT NULL guard on every "
                        + "key column, got: "
                        + middleSql);
        Assertions.assertFalse(
                middleSql.contains("IS NULL"),
                "Middle split must not capture NULL rows, got: " + middleSql);

        JdbcSourceSplit last =
                new JdbcSourceSplit(
                        TablePath.of("db", "schema", "table"),
                        "split-2",
                        null,
                        "order_id,line_no",
                        keyType,
                        new Object[] {200L, 9},
                        null);
        String lastSql =
                splitter.createDynamicSplitQuerySQL(last, schema)
                        .replace("SELECT * FROM `db`.`table` WHERE ", "");
        Assertions.assertTrue(
                lastSql.contains(notNullGuard),
                "Last split must exclude NULL key components with an IS NOT NULL guard on every "
                        + "key column, got: "
                        + lastSql);
        Assertions.assertFalse(
                lastSql.contains("IS NULL"),
                "Last split must not capture NULL rows, got: " + lastSql);
    }

    @Test
    public void testPartitionColumnOptsOutOfCompositeSplit() {
        // Setting an explicit partition_column keeps the previous single-column split behavior:
        // the explicit column takes precedence over the composite primary key branch.
        JdbcSourceConfig config = config();
        CatalogTable ct =
                catalogTable(
                        compositePkColumns(),
                        new PrimaryKey("pk", Arrays.asList("order_id", "line_no")));
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("db", "schema", "table"))
                        .catalogTable(ct)
                        .partitionColumn("line_no")
                        .build();

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        Optional<SeaTunnelRowType> splitKey = splitter.findSplitKey(table);

        Assertions.assertTrue(splitKey.isPresent());
        SeaTunnelRowType rowType = splitKey.get();
        Assertions.assertEquals(1, rowType.getTotalFields());
        Assertions.assertEquals("line_no", rowType.getFieldName(0));
    }
}
