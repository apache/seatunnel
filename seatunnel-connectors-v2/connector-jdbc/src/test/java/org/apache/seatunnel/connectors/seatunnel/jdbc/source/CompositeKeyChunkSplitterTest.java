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

    @Test
    public void testFindSplitKeyReturnsAllCompositeKeyColumns() {
        JdbcSourceConfig config = config();
        Assertions.assertTrue(config.isUseDynamicSplitter());

        CatalogTable ct =
                catalogTable(
                        compositePkColumns(),
                        new PrimaryKey("pk", Arrays.asList("order_id", "line_no")));
        JdbcSourceTable table = table(ct);

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
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
    public void testFindSplitKeyFallsBackToSingleColumnForUnsupportedType() {
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

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
        Optional<SeaTunnelRowType> splitKey = splitter.findSplitKey(table);

        Assertions.assertTrue(splitKey.isPresent());
        SeaTunnelRowType rowType = splitKey.get();
        Assertions.assertEquals(1, rowType.getTotalFields());
        Assertions.assertEquals("order_id", rowType.getFieldName(0));
    }

    @Test
    public void testFindSplitKeyFallsBackToSingleColumnForDialectNotOptedIn() {
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

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(config);
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
                        + "AND ((`order_id` < ?) OR (`order_id` = ? AND `line_no` <= ?))",
                sql);

        // first split: a < ? OR (a = ? AND b <= ?)
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
                        + "WHERE ((`order_id` < ?) OR (`order_id` = ? AND `line_no` <= ?))",
                firstSql);

        // last split: a > ? OR (a = ? AND b > ?)
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
                        + "WHERE ((`order_id` > ?) OR (`order_id` = ? AND `line_no` > ?))",
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
                        + "AND ((`order_id` < ?) OR (`order_id` = ? AND `line_no` <= ?))",
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
    public void testCompositeKeySplitDialectSupport() {
        // Composite split SQL is emitted in portable expanded OR/AND form (no row-value
        // constructor). Each dialect opts in via supportCompositeKeySplit() only after its
        // composite-PK path is validated by an official E2E; currently MySQL, PostgreSQL,
        // SQLite, SQL Server and Oracle are covered.
        JdbcDialect mysql = JdbcDialectLoader.load("jdbc:mysql://localhost:3306/test", null, null);
        Assertions.assertTrue(mysql.supportCompositeKeySplit());
        Assertions.assertEquals(" LIMIT 10", mysql.getLimitClause(10));
        Assertions.assertEquals(" LIMIT 1 OFFSET 9", mysql.getOffsetLimitClause(9, 1));

        JdbcDialect postgres =
                JdbcDialectLoader.load("jdbc:postgresql://localhost:5432/test", null, null);
        Assertions.assertTrue(postgres.supportCompositeKeySplit());
        Assertions.assertEquals(" LIMIT 10", postgres.getLimitClause(10));
        Assertions.assertEquals(" LIMIT 1 OFFSET 9", postgres.getOffsetLimitClause(9, 1));

        JdbcDialect sqlite =
                JdbcDialectLoader.load("jdbc:sqlite:/tmp/seatunnel_split_e2e.db", null, null);
        Assertions.assertTrue(sqlite.supportCompositeKeySplit());
        Assertions.assertEquals(" LIMIT 10", sqlite.getLimitClause(10));
        Assertions.assertEquals(" LIMIT 1 OFFSET 9", sqlite.getOffsetLimitClause(9, 1));

        JdbcDialect sqlserver =
                JdbcDialectLoader.load("jdbc:sqlserver://localhost:1433", null, null);
        Assertions.assertTrue(sqlserver.supportCompositeKeySplit());
        Assertions.assertEquals(
                " OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", sqlserver.getLimitClause(10));
        Assertions.assertEquals(
                " OFFSET 9 ROWS FETCH NEXT 1 ROWS ONLY", sqlserver.getOffsetLimitClause(9, 1));

        JdbcDialect oracle =
                JdbcDialectLoader.load("jdbc:oracle:thin:@localhost:1521:xe", null, null);
        Assertions.assertTrue(oracle.supportCompositeKeySplit());
        Assertions.assertEquals(" FETCH FIRST 10 ROWS ONLY", oracle.getLimitClause(10));
        Assertions.assertEquals(
                " OFFSET 9 ROWS FETCH NEXT 1 ROWS ONLY", oracle.getOffsetLimitClause(9, 1));
    }
}
