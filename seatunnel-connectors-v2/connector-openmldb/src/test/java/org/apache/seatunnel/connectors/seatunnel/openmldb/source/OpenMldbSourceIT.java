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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbSqlExecutor;
import org.apache.seatunnel.connectors.seatunnel.openmldb.exception.OpenMldbConnectorException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Runs against a disposable OpenMLDB 0.6.3 server. Enable with -Dopenmldb.integration=true and
 * optionally -Dopenmldb.host / -Dopenmldb.port for standalone mode, or -Dopenmldb.cluster=true
 * -Dopenmldb.zk.host -Dopenmldb.zk.path for cluster mode. The SDK's native library requires a
 * compatible Linux amd64 runtime.
 */
@EnabledIfSystemProperty(named = "openmldb.integration", matches = "true")
class OpenMldbSourceIT {
    private static final String DATABASE = "st_" + UUID.randomUUID().toString().replace("-", "");
    private static final String SECOND_DATABASE = DATABASE + "_second";
    private static final String FIELDS =
            "id=STRING, b=BOOLEAN, s=SMALLINT, i=INT, l=BIGINT, "
                    + "f=FLOAT, d=DOUBLE, text=STRING, day=DATE, ts=TIMESTAMP";
    private static SqlClusterExecutor setup;

    @BeforeAll
    static void prepare() throws Exception {
        setup = OpenMldbSqlExecutor.create(parameters("select * from values_table"));
        assertTrue(setup.createDB(DATABASE));
        assertTrue(setup.createDB(SECOND_DATABASE));
        String columns =
                "(id string, b bool, s smallint, i int, l bigint, f float, d double, "
                        + "text string, day date, ts timestamp, index(key=id));";
        assertTrue(setup.executeDDL(DATABASE, "create table values_table" + columns));
        assertTrue(setup.executeDDL(DATABASE, "create table empty_table" + columns));
        assertTrue(
                setup.executeInsert(
                        DATABASE,
                        "insert into values_table values"
                                + "('nulls', null, null, null, null, null, null, null, null, null),"
                                + "('zeros', false, 0, 0, 0, 0.0, 0.0, '', '2020-02-29', 0),"
                                + "('values', true, -12, 42, 1234567890123, 1.5, 2.25, 'hello', '2024-01-02', 1704153600000);"));
        assertTrue(
                setup.executeDDL(
                        SECOND_DATABASE,
                        "create table orders(id string, amount int, index(key=id));"));
        assertTrue(setup.executeInsert(SECOND_DATABASE, "insert into orders values('order', 7);"));
        assertTrue(
                setup.executeDDL(
                        DATABASE,
                        "create table bulk_table(id string, seq int, index(key=id))"
                                + (Boolean.getBoolean("openmldb.cluster")
                                        ? " options(partitionnum=4,replicanum=1)"
                                        : "")
                                + ";"));
        StringBuilder bulk = new StringBuilder("insert into bulk_table values");
        for (int i = 0; i < 1101; i++) {
            if (i > 0) {
                bulk.append(',');
            }
            bulk.append("('key").append(i).append("',").append(i).append(')');
        }
        assertTrue(setup.executeInsert(DATABASE, bulk.append(';').toString()));
    }

    @AfterAll
    static void cleanup() {
        if (setup != null) {
            setup.executeDDL(DATABASE, "drop table values_table;");
            setup.executeDDL(DATABASE, "drop table empty_table;");
            setup.executeDDL(DATABASE, "drop table bulk_table;");
            setup.executeDDL(SECOND_DATABASE, "drop table orders;");
            setup.dropDB(DATABASE);
            setup.dropDB(SECOND_DATABASE);
            setup.close();
        }
    }

    @Test
    void multiTablePreservesNullsTypesAndTableIdentity() throws Exception {
        OpenMldbSource source =
                source(
                        entry("empty", "select * from empty_table", FIELDS, null)
                                + ","
                                + entry("values", "select * from values_table", FIELDS, null)
                                + ","
                                + entry(
                                        "orders",
                                        "select amount as total, id as order_id from orders",
                                        "order_id=STRING, total=INT",
                                        SECOND_DATABASE));
        assertEquals(3, source.getProducedCatalogTables().size());
        SingleSplitReaderContext context = context();
        List<SeaTunnelRow> rows = new ArrayList<>();
        try (OpenMldbSourceReader reader = (OpenMldbSourceReader) source.createReader(context)) {
            reader.open();
            reader.pollNext(collector(rows));
            reader.pollNext(collector(rows));
        }
        assertEquals(4, rows.size());
        verify(context).signalNoMoreElement();
        CatalogTable values = source.getProducedCatalogTables().get(1);
        Map<String, SeaTunnelRow> byId = new HashMap<>();
        for (SeaTunnelRow row : rows) {
            if (values.getTableId().toTablePath().toString().equals(row.getTableId())) {
                byId.put((String) field(row, values.getSeaTunnelRowType(), "id"), row);
            }
        }
        assertEquals(3, byId.size());
        SeaTunnelRow nulls = byId.get("nulls");
        for (String name : new String[] {"b", "s", "i", "l", "f", "d", "text", "day", "ts"}) {
            assertNull(field(nulls, values.getSeaTunnelRowType(), name), name);
        }
        SeaTunnelRow zeros = byId.get("zeros");
        assertEquals(false, field(zeros, values.getSeaTunnelRowType(), "b"));
        assertEquals((short) 0, field(zeros, values.getSeaTunnelRowType(), "s"));
        assertEquals(0, field(zeros, values.getSeaTunnelRowType(), "i"));
        assertEquals(0L, field(zeros, values.getSeaTunnelRowType(), "l"));
        assertEquals(0F, field(zeros, values.getSeaTunnelRowType(), "f"));
        assertEquals(0D, field(zeros, values.getSeaTunnelRowType(), "d"));
        assertEquals("", field(zeros, values.getSeaTunnelRowType(), "text"));
        assertEquals(LocalDate.of(2020, 2, 29), field(zeros, values.getSeaTunnelRowType(), "day"));
        assertEquals(
                new Timestamp(0).toLocalDateTime(),
                field(zeros, values.getSeaTunnelRowType(), "ts"));
        SeaTunnelRow nonNull = byId.get("values");
        Object[] expected = {
            true,
            (short) -12,
            42,
            1234567890123L,
            1.5F,
            2.25D,
            "hello",
            LocalDate.of(2024, 1, 2),
            new Timestamp(1704153600000L).toLocalDateTime()
        };
        String[] names = {"b", "s", "i", "l", "f", "d", "text", "day", "ts"};
        for (int i = 0; i < names.length; i++) {
            assertEquals(
                    expected[i], field(nonNull, values.getSeaTunnelRowType(), names[i]), names[i]);
        }
        assertNotSame(nulls.getFields(), zeros.getFields());
        CatalogTable orders = source.getProducedCatalogTables().get(2);
        SeaTunnelRow order =
                rows.stream()
                        .filter(
                                row ->
                                        row.getTableId()
                                                .equals(
                                                        orders.getTableId()
                                                                .toTablePath()
                                                                .toString()))
                        .findFirst()
                        .get();
        assertEquals("order", field(order, orders.getSeaTunnelRowType(), "order_id"));
        assertEquals(7, field(order, orders.getSeaTunnelRowType(), "total"));
    }

    @Test
    void legacySourcePreservesNullsAndReaderOwnership() throws Exception {
        OpenMldbSource legacy = new OpenMldbSource(parameters("select * from values_table"));
        assertTrue(
                legacy.getProducedCatalogTables()
                        .get(0)
                        .getTableSchema()
                        .getColumns()
                        .get(1)
                        .isNullable());
        SingleSplitReaderContext context = context();
        try (OpenMldbSourceReader first = (OpenMldbSourceReader) legacy.createReader(context);
                OpenMldbSourceReader second = (OpenMldbSourceReader) legacy.createReader(context)) {
            first.open();
            second.open();
            first.close();
            List<SeaTunnelRow> rows = new ArrayList<>();
            second.pollNext(collector(rows));
            assertEquals(3, rows.size());
            SeaTunnelRow nulls =
                    rows.stream().filter(row -> "nulls".equals(row.getField(0))).findFirst().get();
            for (int i = 1; i < 10; i++) {
                assertNull(nulls.getField(i));
            }
        }
    }

    @Test
    void rejectsInvalidQueryAndSchemaWithoutSuccessfulCompletion() throws Exception {
        for (String entry :
                new String[] {
                    entry("missing", "select * from missing_table", "id=STRING", null),
                    entry("count", "select * from values_table", "id=STRING", null),
                    entry("type", "select i from values_table", "i=STRING", null),
                    entry("name", "select i from values_table", "wrong=INT", null),
                    entry(
                            "duplicate",
                            "select i as value, i as value from values_table",
                            "value=INT, other=INT",
                            null)
                }) {
            OpenMldbSource source = source(entry);
            SingleSplitReaderContext context = context();
            List<SeaTunnelRow> rows = new ArrayList<>();
            try (OpenMldbSourceReader reader =
                    (OpenMldbSourceReader) source.createReader(context)) {
                reader.open();
                OpenMldbConnectorException error =
                        assertThrows(
                                OpenMldbConnectorException.class,
                                () -> reader.pollNext(collector(rows)));
                assertTrue(error.getCause() instanceof SQLException);
            }
            assertTrue(rows.isEmpty());
            verify(context, never()).signalNoMoreElement();
        }
    }

    @Test
    void invalidQueryDoesNotPreventLaterQueriesOnSameClient() throws Exception {
        OpenMldbSource source =
                source(entry("one", "select id from values_table", "id=STRING", null));
        SeaTunnelRowType type = source.getProducedCatalogTables().get(0).getSeaTunnelRowType();
        try (OpenMldbReadClient client =
                new OpenMldbReadClient(parameters("select id from values_table"))) {
            assertThrows(
                    SQLException.class,
                    () -> client.execute(DATABASE, "select * from missing_table", type, true));
            try (OpenMldbReadClient.Query query =
                    client.execute(DATABASE, "select id from values_table", type, true)) {
                int rows = 0;
                while (query.next()) {
                    assertFalse(query.readRow().getField(0).toString().isEmpty());
                    rows++;
                }
                assertEquals(3, rows);
            }
        }
    }

    private static Object field(SeaTunnelRow row, SeaTunnelRowType type, String name) {
        return row.getField(type.indexOf(name));
    }

    @Test
    void readsAllRowsAcrossPartitions() throws Exception {
        OpenMldbSource source =
                source(entry("bulk", "select seq, id from bulk_table", "id=STRING, seq=INT", null));
        List<SeaTunnelRow> rows = new ArrayList<>();
        try (OpenMldbSourceReader reader = (OpenMldbSourceReader) source.createReader(context())) {
            reader.open();
            reader.pollNext(collector(rows));
        }
        assertEquals(1101, rows.size());
        HashSet<Object> keys = new HashSet<>();
        SeaTunnelRowType type = source.getProducedCatalogTables().get(0).getSeaTunnelRowType();
        for (SeaTunnelRow row : rows) {
            Object id = field(row, type, "id");
            assertEquals("key" + field(row, type, "seq"), id);
            assertTrue(keys.add(id));
        }
    }

    private static String connection() {
        if (Boolean.getBoolean("openmldb.cluster")) {
            return "cluster_mode=true\nzk_host=\""
                    + System.getProperty("openmldb.zk.host", "127.0.0.1:2181")
                    + "\"\nzk_path=\""
                    + System.getProperty("openmldb.zk.path", "/openmldb")
                    + "\"\ndatabase="
                    + DATABASE
                    + "\n";
        }
        return "cluster_mode=false\nhost=\""
                + System.getProperty("openmldb.host", "127.0.0.1")
                + "\"\nport="
                + System.getProperty("openmldb.port", "6527")
                + "\ndatabase="
                + DATABASE
                + "\n";
    }

    private static OpenMldbParameters parameters(String sql) {
        return OpenMldbParameters.buildWithConfig(
                ConfigFactory.parseString(connection() + "sql=\"" + sql + "\""));
    }

    private static OpenMldbSource source(String entries) {
        return (OpenMldbSource)
                new OpenMldbSourceFactory()
                        .<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState>createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromConfig(
                                                ConfigFactory.parseString(
                                                        connection()
                                                                + "tables_configs=["
                                                                + entries
                                                                + "]")),
                                        OpenMldbSourceIT.class.getClassLoader()))
                        .createSource();
    }

    private static String entry(String table, String sql, String fields, String database) {
        return "{sql=\""
                + sql
                + "\", schema {table="
                + table
                + ", fields {"
                + fields
                + "}}"
                + (database == null ? "" : ", database=" + database)
                + "}";
    }

    private static SingleSplitReaderContext context() {
        SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        return context;
    }

    private static Collector<SeaTunnelRow> collector(List<SeaTunnelRow> rows) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow row) {
                rows.add(row);
            }

            @Override
            public Object getCheckpointLock() {
                return rows;
            }
        };
    }
}
