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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.reader;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessSourceSplit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** Integration tests that exercise stable startup, table identity and checkpoint restore. */
class TestVitessSourceReaderIT {

    private static final VitessContainer VITESS_CONTAINER =
            new VitessContainer()
                    .withKeyspace("test")
                    .withExposedPorts(VitessContainer.MYSQL_PORT, VitessContainer.GRPC_PORT);

    @BeforeAll
    static void startContainer() {
        Startables.deepStart(Stream.of(VITESS_CONTAINER)).join();
    }

    @AfterAll
    static void stopContainer() {
        VITESS_CONTAINER.stop();
    }

    @BeforeEach
    void initializeTables() throws Exception {
        executeStatements(
                "USE test",
                "DROP TABLE IF EXISTS customers",
                "DROP TABLE IF EXISTS products",
                "CREATE TABLE products ("
                        + "id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                        + "name VARCHAR(255) NOT NULL,"
                        + "description VARCHAR(512),"
                        + "weight FLOAT)",
                "ALTER TABLE products AUTO_INCREMENT = 101",
                "CREATE TABLE customers ("
                        + "id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                        + "name VARCHAR(255) NOT NULL)",
                "ALTER TABLE customers AUTO_INCREMENT = 201");
    }

    @Test
    void testCheckpointRestorePreservesOffsetsAndTableIdentity() throws Exception {
        ReadonlyConfig config =
                createConfig(
                        StartupMode.LATEST, null, Arrays.asList("test.products", "test.customers"));
        List<CatalogTable> catalogTables = Arrays.asList(productTable(), customerTable());

        VitessSourceConfig sourceConfig = VitessSourceConfig.of(config, catalogTables);
        VitessSourceReader reader =
                new VitessSourceReader(new TestingSourceReaderContext(), sourceConfig);
        reader.open();
        reader.addSplits(Collections.singletonList(sourceConfig.createInitialSplit()));
        waitForVitessStreamStartup();

        TestingCollector collector = new TestingCollector();
        emitUntilRowObserved(
                reader,
                collector,
                "INSERT INTO products VALUES (default, 'scooter', 'Small 2-wheel scooter', 3.14)",
                row ->
                        "test.products".equals(row.getTableId())
                                && "scooter".equals(row.getField(1)));
        emitUntilRowObserved(
                reader,
                collector,
                "INSERT INTO customers VALUES (default, 'alice')",
                row ->
                        "test.customers".equals(row.getTableId())
                                && "alice".equals(row.getField(1)));

        Assertions.assertTrue(
                collector.rows.stream()
                        .anyMatch(
                                row ->
                                        "test.products".equals(row.getTableId())
                                                && RowKind.INSERT.equals(row.getRowKind())
                                                && "scooter".equals(row.getField(1))));
        Assertions.assertTrue(
                collector.rows.stream()
                        .anyMatch(
                                row ->
                                        "test.customers".equals(row.getTableId())
                                                && "alice".equals(row.getField(1))));

        List<VitessSourceSplit> checkpointSplits = reader.snapshotState(1L);
        Assertions.assertEquals(1, checkpointSplits.size());
        Assertions.assertNotNull(checkpointSplits.get(0).getOffset());
        Assertions.assertTrue(checkpointSplits.get(0).getOffset().containsKey("vgtid"));
        Assertions.assertNotNull(checkpointSplits.get(0).getTableSchemas());
        Assertions.assertEquals(2, checkpointSplits.get(0).getTableSchemas().size());
        reader.close();

        executeStatements(
                "INSERT INTO products VALUES (default, 'car battery', '12V car battery', 8.1)");

        VitessSourceReader restoredReader =
                new VitessSourceReader(new TestingSourceReaderContext(), sourceConfig);
        restoredReader.open();
        restoredReader.addSplits(checkpointSplits);
        waitForVitessStreamStartup();

        TestingCollector restoredCollector = new TestingCollector();
        waitUntilRowMatches(
                restoredReader,
                restoredCollector,
                row ->
                        "test.products".equals(row.getTableId())
                                && "car battery".equals(row.getField(1)));

        Assertions.assertTrue(
                restoredCollector.rows.stream()
                        .anyMatch(
                                row ->
                                        "test.products".equals(row.getTableId())
                                                && "car battery".equals(row.getField(1))));
        restoredReader.close();
    }

    @Test
    void testSpecificStartupUsesCapturedVgtid() throws Exception {
        ReadonlyConfig latestConfig =
                createConfig(StartupMode.LATEST, null, Collections.singletonList("test.products"));
        VitessSourceConfig latestSourceConfig =
                VitessSourceConfig.of(latestConfig, Collections.singletonList(productTable()));
        VitessSourceReader bootstrapReader =
                new VitessSourceReader(new TestingSourceReaderContext(), latestSourceConfig);
        bootstrapReader.open();
        bootstrapReader.addSplits(
                Collections.singletonList(latestSourceConfig.createInitialSplit()));
        waitForVitessStreamStartup();

        TestingCollector bootstrapCollector = new TestingCollector();
        emitUntilRowObserved(
                bootstrapReader,
                bootstrapCollector,
                "INSERT INTO products VALUES (default, 'scooter', 'Small 2-wheel scooter', 3.14)",
                row ->
                        "test.products".equals(row.getTableId())
                                && "scooter".equals(row.getField(1)));
        List<VitessSourceSplit> bootstrapCheckpointSplits = bootstrapReader.snapshotState(1L);
        String capturedVgtid = (String) bootstrapCheckpointSplits.get(0).getOffset().get("vgtid");
        Assertions.assertNotNull(bootstrapCheckpointSplits.get(0).getTableSchemas());
        bootstrapReader.close();

        executeStatements(
                "INSERT INTO products VALUES (default, 'hammer', '16oz carpenters hammer', 1.0)");

        ReadonlyConfig specificConfig =
                createConfig(
                        StartupMode.SPECIFIC,
                        capturedVgtid,
                        Collections.singletonList("test.products"));
        VitessSourceConfig specificSourceConfig =
                VitessSourceConfig.of(specificConfig, Collections.singletonList(productTable()));
        VitessSourceReader specificReader =
                new VitessSourceReader(new TestingSourceReaderContext(), specificSourceConfig);
        specificReader.open();
        VitessSourceSplit specificSplit = specificSourceConfig.createInitialSplit();
        Assertions.assertNotNull(specificSplit.getTableSchemas());
        specificReader.addSplits(Collections.singletonList(specificSplit));
        waitForVitessStreamStartup();

        TestingCollector specificCollector = new TestingCollector();
        waitUntilRowMatches(
                specificReader,
                specificCollector,
                row ->
                        "test.products".equals(row.getTableId())
                                && "hammer".equals(row.getField(1)));

        Assertions.assertTrue(
                specificCollector.rows.stream()
                        .anyMatch(
                                row ->
                                        "test.products".equals(row.getTableId())
                                                && "hammer".equals(row.getField(1))));
        specificReader.close();
    }

    private static CatalogTable productTable() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of("pk_products", Collections.singletonList("id")))
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("description")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("weight")
                                        .dataType(BasicType.FLOAT_TYPE)
                                        .build())
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("test", "products")),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static CatalogTable customerTable() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of("pk_customers", Collections.singletonList("id")))
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("test", "customers")),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static ReadonlyConfig createConfig(
            StartupMode startupMode, String specificVgtid, List<String> tableNames) {
        Map<String, Object> options = new HashMap<>();
        options.put(VitessSourceOptions.HOSTNAME.key(), VITESS_CONTAINER.getHost());
        options.put(VitessSourceOptions.PORT.key(), VITESS_CONTAINER.getGrpcPort());
        options.put(VitessSourceOptions.KEYSPACE.key(), VITESS_CONTAINER.getKeyspace());
        options.put(VitessSourceOptions.STARTUP_MODE.key(), startupMode.name());
        options.put(ConnectorCommonOptions.TABLE_NAMES.key(), tableNames);
        options.put(VitessSourceOptions.SERVER_TIME_ZONE.key(), "UTC");
        options.put(
                SourceOptions.DEBEZIUM_PROPERTIES.key(),
                Collections.singletonMap("poll.interval.ms", "100"));
        if (specificVgtid != null) {
            options.put(VitessSourceOptions.STARTUP_SPECIFIC_OFFSET_VGTID.key(), specificVgtid);
        }
        return ReadonlyConfig.fromMap(options);
    }

    private static void executeStatements(String... sqlStatements) throws Exception {
        try (Connection connection = DriverManager.getConnection(VITESS_CONTAINER.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            for (String sqlStatement : sqlStatements) {
                statement.execute(sqlStatement);
            }
        }
    }

    private static void emitUntilRowObserved(
            VitessSourceReader reader,
            TestingCollector collector,
            String sqlStatement,
            Predicate<SeaTunnelRow> predicate)
            throws Exception {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            executeStatements(sqlStatement);
            if (waitUntilRowMatches(reader, collector, predicate, 5_000L)) {
                return;
            }
        }
        Assertions.fail(
                "Timed out waiting for Vitess CDC rows after repeated inserts. "
                        + "collectorRows="
                        + collectorDebug(collector));
    }

    private static void waitUntilRowMatches(
            VitessSourceReader reader,
            TestingCollector collector,
            Predicate<SeaTunnelRow> predicate)
            throws Exception {
        Assertions.assertTrue(
                waitUntilRowMatches(reader, collector, predicate, 30_000L),
                "Timed out waiting for Vitess CDC rows.");
    }

    private static boolean waitUntilRowMatches(
            VitessSourceReader reader,
            TestingCollector collector,
            Predicate<SeaTunnelRow> predicate,
            long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            reader.pollNext(collector);
            if (collector.rows.stream().anyMatch(predicate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Flink CDC waits until the Debezium task reports a fully started state before producing test
     * changes. SeaTunnel's source-reader abstraction does not expose that signal, so the IT keeps
     * the same startup budget with a fixed warm-up window.
     */
    private static void waitForVitessStreamStartup() throws InterruptedException {
        Thread.sleep(10_000L);
    }

    private static String collectorDebug(TestingCollector collector) {
        if (collector.rows.isEmpty()) {
            return "[]";
        }
        int fromIndex = Math.max(0, collector.rows.size() - 5);
        return collector.rows.subList(fromIndex, collector.rows.size()).toString();
    }

    /** Minimal source reader context used by the integration tests. */
    static final class TestingSourceReaderContext implements SourceReader.Context {

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.UNBOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }

    /** Collector used by the integration tests to capture emitted SeaTunnel CDC rows. */
    static final class TestingCollector implements Collector<SeaTunnelRow> {

        private final Object checkpointLock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record.copy());
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
