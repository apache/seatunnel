/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.PartitionDefinition;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.source.reader.DorisSourceReader;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DorisTableConfigTest {

    @Test
    public void testDuplicateTableListRejected() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "table_list",
                Arrays.asList(tableEntry("test_db", "table1"), tableEntry("test_db", "table1")));
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> DorisTableConfig.of(config));

        Assertions.assertTrue(exception.getMessage().contains("not allow null/duplicate"));
    }

    @Test
    public void testDistinctTableListReturnsTwoConfigs() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "table_list",
                Arrays.asList(tableEntry("test_db", "table1"), tableEntry("test_db", "table2")));
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        List<DorisTableConfig> result = DorisTableConfig.of(config);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("test_db.table1", result.get(0).getTableIdentifier());
        Assertions.assertEquals("test_db.table2", result.get(1).getTableIdentifier());
    }

    @Test
    public void testSingleTableCaseInsensitiveLowercasesIdentifiers() {
        Map<String, Object> options = new HashMap<>();
        options.put("database", "TestDB");
        options.put("table", "TestTable");
        options.put("case_sensitive", false);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        List<DorisTableConfig> result = DorisTableConfig.of(config);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("testdb.testtable", result.get(0).getTableIdentifier());
    }

    @Test
    public void testSingleTableCaseSensitivePreservesIdentifiers() {
        Map<String, Object> options = new HashMap<>();
        options.put("database", "TestDB");
        options.put("table", "TestTable");
        options.put("case_sensitive", true);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        List<DorisTableConfig> result = DorisTableConfig.of(config);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("TestDB.TestTable", result.get(0).getTableIdentifier());
    }

    @Test
    public void testSingleTableCaseSensitiveAbsentPreservesIdentifiers() {
        Map<String, Object> options = new HashMap<>();
        options.put("database", "TestDB");
        options.put("table", "TestTable");
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        List<DorisTableConfig> result = DorisTableConfig.of(config);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("TestDB.TestTable", result.get(0).getTableIdentifier());
    }

    @Test
    public void testSingleTableCaseInsensitiveLowercasesWithTurkishDefaultLocale() {
        Locale originalLocale = Locale.getDefault();
        try {
            // A Turkish default locale maps capital I to dotless ı (U+0131) in a no-arg
            // toLowerCase(), so the identifiers deliberately start with a capital I: a
            // locale-sensitive regression would produce "ınventorydb" instead. The
            // conversion must stay locale-independent (Locale.ROOT).
            Locale.setDefault(new Locale("tr", "TR"));

            Map<String, Object> options = new HashMap<>();
            options.put("database", "InventoryDB");
            options.put("table", "InventoryTable");
            options.put("case_sensitive", false);
            ReadonlyConfig config = ReadonlyConfig.fromMap(options);

            List<DorisTableConfig> result = DorisTableConfig.of(config);

            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(
                    "inventorydb.inventorytable", result.get(0).getTableIdentifier());
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    @Test
    public void testRestoredMixedCaseSplitFailsFastAgainstLowercaseTableMap() throws Exception {
        // A checkpoint saved by an earlier version can hold splits with the original
        // mixed-case names. The source map is keyed by the normalized (lowercase)
        // TablePath, so the reader must fail fast instead of silently reading another
        // table or rewriting the split identity (see incompatible-changes.md).
        Map<String, Object> options = new HashMap<>();
        options.put("fenodes", "127.0.0.1:8030");
        options.put("username", "dummy_user");
        options.put("password", "dummy_pass");
        options.put("database", "TestDB");
        options.put("table", "TestTable");
        options.put("case_sensitive", false);
        DorisSourceConfig sourceConfig = DorisSourceConfig.of(ReadonlyConfig.fromMap(options));

        TablePath lowercasePath = TablePath.of("testdb", "testtable");
        DorisSourceTable lowercaseTable =
                DorisSourceTable.builder()
                        .tablePath(lowercasePath)
                        .catalogTable(
                                CatalogTable.of(
                                        TableIdentifier.of("doris", lowercasePath),
                                        TableSchema.builder().build(),
                                        Collections.emptyMap(),
                                        Collections.emptyList(),
                                        null))
                        .build();
        Map<TablePath, DorisSourceTable> tables = new HashMap<>();
        tables.put(lowercasePath, lowercaseTable);

        DorisSourceReader reader =
                new DorisSourceReader(
                        new SourceReader.Context() {
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
                        },
                        sourceConfig,
                        tables);
        PartitionDefinition legacyPartition =
                new PartitionDefinition(
                        "TestDB",
                        "TestTable",
                        "127.0.0.1:9060",
                        Collections.singleton(10001L),
                        "select 1");
        reader.addSplits(
                Collections.singletonList(new DorisSourceSplit(legacyPartition, "legacy-0")));

        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class, () -> reader.pollNext(new NoopCollector()));

        Assertions.assertEquals(
                DorisConnectorErrorCode.SHOULD_NEVER_HAPPEN, exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("cannot be found in table_list of job configuration"));
    }

    private static final class NoopCollector implements Collector<SeaTunnelRow> {

        @Override
        public void collect(SeaTunnelRow record) {}

        @Override
        public Object getCheckpointLock() {
            return new Object();
        }
    }

    private static Map<String, Object> tableEntry(String database, String table) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("database", database);
        entry.put("table", table);
        return entry;
    }
}
