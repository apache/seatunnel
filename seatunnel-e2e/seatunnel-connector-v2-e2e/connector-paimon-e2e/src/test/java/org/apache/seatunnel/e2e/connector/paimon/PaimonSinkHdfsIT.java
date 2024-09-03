/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
@Disabled(
        "HDFS is not available in CI, if you want to run this test, please set up your own HDFS environment in the test case file and the below setup")
public class PaimonSinkHdfsIT extends TestSuiteBase {

    private String hiveExecUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar";
    }

    private String libfb303Url() {
        return "https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.0/libfb303-0.9.0.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Paimon/lib && cd /tmp/seatunnel/plugins/Paimon/lib && wget "
                                        + hiveExecUrl()
                                        + " && wget "
                                        + libfb303Url());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    private Map<String, Object> PAIMON_SINK_PROPERTIES;

    @BeforeAll
    public void setup() {
        Map<String, Object> map = new HashMap<>();
        map.put("warehouse", "hdfs:///tmp/paimon");
        map.put("database", "seatunnel_namespace1");
        map.put("table", "st_test");
        Map<String, Object> paimonHadoopConf = new HashMap<>();
        paimonHadoopConf.put("fs.defaultFS", "hdfs://nameservice1");
        paimonHadoopConf.put("dfs.nameservices", "nameservice1");
        paimonHadoopConf.put("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        paimonHadoopConf.put("dfs.namenode.rpc-address.nameservice1.nn1", "hadoop03:8020");
        paimonHadoopConf.put("dfs.namenode.rpc-address.nameservice1.nn2", "hadoop04:8020");
        paimonHadoopConf.put(
                "dfs.client.failover.proxy.provider.nameservice1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        paimonHadoopConf.put("dfs.client.use.datanode.hostname", "true");
        map.put("paimon.hadoop.conf", paimonHadoopConf);
        this.PAIMON_SINK_PROPERTIES = map;
    }

    @TestTemplate
    public void testFakeCDCSinkPaimon(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fake_cdc_sink_paimon_with_hdfs_ha.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(200L, TimeUnit.MILLISECONDS)
                .atMost(40L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            PaimonSinkConfig paimonSinkConfig =
                                    new PaimonSinkConfig(
                                            ReadonlyConfig.fromMap(PAIMON_SINK_PROPERTIES));
                            PaimonCatalogLoader paimonCatalogLoader =
                                    new PaimonCatalogLoader(paimonSinkConfig);
                            Catalog catalog = paimonCatalogLoader.loadCatalog();
                            Table table =
                                    catalog.getTable(
                                            Identifier.create("seatunnel_namespace1", "st_test"));
                            ReadBuilder readBuilder = table.newReadBuilder();
                            TableScan.Plan plan = readBuilder.newScan().plan();
                            TableRead tableRead = readBuilder.newRead();
                            List<PaimonRecord> paimonRecords = new ArrayList<>();
                            try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
                                reader.forEachRemaining(
                                        row ->
                                                paimonRecords.add(
                                                        new PaimonRecord(
                                                                row.getLong(0),
                                                                row.getString(1).toString())));
                            }
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A_1", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                        });

        Container.ExecResult readResult =
                container.executeJob("/read_from_paimon_with_hdfs_ha_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testFakeCDCSinkPaimonWithHiveCatalogAndRead(TestContainer container)
            throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fake_cdc_sink_paimon_with_hdfs_with_hive_catalog.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(200L, TimeUnit.MILLISECONDS)
                .atMost(40L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            PaimonSinkConfig paimonSinkConfig =
                                    new PaimonSinkConfig(
                                            ReadonlyConfig.fromMap(PAIMON_SINK_PROPERTIES));
                            PaimonCatalogLoader paimonCatalogLoader =
                                    new PaimonCatalogLoader(paimonSinkConfig);
                            Catalog catalog = paimonCatalogLoader.loadCatalog();
                            Table table =
                                    catalog.getTable(
                                            Identifier.create("seatunnel_namespace1", "st_test"));
                            ReadBuilder readBuilder = table.newReadBuilder();
                            TableScan.Plan plan = readBuilder.newScan().plan();
                            TableRead tableRead = readBuilder.newRead();
                            List<PaimonRecord> paimonRecords = new ArrayList<>();
                            try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
                                reader.forEachRemaining(
                                        row ->
                                                paimonRecords.add(
                                                        new PaimonRecord(
                                                                row.getLong(0),
                                                                row.getString(1).toString())));
                            }
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A_1", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                        });

        Container.ExecResult readResult =
                container.executeJob("/paimon_to_assert_with_hivecatalog.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testSinkPaimonHdfsTruncateTable(TestContainer container) throws Exception {
        Container.ExecResult writeResult =
                container.executeJob("/fake_sink_paimon_truncate_with_hdfs_case1.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());
        Container.ExecResult readResult =
                container.executeJob("/fake_sink_paimon_truncate_with_hdfs_case2.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(180L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            PaimonSinkConfig paimonSinkConfig =
                                    new PaimonSinkConfig(
                                            ReadonlyConfig.fromMap(PAIMON_SINK_PROPERTIES));
                            PaimonCatalogLoader paimonCatalogLoader =
                                    new PaimonCatalogLoader(paimonSinkConfig);
                            Catalog catalog = paimonCatalogLoader.loadCatalog();
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData(catalog, "seatunnel_namespace11", "st_test");
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("Aa", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 2) {
                                            Assertions.assertEquals("Bb", paimonRecord.getName());
                                        }
                                        Assertions.assertEquals(200, paimonRecord.getScore());
                                    });
                            List<Long> ids =
                                    paimonRecords.stream()
                                            .map(PaimonRecord::getPkId)
                                            .collect(Collectors.toList());
                            Assertions.assertFalse(ids.contains(3L));
                        });
    }

    @TestTemplate
    public void testSinkPaimonHiveTruncateTable(TestContainer container) throws Exception {
        Container.ExecResult writeResult =
                container.executeJob("/fake_sink_paimon_truncate_with_hive_case1.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());
        Container.ExecResult readResult =
                container.executeJob("/fake_sink_paimon_truncate_with_hive_case2.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(180L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            PaimonSinkConfig paimonSinkConfig =
                                    new PaimonSinkConfig(
                                            ReadonlyConfig.fromMap(PAIMON_SINK_PROPERTIES));
                            PaimonCatalogLoader paimonCatalogLoader =
                                    new PaimonCatalogLoader(paimonSinkConfig);
                            Catalog catalog = paimonCatalogLoader.loadCatalog();
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData(catalog, "seatunnel_namespace12", "st_test");
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("Aa", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 2) {
                                            Assertions.assertEquals("Bb", paimonRecord.getName());
                                        }
                                        Assertions.assertEquals(200, paimonRecord.getScore());
                                    });
                            List<Long> ids =
                                    paimonRecords.stream()
                                            .map(PaimonRecord::getPkId)
                                            .collect(Collectors.toList());
                            Assertions.assertFalse(ids.contains(3L));
                        });
    }

    @TestTemplate
    public void testSinkPaimonHiveTruncateTable1(TestContainer container) throws Exception {
        PaimonSinkConfig paimonSinkConfig =
                new PaimonSinkConfig(ReadonlyConfig.fromMap(PAIMON_SINK_PROPERTIES));
        PaimonCatalogLoader paimonCatalogLoader = new PaimonCatalogLoader(paimonSinkConfig);
        Catalog catalog = paimonCatalogLoader.loadCatalog();
        List<PaimonRecord> paimonRecords =
                loadPaimonData(catalog, "seatunnel_namespace11", "st_test");
        Assertions.assertEquals(2, paimonRecords.size());
        paimonRecords.forEach(
                paimonRecord -> {
                    if (paimonRecord.getPkId() == 1) {
                        Assertions.assertEquals("Aa", paimonRecord.getName());
                    }
                    if (paimonRecord.getPkId() == 2) {
                        Assertions.assertEquals("Bb", paimonRecord.getName());
                    }
                    Assertions.assertEquals(200, paimonRecord.getScore());
                });
        List<Long> ids =
                paimonRecords.stream().map(PaimonRecord::getPkId).collect(Collectors.toList());
        Assertions.assertFalse(ids.contains(3L));
    }

    private List<PaimonRecord> loadPaimonData(Catalog catalog, String dbName, String tbName)
            throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create(dbName, tbName));
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        TableRead tableRead = readBuilder.newRead();
        List<PaimonRecord> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        PaimonRecord paimonRecord =
                                new PaimonRecord(row.getLong(0), row.getString(1).toString());
                        if (table.schema().fieldNames().contains("score")) {
                            paimonRecord.setScore(row.getInt(2));
                        }
                        result.add(paimonRecord);
                    });
        }
        return result;
    }
}
