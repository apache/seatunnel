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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Spark and Flink engine can not auto create paimon table on worker node in local file(e.g flink tm) by savemode feature which can lead error")
@Slf4j
public class PaimonSinkCDCIT extends TestSuiteBase implements TestResource {
    private static final String CATALOG_ROOT_DIR = "/tmp/";
    private static final String NAMESPACE = "paimon";
    private static final String NAMESPACE_TAR = "paimon.tar.gz";
    private static final String CATALOG_DIR = CATALOG_ROOT_DIR + NAMESPACE + "/";
    private static final String TARGET_TABLE = "st_test";
    private static final String TARGET_DATABASE = "seatunnel_namespace";
    private static final String FAKE_TABLE1 = "FakeTable1";
    private static final String FAKE_DATABASE1 = "FakeDatabase1";
    private static final String FAKE_TABLE2 = "FakeTable1";
    private static final String FAKE_DATABASE2 = "FakeDatabase2";

    @BeforeAll
    @Override
    public void startUp() throws Exception {}

    @AfterAll
    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    public void testFakeCDCSinkPaimon(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case1.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData(TARGET_DATABASE, TARGET_TABLE);
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

        cleanPaimonTable(container);
    }

    @TestTemplate
    public void testFakeMultipleTableSinkPaimon(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case2.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            // Check FakeDatabase1.FakeTable1
                            List<PaimonRecord> fake1PaimonRecords =
                                    loadPaimonData(FAKE_DATABASE1, FAKE_TABLE1);
                            Assertions.assertEquals(2, fake1PaimonRecords.size());
                            fake1PaimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A_1", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                            // Check FakeDatabase2.FakeTable1
                            List<PaimonRecord> fake2PaimonRecords =
                                    loadPaimonData(FAKE_DATABASE2, FAKE_TABLE2);
                            Assertions.assertEquals(2, fake2PaimonRecords.size());
                            fake2PaimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 100) {
                                            Assertions.assertEquals(
                                                    "A_100", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 200) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                        });

        cleanPaimonTable(container);
    }

    protected final ContainerExtendedFactory cleanContainerExtendedFactory =
            genericContainer ->
                    genericContainer.execInContainer("sh", "-c", "rm -rf  " + CATALOG_DIR + "**");

    private void cleanPaimonTable(TestContainer container)
            throws IOException, InterruptedException {
        // clean table
        container.executeExtraCommands(cleanContainerExtendedFactory);
    }

    protected final ContainerExtendedFactory containerExtendedFactory =
            container -> {
                FileUtils.deleteFile(CATALOG_ROOT_DIR + NAMESPACE_TAR);
                FileUtils.createNewDir(CATALOG_DIR);
                container.execInContainer(
                        "sh",
                        "-c",
                        "cd "
                                + CATALOG_ROOT_DIR
                                + " && tar -czvf "
                                + NAMESPACE_TAR
                                + " "
                                + NAMESPACE);
                container.copyFileFromContainer(
                        CATALOG_ROOT_DIR + NAMESPACE_TAR, CATALOG_ROOT_DIR + NAMESPACE_TAR);
                extractFiles();
            };

    private void extractFiles() {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(
                "sh", "-c", "cd " + CATALOG_ROOT_DIR + " && tar -zxvf " + NAMESPACE_TAR);
        try {
            Process process = processBuilder.start();
            // wait command completed
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                log.info("Extract files successful.");
            } else {
                log.error("Extract files failed with exit code " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private List<PaimonRecord> loadPaimonData(String dbName, String tbName) throws Exception {
        Table table = getTable(dbName, tbName);
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        TableRead tableRead = readBuilder.newRead();
        List<PaimonRecord> result = new ArrayList<>();
        log.info(
                "====================================Paimon data===========================================");
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        result.add(new PaimonRecord(row.getLong(0), row.getString(1).toString()));
                        log.info("key_id:" + row.getLong(0) + ", name:" + row.getString(1));
                    });
        }
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        return result;
    }

    private Table getTable(String dbName, String tbName) {
        try {
            return getCatalog().getTable(getIdentifier(dbName, tbName));
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        }
    }

    private Identifier getIdentifier(String dbName, String tbName) {
        return Identifier.create(dbName, tbName);
    }

    private Catalog getCatalog() {
        Options options = new Options();
        options.set("warehouse", "file://" + CATALOG_DIR);
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        return catalog;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public class PaimonRecord {
        private Long pkId;
        private String name;
    }
}
