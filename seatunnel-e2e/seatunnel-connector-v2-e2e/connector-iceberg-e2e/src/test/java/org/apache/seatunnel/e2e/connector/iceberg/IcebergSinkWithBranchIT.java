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

package org.apache.seatunnel.e2e.connector.iceberg;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {},
        disabledReason = "")
@DisabledOnOs(OS.WINDOWS)
public class IcebergSinkWithBranchIT extends TestSuiteBase {

    private static final String CATALOG_DIR = "/tmp/seatunnel/iceberg/hadoop-sink/";

    private static final String NAMESPACE = "seatunnel_namespace";

    private static final String commitBranch = "commit-branch";

    private String zstdUrl() {
        return "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-5/zstd-jni-1.5.5-5.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("sh", "-c", "mkdir -p " + CATALOG_DIR);
                container.execInContainer("sh", "-c", "chmod -R 777  " + CATALOG_DIR);
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p /tmp/seatunnel/plugins/Iceberg/lib && cd /tmp/seatunnel/plugins/Iceberg/lib && wget "
                                + zstdUrl());
            };

    private final String NAMESPACE_TAR = NAMESPACE + ".tar.gz";
    protected final ContainerExtendedFactory containerExtendedFactory =
            new ContainerExtendedFactory() {
                @Override
                public void extend(GenericContainer<?> container)
                        throws IOException, InterruptedException {
                    FileUtils.createNewDir(CATALOG_DIR);
                    container.execInContainer(
                            "sh",
                            "-c",
                            "cd "
                                    + CATALOG_DIR
                                    + " && tar -czvf "
                                    + NAMESPACE_TAR
                                    + " "
                                    + NAMESPACE);
                    container.copyFileFromContainer(
                            CATALOG_DIR + NAMESPACE_TAR, CATALOG_DIR + NAMESPACE_TAR);
                    extractFiles();
                }

                private void extractFiles() {
                    ProcessBuilder processBuilder = new ProcessBuilder();
                    processBuilder.command(
                            "sh", "-c", "cd " + CATALOG_DIR + " && tar -zxvf " + NAMESPACE_TAR);
                    try {
                        Process process = processBuilder.start();
                        int exitCode = process.waitFor();
                        if (exitCode == 0) {
                            log.info("Extract files successful.");
                        } else {
                            log.error("Extract files failed with exit code " + exitCode);
                        }
                    } catch (IOException | InterruptedException e) {
                        log.error("Extract data files from container error :", e);
                    }
                }
            };

    @TestTemplate
    public void testInsertAndCheckDataE2e(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/iceberg/fake_to_iceberg_with_branch.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        // stream stage
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy iceberg to local
                            container.executeExtraCommands(containerExtendedFactory);
                            // check branch exists
                            Assertions.assertEquals(true, checkBranchExists());
                            // load from branch
                            Assertions.assertEquals(100, loadDataFromIcebergTableBranch().size());
                        });
    }

    private boolean checkBranchExists() {
        Table table = getTable();
        Map<String, SnapshotRef> refs = table.refs();
        if (refs.containsKey(commitBranch)) {
            return true;
        }
        return false;
    }

    private List<Object> loadDataFromIcebergTableBranch() {
        List<Object> results = new ArrayList<>();
        Table table = getTable();
        TableScan branchRead = table.newScan().useRef(commitBranch);
        CloseableIterable<FileScanTask> fileScanTasks = branchRead.planFiles();
        fileScanTasks.forEach(
                fileScanTask -> {
                    try {
                        DataFile file = fileScanTask.file();
                        HadoopInputFile inputFile =
                                HadoopInputFile.fromPath(
                                        new Path(file.path().toString()), new Configuration());
                        try (ParquetReader<Object> reader =
                                AvroParquetReader.builder(inputFile).build()) {
                            Object record;
                            while ((record = reader.read()) != null) {
                                results.add(record);
                            }
                        }
                    } catch (IOException e) {
                        log.error("Table scan branch error :", e);
                    }
                });
        return results;
    }

    public Table getTable() {

        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        configs.put(CommonConfig.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(CommonConfig.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(CommonConfig.KEY_TABLE.key(), "iceberg_sink_table");
        configs.put(CommonConfig.CATALOG_PROPS.key(), catalogProps);
        IcebergTableLoader tableLoader =
                IcebergTableLoader.create(new SourceConfig(ReadonlyConfig.fromMap(configs)));
        tableLoader.open();
        // from branch
        return tableLoader.loadTable();
    }
}
