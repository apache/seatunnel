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

package org.apache.seatunnel.e2e.connector.hudi;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {},
        disabledReason = "")
@Slf4j
public class HudiIT extends TestSuiteBase {

    private static final String DATABASE = "st";
    private static final String DEFAULT_DATABASE = "default";
    private static final String TABLE_NAME = "st_test";
    private static final String TABLE_PATH = "/tmp/hudi/";
    private static final String NAMESPACE = "hudi";
    private static final String NAMESPACE_TAR = "hudi.tar.gz";

    protected final ContainerExtendedFactory containerExtendedFactory =
            new ContainerExtendedFactory() {
                @Override
                public void extend(GenericContainer<?> container)
                        throws IOException, InterruptedException {
                    container.execInContainer(
                            "sh",
                            "-c",
                            "cd /tmp" + " && tar -czvf " + NAMESPACE_TAR + " " + NAMESPACE);
                    container.copyFileFromContainer(
                            "/tmp/" + NAMESPACE_TAR, "/tmp/" + NAMESPACE_TAR);

                    extractFiles();
                }

                private void extractFiles() {
                    ProcessBuilder processBuilder = new ProcessBuilder();
                    processBuilder.command(
                            "sh", "-c", "cd /tmp" + " && tar -zxvf " + NAMESPACE_TAR);
                    try {
                        Process process = processBuilder.start();
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
            };

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("sh", "-c", "mkdir -p " + TABLE_PATH);
                container.execInContainer("sh", "-c", "chmod -R 777  " + TABLE_PATH);
            };

    @TestTemplate
    @DisabledOnContainer(
            value = {TestContainerId.SPARK_2_4},
            type = {EngineType.FLINK},
            disabledReason = "FLINK do not support local file catalog in hudi.")
    public void testWriteHudi(TestContainer container)
            throws IOException, InterruptedException, URISyntaxException {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_hudi.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", LocalFileSystem.DEFAULT_FS);
        Path inputPath =
                new Path(TABLE_PATH + File.separator + DATABASE + File.separator + TABLE_NAME);

        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy hudi to local
                            container.executeExtraCommands(containerExtendedFactory);
                            ParquetReader<Group> reader =
                                    ParquetReader.builder(new GroupReadSupport(), inputPath)
                                            .withConf(configuration)
                                            .build();

                            long rowCount = 0;

                            // Read data and count rows
                            while (reader.read() != null) {
                                rowCount++;
                            }
                            Assertions.assertEquals(5, rowCount);
                        });
        FileUtils.deleteFile(TABLE_PATH);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {TestContainerId.SPARK_2_4},
            type = {EngineType.FLINK},
            disabledReason = "FLINK do not support local file catalog in hudi.")
    public void testWriteHudiWithOmitConfigItem(TestContainer container)
            throws IOException, InterruptedException, URISyntaxException {
        Container.ExecResult textWriteResult =
                container.executeJob("/fake_to_hudi_with_omit_config_item.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", LocalFileSystem.DEFAULT_FS);
        Path inputPath =
                new Path(
                        TABLE_PATH
                                + File.separator
                                + DEFAULT_DATABASE
                                + File.separator
                                + TABLE_NAME);

        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy hudi to local
                            container.executeExtraCommands(containerExtendedFactory);
                            ParquetReader<Group> reader =
                                    ParquetReader.builder(new GroupReadSupport(), inputPath)
                                            .withConf(configuration)
                                            .build();

                            long rowCount = 0;

                            // Read data and count rows
                            while (reader.read() != null) {
                                rowCount++;
                            }
                            Assertions.assertEquals(5, rowCount);
                        });
        FileUtils.deleteFile(TABLE_PATH);
    }
}
