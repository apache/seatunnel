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
import org.apache.hadoop.fs.FileUtil;
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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@Slf4j
public class HudiMultiTableIT extends TestSuiteBase {

    private static final String DATABASE_1 = "st1";
    private static final String TABLE_NAME_1 = "st_test_1";
    private static final String DATABASE_2 = "default";
    private static final String TABLE_NAME_2 = "st_test_2";
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
    public void testMultiWrite(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/hudi/multi_fake_to_hudi.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", LocalFileSystem.DEFAULT_FS);
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy hudi to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Path inputPath1 =
                                    getNewestCommitFilePath(
                                            new File(
                                                    TABLE_PATH
                                                            + File.separator
                                                            + DATABASE_1
                                                            + File.separator
                                                            + TABLE_NAME_1));
                            Path inputPath2 =
                                    getNewestCommitFilePath(
                                            new File(
                                                    TABLE_PATH
                                                            + File.separator
                                                            + DATABASE_2
                                                            + File.separator
                                                            + TABLE_NAME_2));
                            ParquetReader<Group> reader1 =
                                    ParquetReader.builder(new GroupReadSupport(), inputPath1)
                                            .withConf(configuration)
                                            .build();
                            ParquetReader<Group> reader2 =
                                    ParquetReader.builder(new GroupReadSupport(), inputPath2)
                                            .withConf(configuration)
                                            .build();

                            long rowCount1 = 0;
                            long rowCount2 = 0;
                            // Read data and count rows
                            while (reader1.read() != null) {
                                rowCount1++;
                            }
                            // Read data and count rows
                            while (reader2.read() != null) {
                                rowCount2++;
                            }
                            Assertions.assertEquals(100, rowCount1);
                            Assertions.assertEquals(240, rowCount2);
                        });
        FileUtils.deleteFile(TABLE_PATH);
    }

    public static Path getNewestCommitFilePath(File tablePathDir) throws IOException {
        File[] files = FileUtil.listFiles(tablePathDir);
        Long newestCommitTime =
                Arrays.stream(files)
                        .filter(file -> file.getName().endsWith(".parquet"))
                        .map(
                                file ->
                                        Long.parseLong(
                                                file.getName()
                                                        .substring(
                                                                file.getName().lastIndexOf("_") + 1,
                                                                file.getName()
                                                                        .lastIndexOf(".parquet"))))
                        .max(Long::compareTo)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Not found parquet file in " + tablePathDir));
        for (File file : files) {
            if (file.getName().endsWith(newestCommitTime + ".parquet")) {
                return new Path(file.toURI());
            }
        }
        throw new IllegalArgumentException("Not found parquet file in " + tablePathDir);
    }
}
