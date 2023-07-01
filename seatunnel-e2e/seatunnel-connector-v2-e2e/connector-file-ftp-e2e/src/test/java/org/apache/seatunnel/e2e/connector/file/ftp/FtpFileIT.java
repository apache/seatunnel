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

package org.apache.seatunnel.e2e.connector.file.ftp;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason =
                "1.The apache-compress version is not compatible with apache-poi. 2.Spark Engine is not compatible with commons-net")
@Slf4j
public class FtpFileIT extends TestSuiteBase implements TestResource {

    private static final String FTP_IMAGE = "fauria/vsftpd:latest";

    private static final String ftp_CONTAINER_HOST = "ftp";

    private static final int FTP_PORT = 21;

    private static final String USERNAME = "seatunnel";

    private static final String PASSWORD = "pass";

    private GenericContainer<?> ftpContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        ftpContainer =
                new GenericContainer<>(FTP_IMAGE)
                        .withExposedPorts(FTP_PORT)
                        .withNetwork(NETWORK)
                        .withExposedPorts(FTP_PORT)
                        .withNetworkAliases(ftp_CONTAINER_HOST)
                        .withEnv("FILE_OPEN_MODE", "0666")
                        .withEnv("WRITE_ENABLE", "YES")
                        .withEnv("ALLOW_WRITEABLE_CHROOT", "YES")
                        .withEnv("ANONYMOUS_ENABLE", "YES")
                        .withEnv("LOCAL_ENABLE", "YES")
                        .withEnv("LOCAL_UMASK", "000")
                        .withEnv("FTP_USER", USERNAME)
                        .withEnv("FTP_PASS", PASSWORD)
                        .withEnv("PASV_ADDRESS", "0.0.0.0")
                        .withLogConsumer(new Slf4jLogConsumer(log))
                        .withPrivilegedMode(true);

        ftpContainer.setPortBindings(Collections.singletonList("21:21"));
        ftpContainer.start();
        Startables.deepStart(Stream.of(ftpContainer)).join();
        log.info("ftp container started");

        Path jsonPath = ContainerUtil.getResourcesFile("/json/e2e.json").toPath();
        Path textPath = ContainerUtil.getResourcesFile("/text/e2e.txt").toPath();
        Path excelPath = ContainerUtil.getResourcesFile("/excel/e2e.xlsx").toPath();

        ftpContainer.copyFileToContainer(
                MountableFile.forHostPath(jsonPath),
                "/home/vsftpd/seatunnel/tmp/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json");
        ftpContainer.copyFileToContainer(
                MountableFile.forHostPath(textPath),
                "/home/vsftpd/seatunnel/tmp/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt");
        ftpContainer.copyFileToContainer(
                MountableFile.forHostPath(excelPath),
                "/home/vsftpd/seatunnel/tmp/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx");
        ftpContainer.execInContainer("sh", "-c", "chmod -R 777 /home/vsftpd/seatunnel/");
        ftpContainer.execInContainer("sh", "-c", "chown -R ftp:ftp /home/vsftpd/seatunnel/");
    }

    @TestTemplate
    public void testFtpFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        // test write ftp excel file
        Container.ExecResult excelWriteResult =
                container.executeJob("/excel/fake_source_to_ftp_excel.conf");
        Assertions.assertEquals(0, excelWriteResult.getExitCode(), excelWriteResult.getStderr());
        // test read ftp excel file
        Container.ExecResult excelReadResult =
                container.executeJob("/excel/ftp_excel_to_assert.conf");
        Assertions.assertEquals(0, excelReadResult.getExitCode(), excelReadResult.getStderr());
        // test read ftp excel file with projection
        Container.ExecResult excelProjectionReadResult =
                container.executeJob("/excel/ftp_excel_projection_to_assert.conf");
        Assertions.assertEquals(
                0, excelProjectionReadResult.getExitCode(), excelProjectionReadResult.getStderr());
        // test write ftp text file
        Container.ExecResult textWriteResult =
                container.executeJob("/text/fake_to_ftp_file_text.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        // test read skip header
        Container.ExecResult textWriteAndSkipResult =
                container.executeJob("/text/ftp_file_text_skip_headers.conf");
        Assertions.assertEquals(0, textWriteAndSkipResult.getExitCode());
        // test read ftp text file
        Container.ExecResult textReadResult =
                container.executeJob("/text/ftp_file_text_to_assert.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());
        // test read ftp text file with projection
        Container.ExecResult textProjectionResult =
                container.executeJob("/text/ftp_file_text_projection_to_assert.conf");
        Assertions.assertEquals(0, textProjectionResult.getExitCode());
        // test write ftp json file
        Container.ExecResult jsonWriteResult =
                container.executeJob("/json/fake_to_ftp_file_json.conf");
        Assertions.assertEquals(0, jsonWriteResult.getExitCode());
        // test read ftp json file
        Container.ExecResult jsonReadResult =
                container.executeJob("/json/ftp_file_json_to_assert.conf");
        Assertions.assertEquals(0, jsonReadResult.getExitCode());
        // test write ftp parquet file
        Container.ExecResult parquetWriteResult =
                container.executeJob("/parquet/fake_to_ftp_file_parquet.conf");
        Assertions.assertEquals(0, parquetWriteResult.getExitCode());
        // test write ftp orc file
        Container.ExecResult orcWriteResult =
                container.executeJob("/orc/fake_to_ftp_file_orc.conf");
        Assertions.assertEquals(0, orcWriteResult.getExitCode());
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (ftpContainer != null) {
            ftpContainer.close();
        }
    }
}
