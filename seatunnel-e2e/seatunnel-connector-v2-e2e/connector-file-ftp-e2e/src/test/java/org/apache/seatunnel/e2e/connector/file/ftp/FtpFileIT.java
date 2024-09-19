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
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.github.dockerjava.core.command.ExecStartResultCallback;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                "/home/vsftpd/seatunnel/tmp/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/vsftpd/seatunnel/tmp/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "text/e2e-txt.zip",
                "/home/vsftpd/seatunnel/tmp/seatunnel/read/zip/txt/single/e2e-txt.zip",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/excel/e2e.xlsx",
                "/home/vsftpd/seatunnel/tmp/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/excel/e2e.xlsx",
                "/home/vsftpd/seatunnel/tmp/seatunnel/read/excel_filter/name=tyrantlucifer/hobby=coding/e2e_filter.xlsx",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/excel/e2e.xlsx", "/home/vsftpd/seatunnel/e2e.xlsx", ftpContainer);

        ftpContainer.execInContainer("sh", "-c", "chmod -R 777 /home/vsftpd/seatunnel/");
        ftpContainer.execInContainer("sh", "-c", "chown -R ftp:ftp /home/vsftpd/seatunnel/");
    }

    @TestTemplate
    public void testFtpFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        // test write ftp excel file
        helper.execute("/excel/fake_source_to_ftp_excel.conf");
        // test read ftp excel file
        helper.execute("/excel/ftp_excel_to_assert.conf");
        // test read ftp excel file with projection
        helper.execute("/excel/ftp_excel_projection_to_assert.conf");
        // test read ftp excel file with filter
        helper.execute("/excel/ftp_filter_excel_to_assert.conf");
        // test write ftp text file
        helper.execute("/text/fake_to_ftp_file_text.conf");
        // test read skip header
        helper.execute("/text/ftp_file_text_skip_headers.conf");
        // test read ftp text file
        helper.execute("/text/ftp_file_text_to_assert.conf");
        // test read ftp text file with projection
        helper.execute("/text/ftp_file_text_projection_to_assert.conf");
        // test read ftp zip text file
        helper.execute("/text/ftp_file_zip_text_to_assert.conf");
        // test write ftp json file
        helper.execute("/json/fake_to_ftp_file_json.conf");
        // test read ftp json file
        helper.execute("/json/ftp_file_json_to_assert.conf");
        // test write ftp parquet file
        helper.execute("/parquet/fake_to_ftp_file_parquet.conf");
        // test write ftp orc file
        helper.execute("/orc/fake_to_ftp_file_orc.conf");
        // test write ftp root path excel file
        helper.execute("/excel/fake_source_to_ftp_root_path_excel.conf");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK},
            disabledReason = "Flink dosen't support multi-table at now")
    public void testMultipleTableAndSaveMode(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        // test mult table and save_mode:RECREATE_SCHEMA DROP_DATA
        String homePath = "/home/vsftpd/seatunnel";
        String path1 = "/tmp/seatunnel_mult/text/source_1";
        String path2 = "/tmp/seatunnel_mult/text/source_2";
        Assertions.assertEquals(getFileListFromContainer(homePath + path1).size(), 0);
        Assertions.assertEquals(getFileListFromContainer(homePath + path2).size(), 0);
        helper.execute("/text/multiple_table_fake_to_ftp_file_text.conf");
        Assertions.assertEquals(getFileListFromContainer(homePath + path1).size(), 1);
        Assertions.assertEquals(getFileListFromContainer(homePath + path2).size(), 1);
        helper.execute("/text/multiple_table_fake_to_ftp_file_text.conf");
        Assertions.assertEquals(getFileListFromContainer(homePath + path1).size(), 1);
        Assertions.assertEquals(getFileListFromContainer(homePath + path2).size(), 1);
        // test mult table and save_mode:CREATE_SCHEMA_WHEN_NOT_EXIST APPEND_DATA
        String path3 = "/tmp/seatunnel_mult2/text/source_1";
        String path4 = "/tmp/seatunnel_mult2/text/source_2";
        Assertions.assertEquals(getFileListFromContainer(homePath + path3).size(), 0);
        Assertions.assertEquals(getFileListFromContainer(homePath + path4).size(), 0);
        helper.execute("/text/multiple_table_fake_to_ftp_file_text_2.conf");
        Assertions.assertEquals(getFileListFromContainer(homePath + path3).size(), 1);
        Assertions.assertEquals(getFileListFromContainer(homePath + path4).size(), 1);
        helper.execute("/text/multiple_table_fake_to_ftp_file_text_2.conf");
        Assertions.assertEquals(getFileListFromContainer(homePath + path3).size(), 2);
        Assertions.assertEquals(getFileListFromContainer(homePath + path4).size(), 2);
    }

    @SneakyThrows
    private List<String> getFileListFromContainer(String path) {
        String command = "ls -1 " + path;
        ExecCreateCmdResponse execCreateCmdResponse =
                dockerClient
                        .execCreateCmd(ftpContainer.getContainerId())
                        .withCmd("sh", "-c", command)
                        .withAttachStdout(true)
                        .withAttachStderr(true)
                        .exec();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        dockerClient
                .execStartCmd(execCreateCmdResponse.getId())
                .exec(new ExecStartResultCallback(outputStream, System.err))
                .awaitCompletion();

        String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
        List<String> fileList = new ArrayList<>();
        log.info("container path file list is :{}", output);
        String[] files = output.split("\n");
        for (String file : files) {
            if (StringUtils.isNotEmpty(file)) {
                log.info("container path file name is :{}", file);
                fileList.add(file);
            }
        }
        return fileList;
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (ftpContainer != null) {
            ftpContainer.close();
        }
    }
}
