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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.catalog.FtpFileCatalog;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.config.FtpConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

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
public class FtpFileMultipleSinkIT extends TestSuiteBase implements TestResource {

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

        ftpContainer.execInContainer("sh", "-c", "chmod -R 777 /home/vsftpd/seatunnel/");
        ftpContainer.execInContainer("sh", "-c", "chown -R ftp:ftp /home/vsftpd/seatunnel/");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "Fink test is multi-node, LocalFile connector will use different containers for obtaining files")
    public void testFtpFileMultipleWriteWithSaveMode(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        // test save_mode
        String path1 = "/tmp/seatunnel/text/source_1";
        String path2 = "/tmp/seatunnel/text/source_2";
        Assertions.assertEquals(getFileListFromContainer(path1).size(), 0);
        Assertions.assertEquals(getFileListFromContainer(path2).size(), 0);
        helper.execute("/text/multiple_table_fake_to_ftp_file_text.conf");
        Assertions.assertEquals(getFileListFromContainer(path1).size(), 1);
        Assertions.assertEquals(getFileListFromContainer(path2).size(), 1);
        helper.execute("/text/multiple_table_fake_to_ftp_file_text.conf");
        Assertions.assertEquals(getFileListFromContainer(path1).size(), 1);
        Assertions.assertEquals(getFileListFromContainer(path2).size(), 1);
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

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "Fink test is multi-node, LocalFile connector will use different containers for obtaining files")
    public void testLocalFileCatalog(TestContainer container)
            throws IOException, InterruptedException {
        String defaultFS = String.format("ftp://%s:%s", ftp_CONTAINER_HOST, FTP_PORT);
        final FtpFileCatalog ftpFileCatalog =
                new FtpFileCatalog(
                        new HadoopFileSystemProxy(new FtpConf(defaultFS)),
                        "/tmp/seatunnel/json/test1",
                        FileSystemType.FTP.getFileSystemPluginName());
        final TablePath tablePath = TablePath.DEFAULT;
        Assertions.assertFalse(ftpFileCatalog.tableExists(tablePath));
        ftpFileCatalog.createTable(null, null, false);
        Assertions.assertTrue(ftpFileCatalog.tableExists(tablePath));
        Assertions.assertFalse(ftpFileCatalog.isExistsData(tablePath));
        ftpFileCatalog.dropTable(tablePath, false);
        Assertions.assertFalse(ftpFileCatalog.tableExists(tablePath));
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (ftpContainer != null) {
            ftpContainer.close();
        }
    }
}
