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

package org.apache.seatunnel.e2e.connector.file.local;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {EngineType.SEATUNNEL},
        disabledReason = "The apache-compress version is not compatible with apache-poi")
@Slf4j
public class FtpFileIT extends TestSuiteBase implements TestResource {

    private static final String FTP_IMAGE = "fauria/vsftpd:latest";

    private static final String SFTP_CONTAINER_HOST = "ftp";

    private static final int FTP_PORT = 21;

    private static final int FTP_BIND_PORT = 2121;

    private static final String USERNAME = "foo";

    private static final String PASSWORD = "pass";

    private GenericContainer<?> ftpContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {

        ftpContainer = new GenericContainer<>(FTP_IMAGE)
                .withExposedPorts(FTP_PORT)
                .withNetwork(NETWORK)
                .withNetworkAliases(SFTP_CONTAINER_HOST)
                .withEnv("FTP_USER", USERNAME)
                .withEnv("FTP_PASS", PASSWORD)
                .withEnv("PASV_ADDRESS", "0.0.0.0") // Use this for passive mode
                .withEnv("PASV_MIN_PORT", "21100")
                .withEnv("PASV_MAX_PORT", "21110");

        ftpContainer.setPortBindings(Collections.singletonList(FTP_BIND_PORT + ":" + FTP_PORT));
        ftpContainer.start();
        Startables.deepStart(Stream.of(ftpContainer)).join();
        log.info("Sftp container started");
        String host = ftpContainer.getContainerIpAddress();
        int mappedPort = ftpContainer.getMappedPort(FTP_PORT);
        initDate(host, mappedPort);
    }

    @TestTemplate
    public void testftpFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult excelWriteResult =
                container.executeJob("/excel/fakesource_to_sftp_excel.conf");
        Assertions.assertEquals(0, excelWriteResult.getExitCode(), excelWriteResult.getStderr());
        Container.ExecResult excelReadResult =
                container.executeJob("/excel/sftp_excel_to_assert.conf");
        Assertions.assertEquals(0, excelReadResult.getExitCode(), excelReadResult.getStderr());
        Container.ExecResult excelProjectionReadResult =
                container.executeJob("/excel/sftp_excel_projection_to_assert.conf");
        Assertions.assertEquals(
                0, excelReadResult.getExitCode(), excelProjectionReadResult.getStderr());
        // test write sftp text file
        Container.ExecResult textWriteResult =
                container.executeJob("/text/fake_to_sftp_file_text.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        // test read skip header
        Container.ExecResult textWriteAndSkipResult =
                container.executeJob("/text/sftp_file_text_skip_headers.conf");
        Assertions.assertEquals(0, textWriteAndSkipResult.getExitCode());
        // test read sftp text file
        Container.ExecResult textReadResult =
                container.executeJob("/text/sftp_file_text_to_assert.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());
        // test read sftp text file with projection
        Container.ExecResult textProjectionResult =
                container.executeJob("/text/sftp_file_text_projection_to_assert.conf");
        Assertions.assertEquals(0, textProjectionResult.getExitCode());
        // test write sftp json file
        Container.ExecResult jsonWriteResult =
                container.executeJob("/json/fake_to_sftp_file_json.conf");
        Assertions.assertEquals(0, jsonWriteResult.getExitCode());
        // test read sftp json file
        Container.ExecResult jsonReadResult =
                container.executeJob("/json/sftp_file_json_to_assert.conf");
        Assertions.assertEquals(0, jsonReadResult.getExitCode());
    }

    public static void initDate(String host, int mappedPort) {
        Map<String, String> filesMap = new HashMap<>();
        filesMap.put(
                "/excel/e2e.xlsx", "/tmp/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/");
        filesMap.put("/json/e2e.json", "/tmp/seatunnel/read/json/name=tyrantlucifer/hobby=coding/");
        filesMap.put("/text/e2e.txt", "/tmp/seatunnel/read/text/name=tyrantlucifer/hobby=coding/");

        for (Map.Entry<String, String> entry : filesMap.entrySet()) {
            String localPath = entry.getKey();
            String remotePath = entry.getValue();
            Path path = ContainerUtil.getResourcesFile(localPath).toPath();
            uploadFileToFtpServer(
                    host, mappedPort, USERNAME, PASSWORD, path, remotePath);
        }
    }

    public static void uploadFileToFtpServer(String host, int port, String username, String password, Path localFilePath, String remotePath) {
        FTPClient ftpClient = new FTPClient();
        try {
            // Connect and login to the FTP server
            ftpClient.connect(host, port);
            ftpClient.login(username, password);

            // Set the file transfer mode to binary
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);

            // Separate remote directory and filename from remotePath
            Path remoteFilePath = Paths.get(remotePath);
            String remoteDir = remoteFilePath.getParent().toString();
            String remoteFilename = remoteFilePath.getFileName().toString();

            // Create the remote directory
            createRemoteDirectory(ftpClient, remoteDir);

            // Change to the remote directory
            ftpClient.changeWorkingDirectory(remoteDir);

            // Upload the file
            try (InputStream inputStream = Files.newInputStream(localFilePath)) {
                ftpClient.storeFile(remoteFilename, inputStream);
            }

            System.out.println("File uploaded successfully.");

            // Logout and disconnect from the FTP server
            ftpClient.logout();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.disconnect();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void createRemoteDirectory(FTPClient ftpClient, String remoteDir) throws IOException {
        String[] dirs = remoteDir.split("/");
        for (String dir : dirs) {
            if (!dir.isEmpty()) {
                if (!ftpClient.changeWorkingDirectory(dir)) {
                    ftpClient.makeDirectory(dir);
                    ftpClient.changeWorkingDirectory(dir);
                }
            }
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (ftpContainer != null) {
            ftpContainer.close();
        }
    }
}