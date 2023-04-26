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

package org.apache.seatunnel.e2e.connector.file.fstp;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        disabledReason = "The apache-compress version is not compatible with apache-poi")
@Slf4j
public class SftpFileIT extends TestSuiteBase implements TestResource {

    private static final String SFTP_IMAGE = "atmoz/sftp:latest";

    private static final String SFTP_CONTAINER_HOST = "sft";

    private static final int SFTP_PORT = 22;

    private static final int SFTP_BIND_PORT = 2222;

    private static final String USERNAME = "foo";

    private static final String PASSWORD = "pass";

    private GenericContainer<?> sftpContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {

        sftpContainer =
                new GenericContainer<>(SFTP_IMAGE)
                        .withEnv("SFTP_USERS", USERNAME + ":" + PASSWORD)
                        .withCommand(USERNAME + ":" + PASSWORD + ":::tmp")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(SFTP_CONTAINER_HOST)
                        .withExposedPorts(SFTP_PORT);

        sftpContainer.setPortBindings(Collections.singletonList(SFTP_BIND_PORT + ":" + SFTP_PORT));
        sftpContainer.start();
        Startables.deepStart(Stream.of(sftpContainer)).join();
        log.info("Sftp container started");
        String host = sftpContainer.getContainerIpAddress();
        int mappedPort = sftpContainer.getMappedPort(SFTP_PORT);
        initDate(host, mappedPort);
    }

    @TestTemplate
    public void testSftpFileReadAndWrite(TestContainer container)
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
                0, excelProjectionReadResult.getExitCode(), excelProjectionReadResult.getStderr());
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

            tmpFileToSftpServerWithCustomPath(
                    host, mappedPort, USERNAME, PASSWORD, path.toFile(), remotePath);
        }
    }

    public static void tmpFileToSftpServerWithCustomPath(
            String host,
            int port,
            String username,
            String password,
            File localFilePath,
            String remotePath) {
        JSch jsch = new JSch();
        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            session = jsch.getSession(username, host, port);
            session.setPassword(password);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            config.put("PreferredAuthentications", "password");
            session.setConfig(config);
            session.connect();

            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            StringTokenizer token = new StringTokenizer(remotePath, "/");
            String path = "";

            while (token.hasMoreTokens()) {
                path = path + "/" + token.nextToken();
                try {
                    channelSftp.cd(path);
                } catch (SftpException e) {
                    channelSftp.mkdir(path);
                    channelSftp.cd(path);
                }
            }

            FileInputStream fis = new FileInputStream(localFilePath);

            channelSftp.put(fis, localFilePath.getName());

            fis.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (channelSftp != null) {
                channelSftp.exit();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (sftpContainer != null) {
            sftpContainer.close();
        }
    }
}
