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

package org.apache.seatunnel.e2e.connector.file.smb;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        disabledReason = "The apache-compress version is not compatible with apache-poi")
@Slf4j
public class SmbFileIT extends TestSuiteBase implements TestResource {

    private static final String SMB_IMAGE = "dperson/samba";

    private static final String SMB_CONTAINER_HOST = "smb";

    private static final int SMB_PORT = 445;

    private static final String SHARE_PATH = "/share";

    private static final String USERNAME = "seatunnel";

    private static final String PASSWORD = "pass";

    private static final String SHARE_NAME = "data";

    private GenericContainer<?> smbContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        smbContainer =
                new GenericContainer<>(SMB_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(SMB_CONTAINER_HOST)
                        .withExposedPorts(SMB_PORT)
                        .withCommand(
                                "-u",
                                USERNAME + ";" + PASSWORD,
                                "-s",
                                SHARE_NAME
                                        + ";"
                                        + SHARE_PATH
                                        + ";no;no;no;"
                                        + USERNAME
                                        + ";none;"
                                        + USERNAME)
                        .waitingFor(Wait.forListeningPort());

        Startables.deepStart(Stream.of(smbContainer)).join();
        log.info("SMB container started");

        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                SHARE_PATH + "/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                smbContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                SHARE_PATH + "/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                smbContainer);

        Container.ExecResult chownResult =
                smbContainer.execInContainer("sh", "-c", "chmod -R 777 " + SHARE_PATH);
        Assertions.assertEquals(0, chownResult.getExitCode(), chownResult.getStderr());
    }

    @TestTemplate
    public void testSmbFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/json/fake_to_smb_file_json.conf");
        helper.execute("/json/smb_file_json_to_assert.conf");
        helper.execute("/text/fake_to_smb_file_text.conf");
        helper.execute("/text/smb_file_text_to_assert.conf");
    }

    @SneakyThrows
    private List<String> getFileListFromContainer(String path) {
        Container.ExecResult result = smbContainer.execInContainer("sh", "-c", "ls -1 " + path);
        if (result.getExitCode() != 0) {
            log.info("ls failed for path {}: {}", path, result.getStderr());
            return new ArrayList<>();
        }
        String output = result.getStdout() == null ? "" : result.getStdout().trim();
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

    @SneakyThrows
    private void deleteFileFromContainer(String path) {
        smbContainer.execInContainer("sh", "-c", "rm -rf " + path);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (smbContainer != null) {
            smbContainer.close();
        }
    }
}
