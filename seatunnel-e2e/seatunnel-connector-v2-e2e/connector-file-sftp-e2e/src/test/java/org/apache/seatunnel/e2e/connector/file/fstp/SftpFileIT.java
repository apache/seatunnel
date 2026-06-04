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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        disabledReason = "The apache-compress version is not compatible with apache-poi")
@Slf4j
public class SftpFileIT extends TestSuiteBase implements TestResource {

    private static final String SFTP_IMAGE = "atmoz/sftp:alpine-3.7";

    private static final String SFTP_CONTAINER_HOST = "sftp";

    private static final int SFTP_PORT = 22;

    private static final String SFTP_CONTAINER_HOME = "/home/seatunnel";

    private static final String USERNAME = "seatunnel";

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
                        .withExposedPorts(SFTP_PORT)
                        .waitingFor(Wait.forListeningPort());

        Startables.deepStart(Stream.of(sftpContainer)).join();
        log.info("Sftp container started");

        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                "/home/seatunnel/tmp/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e-text.zip",
                "/home/seatunnel/tmp/seatunnel/read/zip/text/e2e-text.zip",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/excel/e2e.xlsx",
                "/home/seatunnel/tmp/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/excel/e2e.xlsx",
                "/home/seatunnel/tmp/seatunnel/read/excel_filter/name=tyrantlucifer/hobby=coding/e2e_filter.xlsx",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/xml/e2e.xml",
                "/home/seatunnel/tmp/seatunnel/read/xml/name=tyrantlucifer/hobby=coding/e2e.xml",
                sftpContainer);

        // Windows does not support files with wildcard characters. We can rename `e2e.txt` to
        // `e*e.txt` when copying to a container
        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e*e.txt",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/wildcard/e2e.txt",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/recursive/e2e.txt",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/recursive/subdir/e2e.txt",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/recursive/subdir/deeper/e2e.txt",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/recursive/subdir/deeper/final/e2e.txt",
                sftpContainer);

        Container.ExecResult chownResult =
                sftpContainer.execInContainer(
                        "sh", "-c", "chown -R seatunnel /home/seatunnel/tmp/");
        Assertions.assertEquals(0, chownResult.getExitCode(), chownResult.getStderr());
    }

    @TestTemplate
    public void testFtpToAssertJsonFilter(TestContainer container)
            throws IOException, InterruptedException {

        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                "/home/seatunnel/tmp/seatunnel/read/filter/json/name=tyrantlucifer/hobby=codin/e2e.json",
                sftpContainer);
        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                "/home/seatunnel/tmp/seatunnel/read/filter/json2025/name=tyrantlucifer/hobby=coding/e2e_2025.json",
                sftpContainer);
        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/filter/json2025/name=tyrantlucifer/hobby=coding/e2e_2025.txt",
                sftpContainer);
        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                "/home/seatunnel/tmp/seatunnel/read/filter/json2024/name=tyrantlucifer/hobby=coding/e2e_2024.json",
                sftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                "/home/seatunnel/tmp/seatunnel/read/filter/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                sftpContainer);
        Container.ExecResult chownResult2 =
                sftpContainer.execInContainer(
                        "sh", "-c", "chown -R seatunnel /home/seatunnel/tmp/");
        Assertions.assertEquals(0, chownResult2.getExitCode(), chownResult2.getStderr());

        String filterPath = "/home/seatunnel/tmp/seatunnel/read/filter";
        try {
            TestHelper helper = new TestHelper(container);
            helper.execute("/json/sftp_to_access_for_json_path_filter.conf");
            helper.execute("/json/sftp_to_access_for_json_name_filter.conf");
        } finally {
            deleteFileFromContainer(filterPath);
        }
    }

    @TestTemplate
    public void testSftpFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        // test write sftp excel file
        helper.execute("/excel/fakesource_to_sftp_excel.conf");
        // test read sftp excel file
        helper.execute("/excel/sftp_excel_to_assert.conf");
        // test read sftp excel file with projection
        helper.execute("/excel/sftp_excel_projection_to_assert.conf");
        // test read sftp excel file with filter pattern
        helper.execute("/excel/sftp_filter_excel_to_assert.conf");
        // test write sftp text file
        helper.execute("/text/fake_to_sftp_file_text.conf");
        // test read skip header
        helper.execute("/text/sftp_file_text_skip_headers.conf");
        // test read sftp text file
        helper.execute("/text/sftp_file_text_to_assert.conf");
        // test read sftp text file with projection
        helper.execute("/text/sftp_file_text_projection_to_assert.conf");
        // test read sftp zip text file
        helper.execute("/text/sftp_file_zip_text_to_assert.conf");
        // test read file wit wildcard character, should match tmp/seatunnel/read/wildcard/e*e.txt
        // and tmp/seatunnel/read/wildcard/e2e.txt
        helper.execute("/text/sftp_file_text_wildcard_character_to_assert.conf");
        // test write sftp json file
        helper.execute("/json/fake_to_sftp_file_json.conf");
        // test read sftp json file
        helper.execute("/json/sftp_file_json_to_assert.conf");
        // test write sftp xml file
        helper.execute("/xml/fake_to_sftp_file_xml.conf");
        // test read sftp xml file
        helper.execute("/xml/sftp_file_xml_to_assert.conf");
        // test sftp source support multipleTable
        String homePath = "/home/seatunnel";
        String sink01 = "/tmp/multipleSource/seatunnel/json/fake01";
        String sink02 = "/tmp/multipleSource/seatunnel/json/fake02";
        deleteFileFromContainer(homePath + sink01);
        deleteFileFromContainer(homePath + sink02);
        helper.execute("/json/sftp_file_json_to_assert_with_multipletable.conf");
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    1, getFileListFromContainer(homePath + sink01).size());
                            Assertions.assertEquals(
                                    1, getFileListFromContainer(homePath + sink02).size());
                        });
    }

    @TestTemplate
    public void testSftpBinaryUpdateModeDistcp(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        try {
            putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/src/test.bin", "abc");

            TestHelper helper = new TestHelper(container);
            helper.execute("/text/sftp_binary_update_distcp.conf");
            Assertions.assertEquals(
                    "abc",
                    readSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/test.bin"));

            // Make target newer with same length, distcp strategy should SKIP overwrite.
            String srcPath = SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/src/test.bin";
            String dstPath = SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/test.bin";
            long srcMtime = getSftpFileMtimeSeconds(srcPath);
            waitUntilContainerTimeAfter(srcMtime);
            putSftpFile(dstPath, "zzz");
            helper.execute("/text/sftp_binary_update_distcp.conf");
            Assertions.assertEquals(
                    "zzz",
                    readSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/test.bin"));

            // Change source length, distcp strategy should COPY overwrite.
            putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/src/test.bin", "abcd");
            helper.execute("/text/sftp_binary_update_distcp.conf");
            Assertions.assertEquals(
                    "abcd",
                    readSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/test.bin"));
        } finally {
            deleteFileFromContainer(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update");
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testSftpBinaryUpdateModeContinuousDiscoveryDistcp(TestContainer container)
            throws IOException, InterruptedException {
        resetContinuousTestPath();
        try {
            putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous/src/test1.bin", "abc");

            String jobId = String.valueOf(JobIdGenerator.newJobId());
            CompletableFuture<Container.ExecResult> jobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/text/sftp_binary_update_distcp_continuous.conf",
                                            jobId);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "abc",
                                            readSftpFile(
                                                    SFTP_CONTAINER_HOME
                                                            + "/tmp/seatunnel/continuous/dst/test1.bin")));

            String mtimePath = SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous/dst/test1.bin";
            long firstMtimeSeconds = getSftpFileMtimeSeconds(mtimePath);
            Awaitility.await()
                    .during(6, TimeUnit.SECONDS)
                    .atMost(15, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                long currentMtime = getSftpFileMtimeSeconds(mtimePath);
                                Assertions.assertEquals(
                                        firstMtimeSeconds,
                                        currentMtime,
                                        "Continuous discovery should skip unchanged files in update mode.");
                            });

            putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous/src/test2.bin", "def");
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "def",
                                            readSftpFile(
                                                    SFTP_CONTAINER_HOME
                                                            + "/tmp/seatunnel/continuous/dst/test2.bin")));

            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

            Container.ExecResult execResult;
            try {
                execResult = jobFuture.get(120, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Wait continuous job exit failed.", e);
            }
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        } finally {
            deleteFileFromContainer(SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous");
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testSftpBinaryUpdateModeContinuousDiscoveryWithNonRecursiveScan(
            TestContainer container) throws IOException, InterruptedException {
        resetContinuousTestPath();
        try {
            String jobId = String.valueOf(JobIdGenerator.newJobId());
            CompletableFuture<Container.ExecResult> jobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/text/sftp_binary_update_distcp_continuous_non_recursive.conf",
                                            jobId);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous/src/root.bin", "root");
            putSftpFile(
                    SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous/src/subdir/nested.bin",
                    "nested");

            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "root",
                                            readSftpFile(
                                                    SFTP_CONTAINER_HOME
                                                            + "/tmp/seatunnel/continuous/dst/root.bin")));

            Thread.sleep(3000);
            Assertions.assertFalse(
                    isSftpFileExists(
                            SFTP_CONTAINER_HOME
                                    + "/tmp/seatunnel/continuous/dst/subdir/nested.bin"));

            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

            Container.ExecResult execResult;
            try {
                execResult = jobFuture.get(120, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Wait continuous job exit failed.", e);
            }
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        } finally {
            deleteFileFromContainer(SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous");
        }
    }

    @TestTemplate
    public void testSftpBinaryUpdateModeDistcpWithNonRecursiveScan(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/src/root.bin", "root-updated-v2");
        putSftpFile(
                SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/src/subdir/nested.bin",
                "nest-updated-v2");
        putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/root.bin", "root-stale-v1");
        putSftpFile(
                SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/subdir/nested.bin",
                "nest-stale-v1");

        TestHelper helper = new TestHelper(container);
        helper.execute("/text/sftp_binary_update_non_recursive_distcp.conf");

        Assertions.assertEquals(
                "root-updated-v2",
                readSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/root.bin"));
        Assertions.assertEquals(
                "nest-stale-v1",
                readSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/subdir/nested.bin"));

        deleteFileFromContainer(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update");
    }

    @TestTemplate
    public void testSftpBinaryUpdateModeStrictChecksumSkipsNestedChangesWithNonRecursiveScan(
            TestContainer container) throws IOException, InterruptedException {
        resetUpdateTestPath();
        putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/src/root.bin", "root-same-v1");
        putSftpFile(
                SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/src/subdir/nested.bin", "nest-new-v1");
        putSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/root.bin", "root-same-v1");
        putSftpFile(
                SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/subdir/nested.bin", "nest-old-v1");

        TestHelper helper = new TestHelper(container);
        helper.execute("/text/sftp_binary_update_non_recursive_strict_checksum.conf");

        Assertions.assertEquals(
                "root-same-v1",
                readSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/root.bin"));
        Assertions.assertEquals(
                "nest-old-v1",
                readSftpFile(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update/dst/subdir/nested.bin"));

        deleteFileFromContainer(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update");
    }

    @TestTemplate
    public void testMultipleTableAndSaveMode(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        // test mult table and save_mode:RECREATE_SCHEMA DROP_DATA
        String homePath = "/home/seatunnel";
        String path1 = "/tmp/multiple_1/seatunnel/text/source_1";
        String path2 = "/tmp/multiple_1/seatunnel/text/source_2";
        deleteFileFromContainer(homePath + path1);
        deleteFileFromContainer(homePath + path2);
        Assertions.assertEquals(0, getFileListFromContainer(homePath + path1).size());
        Assertions.assertEquals(0, getFileListFromContainer(homePath + path2).size());
        helper.execute("/text/multiple_fake_to_sftp_file_text_recreate_schema.conf");
        awaitFileCount(homePath + path1, 1);
        awaitFileCount(homePath + path2, 1);
        helper.execute("/text/multiple_fake_to_sftp_file_text_recreate_schema.conf");
        awaitFileCount(homePath + path1, 1);
        awaitFileCount(homePath + path2, 1);
        // test mult table and save_mode:CREATE_SCHEMA_WHEN_NOT_EXIST APPEND_DATA
        String path3 = "/tmp/multiple_2/seatunnel/text/source_1";
        String path4 = "/tmp/multiple_2/seatunnel/text/source_2";
        deleteFileFromContainer(homePath + path3);
        deleteFileFromContainer(homePath + path4);
        Assertions.assertEquals(0, getFileListFromContainer(homePath + path3).size());
        Assertions.assertEquals(0, getFileListFromContainer(homePath + path4).size());
        helper.execute("/text/multiple_fake_to_sftp_file_text_append.conf");
        awaitFileCount(homePath + path3, 1);
        awaitFileCount(homePath + path4, 1);
        helper.execute("/text/multiple_fake_to_sftp_file_text_append.conf");
        awaitFileCount(homePath + path3, 2);
        awaitFileCount(homePath + path4, 2);
    }

    private void awaitFileCount(String containerPath, int expectedCount) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedCount,
                                        getFileListFromContainer(containerPath).size(),
                                        "Expected "
                                                + expectedCount
                                                + " files in "
                                                + containerPath));
    }

    private void resetUpdateTestPath() throws IOException, InterruptedException {
        deleteFileFromContainer(SFTP_CONTAINER_HOME + "/tmp/seatunnel/update");
        Container.ExecResult mkdirResult =
                sftpContainer.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + SFTP_CONTAINER_HOME
                                + "/tmp/seatunnel/update/src "
                                + SFTP_CONTAINER_HOME
                                + "/tmp/seatunnel/update/dst "
                                + SFTP_CONTAINER_HOME
                                + "/tmp/seatunnel/update/tmp");
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());
        sftpContainer.execInContainer(
                "sh",
                "-c",
                "chmod -R 777 " + SFTP_CONTAINER_HOME + "/tmp/seatunnel/update || true");
    }

    private void resetContinuousTestPath() throws IOException, InterruptedException {
        deleteFileFromContainer(SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous");
        Container.ExecResult mkdirResult =
                sftpContainer.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + SFTP_CONTAINER_HOME
                                + "/tmp/seatunnel/continuous/src "
                                + SFTP_CONTAINER_HOME
                                + "/tmp/seatunnel/continuous/dst "
                                + SFTP_CONTAINER_HOME
                                + "/tmp/seatunnel/continuous/tmp");
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());
        sftpContainer.execInContainer(
                "sh",
                "-c",
                "chmod -R 777 " + SFTP_CONTAINER_HOME + "/tmp/seatunnel/continuous || true");
    }

    private void putSftpFile(String containerPath, String content)
            throws IOException, InterruptedException {
        String command =
                "mkdir -p $(dirname '"
                        + containerPath
                        + "') && printf '"
                        + content
                        + "' > '"
                        + containerPath
                        + "' && chmod 666 '"
                        + containerPath
                        + "'";
        Container.ExecResult putResult = sftpContainer.execInContainer("sh", "-c", command);
        Assertions.assertEquals(0, putResult.getExitCode(), putResult.getStderr());
    }

    private String readSftpFile(String containerPath) throws IOException, InterruptedException {
        Container.ExecResult catResult =
                sftpContainer.execInContainer("sh", "-c", "cat '" + containerPath + "'");
        Assertions.assertEquals(0, catResult.getExitCode(), catResult.getStderr());
        return catResult.getStdout() == null ? "" : catResult.getStdout().trim();
    }

    private boolean isSftpFileExists(String containerPath)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                sftpContainer.execInContainer("sh", "-c", "test -f '" + containerPath + "'");
        return result.getExitCode() == 0;
    }

    private void waitUntilContainerTimeAfter(long epochSeconds) {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(
                        () -> {
                            Container.ExecResult r =
                                    sftpContainer.execInContainer("sh", "-c", "date +%s");
                            return Long.parseLong(r.getStdout().trim()) > epochSeconds;
                        });
    }

    private long getSftpFileMtimeSeconds(String containerPath)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                sftpContainer.execInContainer("sh", "-c", "stat -c %Y '" + containerPath + "'");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        return Long.parseLong(result.getStdout().trim());
    }

    @SneakyThrows
    private List<String> getFileListFromContainer(String path) {
        Container.ExecResult result = sftpContainer.execInContainer("sh", "-c", "ls -1 " + path);
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
        sftpContainer.execInContainer("sh", "-c", "rm -rf " + path);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (sftpContainer != null) {
            sftpContainer.close();
        }
    }
}
