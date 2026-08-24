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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.github.dockerjava.core.command.ExecStartResultCallback;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason =
                "1.The apache-compress version is not compatible with apache-poi. 2.Spark Engine is not compatible with commons-net")
@Slf4j
public class FtpFileIT extends TestSuiteBase implements TestResource {

    private static final String FTP_IMAGE = "fauria/vsftpd:latest";

    private static final String FTP_CONTAINER_HOST = "ftp";

    private static final int FTP_PORT = 21;

    private static final String USERNAME = "seatunnel";

    private static final String PASSWORD = "pass";

    private static final String CONTINUOUS_DISTCP_PATH = "/tmp/seatunnel/continuous/distcp";

    private static final String CONTINUOUS_DELETE_PATH = "/tmp/seatunnel/continuous/delete";

    private static final String CONTINUOUS_BACKUP_PATH = "/tmp/seatunnel/continuous/backup";

    private static final String CONTINUOUS_RETENTION_PATH = "/tmp/seatunnel/continuous/retention";

    private static final String CONTINUOUS_NON_RECURSIVE_PATH =
            "/tmp/seatunnel/continuous/non-recursive";

    private GenericContainer<?> ftpContainer;

    private String ftpHomeDir;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        int passiveStartPort = 30000;
        int passiveEndPort = 30004;
        ftpContainer =
                new GenericContainer<>(FTP_IMAGE)
                        .withNetwork(NETWORK)
                        .withExposedPorts(FTP_PORT)
                        .withNetworkAliases(FTP_CONTAINER_HOST)
                        .withEnv("FILE_OPEN_MODE", "0666")
                        .withEnv("WRITE_ENABLE", "YES")
                        .withEnv("ALLOW_WRITEABLE_CHROOT", "YES")
                        .withEnv("ANONYMOUS_ENABLE", "YES")
                        .withEnv("LOCAL_ENABLE", "YES")
                        .withEnv("LOCAL_UMASK", "000")
                        .withEnv("FTP_USER", USERNAME)
                        .withEnv("FTP_PASS", PASSWORD)
                        .withEnv("PASV_ADDRESS", FTP_CONTAINER_HOST)
                        .withEnv("PASV_ADDR_RESOLVE", "YES")
                        .withEnv("PASV_MIN_PORT", String.valueOf(passiveStartPort))
                        .withEnv("PASV_MAX_PORT", String.valueOf(passiveEndPort))
                        .withLogConsumer(new Slf4jLogConsumer(log))
                        // Modify the strategy mode because the passive mode port does not need to
                        // be checked here, it does not start with the FTP startup.
                        .waitingFor(Wait.forLogMessage(".*", 1))
                        .withPrivilegedMode(true);

        ftpContainer.start();
        Startables.deepStart(Stream.of(ftpContainer)).join();

        log.info("ftp container started");

        ftpHomeDir = getFtpUserHomeDir();

        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                ftpHomeDir + "/tmp/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                ftpHomeDir + "/tmp/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e-txt.zip",
                ftpHomeDir + "/tmp/seatunnel/read/zip/txt/single/e2e-txt.zip",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/excel/e2e.xlsx",
                ftpHomeDir + "/tmp/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/excel/e2e.xlsx",
                ftpHomeDir
                        + "/tmp/seatunnel/read/excel_filter/name=tyrantlucifer/hobby=coding/e2e_filter.xlsx",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/excel/e2e.xlsx", ftpHomeDir + "/e2e.xlsx", ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                ftpHomeDir + "/tmp/seatunnel/read/recursive/e2e.txt",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                ftpHomeDir + "/tmp/seatunnel/read/recursive/subdir/e2e.txt",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                ftpHomeDir + "/tmp/seatunnel/read/recursive/subdir/deeper/e2e.txt",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                ftpHomeDir + "/tmp/seatunnel/read/recursive/subdir/deeper/final/e2e.txt",
                ftpContainer);

        ftpContainer.execInContainer("sh", "-c", "chmod -R 777 " + ftpHomeDir + "/");
        ftpContainer.execInContainer("sh", "-c", "chown -R ftp:ftp " + ftpHomeDir + "/");
    }

    @TestTemplate
    public void testFtpFileReadAndWriteForPassive(TestContainer container)
            throws IOException, InterruptedException {
        List<String> configParams = Collections.singletonList("ftpHost=" + FTP_CONTAINER_HOST);
        // Test passive mode
        assertJobExecution(
                container, "/text/ftp_file_text_to_assert_for_passive.conf", configParams);
        assertJobExecution(container, "/text/fake_to_ftp_file_text_for_passive.conf", configParams);

        String homePath = ftpHomeDir + "/tmp/seatunnel/passive_text";
        // test write ftp text file
        Assertions.assertEquals(1, getFileListFromContainer(homePath).size());

        // Confirm data is written correctly
        Container.ExecResult execResult =
                ftpContainer.execInContainer("sh", "-c", "awk 'END {print NR}' " + homePath + "/*");
        Assertions.assertEquals("15", execResult.getStdout().trim());

        deleteFileFromContainer(homePath);
    }

    @TestTemplate
    public void testFtpToFtpForBinary(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult execResult = container.executeJob("/text/ftp_to_ftp_for_binary.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String homePath = ftpHomeDir + "/uploads/seatunnel";
        Assertions.assertEquals(1, getFileListFromContainer(homePath).size());

        // Confirm data is written correctly
        Container.ExecResult resultExecResult =
                ftpContainer.execInContainer(
                        "sh", "-c", "awk 'END {print NR}' " + homePath + "/e2e.txt");
        Assertions.assertEquals("5", resultExecResult.getStdout().trim());

        deleteFileFromContainer(homePath);
    }

    @TestTemplate
    public void testFtpBinaryUpdateModeDistcp(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        putFtpFile("/tmp/seatunnel/update/src/test.bin", "abc");

        Container.ExecResult firstRun = container.executeJob("/text/ftp_binary_update_distcp.conf");
        Assertions.assertEquals(0, firstRun.getExitCode(), firstRun.getStderr());
        Assertions.assertEquals("abc", readFtpFile("/tmp/seatunnel/update/dst/test.bin"));

        // Make target newer with same length, distcp strategy should SKIP overwrite.
        putFtpFile("/tmp/seatunnel/update/dst/test.bin", "zzz");
        Container.ExecResult secondRun =
                container.executeJob("/text/ftp_binary_update_distcp.conf");
        Assertions.assertEquals(0, secondRun.getExitCode(), secondRun.getStderr());
        Assertions.assertEquals("zzz", readFtpFile("/tmp/seatunnel/update/dst/test.bin"));

        // Change source length, distcp strategy should COPY overwrite.
        putFtpFile("/tmp/seatunnel/update/src/test.bin", "abcd");
        Container.ExecResult thirdRun = container.executeJob("/text/ftp_binary_update_distcp.conf");
        Assertions.assertEquals(0, thirdRun.getExitCode(), thirdRun.getStderr());
        Assertions.assertEquals("abcd", readFtpFile("/tmp/seatunnel/update/dst/test.bin"));

        deleteFileFromContainer(ftpHomeDir + "/tmp/seatunnel/update");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testFtpBinaryUpdateModeContinuousDiscoveryDistcp(TestContainer container)
            throws Throwable {
        resetContinuousTestPath(CONTINUOUS_DISTCP_PATH);
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture = null;
        Throwable testFailure = null;
        try {
            putFtpFile(CONTINUOUS_DISTCP_PATH + "/src/test1.bin", "abc");

            jobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/text/ftp_binary_update_distcp_continuous.conf",
                                            jobId,
                                            "ftpHost=" + FTP_CONTAINER_HOST);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "abc",
                                            readFtpFile(
                                                    CONTINUOUS_DISTCP_PATH + "/dst/test1.bin")));

            long firstMtimeSeconds =
                    getFtpFileMtimeSeconds(CONTINUOUS_DISTCP_PATH + "/dst/test1.bin");
            Thread.sleep(2500);
            long secondMtimeSeconds =
                    getFtpFileMtimeSeconds(CONTINUOUS_DISTCP_PATH + "/dst/test1.bin");
            Assertions.assertEquals(
                    firstMtimeSeconds,
                    secondMtimeSeconds,
                    "Continuous discovery should skip unchanged files in update mode.");

            putFtpFile(CONTINUOUS_DISTCP_PATH + "/src/test2.bin", "def");
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "def",
                                            readFtpFile(
                                                    CONTINUOUS_DISTCP_PATH + "/dst/test2.bin")));
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            cleanupContinuousJob(
                    container, jobId, jobFuture, ftpHomeDir + CONTINUOUS_DISTCP_PATH, testFailure);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testFtpBinaryUpdateModeContinuousDiscoveryPostSyncDelete(TestContainer container)
            throws Throwable {
        resetContinuousTestPath(CONTINUOUS_DELETE_PATH);
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture = null;
        Throwable testFailure = null;
        try {
            putFtpFile(CONTINUOUS_DELETE_PATH + "/src/delete-test.bin", "abc");

            jobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/text/ftp_binary_update_distcp_continuous_post_sync_delete.conf",
                                            jobId,
                                            "ftpHost=" + FTP_CONTAINER_HOST);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "abc",
                                            readFtpFile(
                                                    CONTINUOUS_DELETE_PATH
                                                            + "/dst/delete-test.bin")));

            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertFalse(
                                            isFtpFileExists(
                                                    CONTINUOUS_DELETE_PATH
                                                            + "/src/delete-test.bin"),
                                            "source file should be deleted after checkpoint-gated post-sync commit"));
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            cleanupContinuousJob(
                    container, jobId, jobFuture, ftpHomeDir + CONTINUOUS_DELETE_PATH, testFailure);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testFtpBinaryUpdateModeContinuousDiscoveryPostSyncBackup(TestContainer container)
            throws Throwable {
        resetContinuousTestPath(CONTINUOUS_BACKUP_PATH);
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture = null;
        Throwable testFailure = null;
        try {
            putFtpFile(CONTINUOUS_BACKUP_PATH + "/src/backup-test.bin", "abc");

            jobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/text/ftp_binary_update_distcp_continuous_post_sync_backup.conf",
                                            jobId,
                                            "ftpHost=" + FTP_CONTAINER_HOST);
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
                                            readFtpFile(
                                                    CONTINUOUS_BACKUP_PATH
                                                            + "/dst/backup-test.bin")));

            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertFalse(
                                            isFtpFileExists(
                                                    CONTINUOUS_BACKUP_PATH
                                                            + "/src/backup-test.bin"),
                                            "source file should be moved from source path after backup commit"));

            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            countFtpFilesByNamePattern(
                                                            CONTINUOUS_BACKUP_PATH + "/backup",
                                                            "backup-test.bin.v*")
                                                    > 0,
                                            "backup target should contain version-suffixed file"));
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            cleanupContinuousJob(
                    container, jobId, jobFuture, ftpHomeDir + CONTINUOUS_BACKUP_PATH, testFailure);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testFtpContinuousBackupRetentionCleanup(TestContainer container) throws Throwable {
        resetContinuousTestPath(CONTINUOUS_RETENTION_PATH);
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture = null;
        Throwable testFailure = null;
        try {
            putFtpFile(CONTINUOUS_RETENTION_PATH + "/src/retention-input.bin", "input");
            putFtpFile(CONTINUOUS_RETENTION_PATH + "/backup/retention-old.bin.v3_123456", "abc");
            setFtpFileMtimeToPast(
                    CONTINUOUS_RETENTION_PATH + "/backup/retention-old.bin.v3_123456");

            jobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/text/ftp_binary_update_distcp_continuous_post_sync_backup_retention.conf",
                                            jobId,
                                            "ftpHost=" + FTP_CONTAINER_HOST);
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
                                            0L,
                                            countFtpFilesByNamePattern(
                                                    CONTINUOUS_RETENTION_PATH + "/backup",
                                                    "retention-old.bin.v*"),
                                            "retention should remove expired SeaTunnel backup files"));
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            cleanupContinuousJob(
                    container,
                    jobId,
                    jobFuture,
                    ftpHomeDir + CONTINUOUS_RETENTION_PATH,
                    testFailure);
        }
    }

    /**
     * Verifies continuous non-recursive FTP sync copies root files while ignoring nested files.
     *
     * <p>The nested-file assertion protects the connector's non-recursive discovery contract.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testFtpBinaryUpdateModeContinuousDiscoveryWithNonRecursiveScan(
            TestContainer container) throws Throwable {
        resetContinuousTestPath(CONTINUOUS_NON_RECURSIVE_PATH);
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture = null;
        Throwable testFailure = null;
        try {
            putFtpFile(CONTINUOUS_NON_RECURSIVE_PATH + "/src/root.bin", "root");
            putFtpFile(CONTINUOUS_NON_RECURSIVE_PATH + "/src/subdir/nested.bin", "nested");

            jobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/text/ftp_binary_update_distcp_continuous_non_recursive.conf",
                                            jobId,
                                            "ftpHost=" + FTP_CONTAINER_HOST);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "root",
                                            readFtpFile(
                                                    CONTINUOUS_NON_RECURSIVE_PATH
                                                            + "/dst/root.bin")));

            Thread.sleep(3000);
            Assertions.assertFalse(
                    isFtpFileExists(CONTINUOUS_NON_RECURSIVE_PATH + "/dst/subdir/nested.bin"));
        } catch (Throwable failure) {
            testFailure = failure;
            throw failure;
        } finally {
            cleanupContinuousJob(
                    container,
                    jobId,
                    jobFuture,
                    ftpHomeDir + CONTINUOUS_NON_RECURSIVE_PATH,
                    testFailure);
        }
    }

    /**
     * Verifies non-recursive FTP distcp updates root files without overwriting nested files.
     *
     * <p>The stale nested destination file must remain unchanged after the job finishes.
     */
    @TestTemplate
    public void testFtpBinaryUpdateModeDistcpWithNonRecursiveScan(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        try {
            putFtpFile("/tmp/seatunnel/update/src/root.bin", "root-updated-v2");
            putFtpFile("/tmp/seatunnel/update/src/subdir/nested.bin", "nest-updated-v2");
            putFtpFile("/tmp/seatunnel/update/dst/root.bin", "root-stale-v1");
            putFtpFile("/tmp/seatunnel/update/dst/subdir/nested.bin", "nest-stale-v1");

            Container.ExecResult execResult =
                    container.executeJob("/text/ftp_binary_update_non_recursive_distcp.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            Assertions.assertEquals(
                    "root-updated-v2", readFtpFile("/tmp/seatunnel/update/dst/root.bin"));
            Assertions.assertEquals(
                    "nest-stale-v1", readFtpFile("/tmp/seatunnel/update/dst/subdir/nested.bin"));
        } finally {
            deleteFileFromContainer(ftpHomeDir + "/tmp/seatunnel/update");
        }
    }

    /**
     * Verifies strict checksum mode keeps nested FTP files untouched during non-recursive scans.
     */
    @TestTemplate
    public void testFtpBinaryUpdateModeStrictChecksumSkipsNestedChangesWithNonRecursiveScan(
            TestContainer container) throws IOException, InterruptedException {
        resetUpdateTestPath();
        try {
            putFtpFile("/tmp/seatunnel/update/src/root.bin", "root-same-v1");
            putFtpFile("/tmp/seatunnel/update/src/subdir/nested.bin", "nest-new-v1");
            putFtpFile("/tmp/seatunnel/update/dst/root.bin", "root-same-v1");
            putFtpFile("/tmp/seatunnel/update/dst/subdir/nested.bin", "nest-old-v1");

            Container.ExecResult execResult =
                    container.executeJob(
                            "/text/ftp_binary_update_non_recursive_strict_checksum.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            Assertions.assertEquals(
                    "root-same-v1", readFtpFile("/tmp/seatunnel/update/dst/root.bin"));
            Assertions.assertEquals(
                    "nest-old-v1", readFtpFile("/tmp/seatunnel/update/dst/subdir/nested.bin"));
        } finally {
            deleteFileFromContainer(ftpHomeDir + "/tmp/seatunnel/update");
        }
    }

    /**
     * Continuous discovery is only considered stopped after the engine reaches CANCELED and the
     * submit command exits, so cleanup regressions still fail this E2E test.
     */
    private void assertContinuousJobStopsAfterCancel(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture)
            throws IOException, InterruptedException {
        if (jobFuture == null) {
            return;
        }
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Container.ExecResult cancelResult = container.cancelJob(jobId);
                            Assertions.assertEquals(
                                    0, cancelResult.getExitCode(), cancelResult.getStderr());
                        });

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "CANCELED",
                                        container.getJobStatus(jobId),
                                        "Continuous job should be canceled before the test exits."));

        Awaitility.await()
                .atMost(180, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(jobFuture::isDone);

        try {
            Container.ExecResult execResult = jobFuture.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        } catch (Exception e) {
            throw new RuntimeException("Wait continuous job exit failed.", e);
        }
    }

    /**
     * Stops a continuous job before deleting its shared test path, without hiding the original test
     * failure when cleanup also fails.
     */
    private void cleanupContinuousJob(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture,
            String cleanupPath,
            Throwable testFailure)
            throws Throwable {
        Throwable cleanupFailure = null;
        try {
            assertContinuousJobStopsAfterCancel(container, jobId, jobFuture);
        } catch (Throwable failure) {
            cleanupFailure = failure;
        }

        if (cleanupFailure == null) {
            try {
                deleteFileFromContainer(cleanupPath);
            } catch (Throwable failure) {
                cleanupFailure = failure;
            }
        }

        if (cleanupFailure == null) {
            return;
        }
        if (testFailure != null) {
            testFailure.addSuppressed(cleanupFailure);
        } else {
            throw cleanupFailure;
        }
    }

    @TestTemplate
    public void testFtpToAssertForJsonFilter(TestContainer container)
            throws IOException, InterruptedException {

        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                ftpHomeDir
                        + "/tmp/seatunnel/read/filter/json/name=tyrantlucifer/hobby=coding/e2e.json",
                ftpContainer);
        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                ftpHomeDir
                        + "/tmp/seatunnel/read/filter/json2025/name=tyrantlucifer/hobby=coding/e2e_2025.json",
                ftpContainer);
        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                ftpHomeDir
                        + "/tmp/seatunnel/read/filter/json2025/name=tyrantlucifer/hobby=coding/e2e_2025.txt",
                ftpContainer);
        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                ftpHomeDir
                        + "/tmp/seatunnel/read/filter/json2024/name=tyrantlucifer/hobby=coding/e2e_2024.json",
                ftpContainer);

        ContainerUtil.copyFileIntoContainers(
                "/text/e2e.txt",
                ftpHomeDir
                        + "/tmp/seatunnel/read/filter/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                ftpContainer);

        ftpContainer.execInContainer("sh", "-c", "chmod -R 777 " + ftpHomeDir + "/");
        ftpContainer.execInContainer("sh", "-c", "chown -R ftp:ftp " + ftpHomeDir + "/");

        TestHelper helper = new TestHelper(container);
        // -----filter based on the file directory at the same time, the expression needs to start
        // with `path`--------
        helper.execute("/json/ftp_to_access_for_json_path_filter.conf");

        // -------filter based on file names, just simply write the regular file names--------
        helper.execute("/json/ftp_to_access_for_json_name_filter.conf");

        // delete path
        String filterPath = ftpHomeDir + "/tmp/seatunnel/read/filter";
        deleteFileFromContainer(filterPath);
    }

    private void assertJobExecution(TestContainer container, String configPath, List<String> params)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob(configPath, params);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
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
        helper.execute("/text/fake_to_ftp_file_text_no_verify.conf");
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
        ensureReadJsonInputFile();
        helper.execute("/json/ftp_file_json_to_assert.conf");
        // test write ftp parquet file
        helper.execute("/parquet/fake_to_ftp_file_parquet.conf");
        // test write ftp orc file
        helper.execute("/orc/fake_to_ftp_file_orc.conf");
        // test write ftp root path excel file
        helper.execute("/excel/fake_source_to_ftp_root_path_excel.conf");
        // test ftp source support multipleTable

        // test read recursive file path
        helper.execute("/text/ftp_file_text_recursive_to_assert.conf");
        helper.execute("/text/ftp_file_text_non_recursive_to_assert.conf");

        String homePath = ftpHomeDir;
        String sink01 = "/tmp/seatunnel/json/sink/multiplesource/fake01";
        String sink02 = "/tmp/seatunnel/json/sink/multiplesource/fake02";
        deleteFileFromContainer(homePath + sink01);
        deleteFileFromContainer(homePath + sink02);
        // Keep a dedicated source file for each logical table. Sharing one FTP path between the
        // two multiple-table entries can leave the second reader with no physical file to open on
        // slower CI runs.
        ensureMultipleTableJsonInputFiles();
        helper.execute("/json/ftp_file_json_to_assert_with_multipletable.conf");
        Assertions.assertEquals(getFileListFromContainer(homePath + sink01).size(), 1);
        Assertions.assertEquals(getFileListFromContainer(homePath + sink02).size(), 1);
    }

    @TestTemplate
    public void testFtpFileWithSpecialCharactersPath(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);

        // Create test file with spaces in path - simpler test to avoid Docker memory issues
        String specialPath = "/tmp/seatunnel/test spaces";
        String fileName = "file with spaces.txt";
        String fullPath = specialPath + "/" + fileName;
        String homePath = ftpHomeDir;
        String containerPath = homePath + fullPath;

        try {
            // Create directory structure with special characters
            Container.ExecResult mkdirResult =
                    ftpContainer.execInContainer("mkdir", "-p", homePath + specialPath);
            log.info(
                    "mkdir result: exit code {}, stdout: {}, stderr: {}",
                    mkdirResult.getExitCode(),
                    mkdirResult.getStdout(),
                    mkdirResult.getStderr());

            // Create test file with content
            String testContent = "name,age,city\nJohn,30,NYC\nJane,25,LA\n";
            Container.ExecResult createResult =
                    ftpContainer.execInContainer(
                            "sh", "-c", "echo '" + testContent + "' > '" + containerPath + "'");
            log.info(
                    "create file result: exit code {}, stdout: {}, stderr: {}",
                    createResult.getExitCode(),
                    createResult.getStdout(),
                    createResult.getStderr());

            // Verify file was created
            Container.ExecResult lsResult =
                    ftpContainer.execInContainer("ls", "-la", containerPath);
            Assertions.assertEquals(
                    0,
                    lsResult.getExitCode(),
                    "Failed to create test file with special characters: " + lsResult.getStderr());
            log.info("File created successfully: {}", lsResult.getStdout());

            // Test reading file with special characters in path using UTF-8 control encoding
            helper.execute("/text/ftp_special_characters_path_to_assert.conf");

        } finally {
            // Clean up
            deleteFileFromContainer(homePath + "/tmp/seatunnel/test\\ spaces");
        }
    }

    @TestTemplate
    public void testMultipleTableAndSaveMode(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        // test mult table and save_mode:RECREATE_SCHEMA DROP_DATA
        String homePath = ftpHomeDir;
        String path1 = "/tmp/seatunnel_mult/text/source_1";
        String path2 = "/tmp/seatunnel_mult/text/source_2";
        deleteFileFromContainer(homePath + path1);
        deleteFileFromContainer(homePath + path2);
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
        deleteFileFromContainer(homePath + path3);
        deleteFileFromContainer(homePath + path4);
        Assertions.assertEquals(getFileListFromContainer(homePath + path3).size(), 0);
        Assertions.assertEquals(getFileListFromContainer(homePath + path4).size(), 0);
        helper.execute("/text/multiple_table_fake_to_ftp_file_text_2.conf");
        Assertions.assertEquals(getFileListFromContainer(homePath + path3).size(), 1);
        Assertions.assertEquals(getFileListFromContainer(homePath + path4).size(), 1);
        helper.execute("/text/multiple_table_fake_to_ftp_file_text_2.conf");
        Assertions.assertEquals(getFileListFromContainer(homePath + path3).size(), 2);
        Assertions.assertEquals(getFileListFromContainer(homePath + path4).size(), 2);
    }

    private void resetUpdateTestPath() throws IOException, InterruptedException {
        deleteFileFromContainer(ftpHomeDir + "/tmp/seatunnel/update");
        Container.ExecResult mkdirResult =
                ftpContainer.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + ftpHomeDir
                                + "/tmp/seatunnel/update/src "
                                + ftpHomeDir
                                + "/tmp/seatunnel/update/dst "
                                + ftpHomeDir
                                + "/tmp/seatunnel/update/tmp");
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());
        ftpContainer.execInContainer(
                "sh", "-c", "chmod -R 777 " + ftpHomeDir + "/tmp/seatunnel/update || true");
        ftpContainer.execInContainer(
                "sh", "-c", "chown -R ftp:ftp " + ftpHomeDir + "/tmp/seatunnel/update || true");
    }

    private void resetContinuousTestPath(String continuousPath)
            throws IOException, InterruptedException {
        deleteFileFromContainer(ftpHomeDir + continuousPath);
        Container.ExecResult mkdirResult =
                ftpContainer.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + ftpHomeDir
                                + continuousPath
                                + "/src "
                                + ftpHomeDir
                                + continuousPath
                                + "/dst "
                                + ftpHomeDir
                                + continuousPath
                                + "/tmp");
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());
        ftpContainer.execInContainer(
                "sh", "-c", "chmod -R 777 " + ftpHomeDir + continuousPath + " || true");
        ftpContainer.execInContainer(
                "sh", "-c", "chown -R ftp:ftp " + ftpHomeDir + continuousPath + " || true");
    }

    private void putFtpFile(String ftpPath, String content)
            throws IOException, InterruptedException {
        String containerPath = ftpHomeDir + ftpPath;
        String command =
                "parent=$(dirname '"
                        + containerPath
                        + "') && mkdir -p \"$parent\" && printf '"
                        + content
                        + "' > '"
                        + containerPath
                        + "' && chmod -R 777 \"$parent\" && chmod 666 '"
                        + containerPath
                        + "'";
        Container.ExecResult putResult = ftpContainer.execInContainer("sh", "-c", command);
        Assertions.assertEquals(0, putResult.getExitCode(), putResult.getStderr());
    }

    private String readFtpFile(String ftpPath) throws IOException, InterruptedException {
        String containerPath = ftpHomeDir + ftpPath;
        Container.ExecResult catResult =
                ftpContainer.execInContainer("sh", "-c", "cat '" + containerPath + "'");
        Assertions.assertEquals(0, catResult.getExitCode(), catResult.getStderr());
        return catResult.getStdout() == null ? "" : catResult.getStdout().trim();
    }

    /**
     * Checks whether a file exists in the FTP container without creating parent directories.
     *
     * <p>This helper is used by negative assertions where creating the path would hide regressions.
     */
    private boolean isFtpFileExists(String ftpPath) throws IOException, InterruptedException {
        String containerPath = ftpHomeDir + ftpPath;
        Container.ExecResult result =
                ftpContainer.execInContainer("sh", "-c", "test -f '" + containerPath + "'");
        return result.getExitCode() == 0;
    }

    private long getFtpFileMtimeSeconds(String ftpPath) throws IOException, InterruptedException {
        String containerPath = ftpHomeDir + ftpPath;
        Container.ExecResult result =
                ftpContainer.execInContainer("sh", "-c", "stat -c %Y '" + containerPath + "'");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        return Long.parseLong(result.getStdout().trim());
    }

    private long countFtpFilesByNamePattern(String ftpPath, String namePattern)
            throws IOException, InterruptedException {
        String containerPath = ftpHomeDir + ftpPath;
        Container.ExecResult result =
                ftpContainer.execInContainer(
                        "sh",
                        "-c",
                        "if [ -d '"
                                + containerPath
                                + "' ]; then find '"
                                + containerPath
                                + "' -type f -name '"
                                + namePattern
                                + "' | wc -l; else echo 0; fi");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        return Long.parseLong(result.getStdout().trim());
    }

    private void setFtpFileMtimeToPast(String ftpPath) throws IOException, InterruptedException {
        Container.ExecResult result =
                ftpContainer.execInContainer(
                        "sh", "-c", "touch -t 202001010000.00 '" + ftpHomeDir + ftpPath + "'");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    /**
     * Best-effort cleanup for assertion failures so a continuous test cannot leak a running job.
     */
    private void cancelContinuousJobQuietly(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture) {
        if (jobFuture == null || jobFuture.isDone()) {
            return;
        }
        try {
            String status = container.getJobStatus(jobId);
            if (!"CANCELED".equals(status)
                    && !"FINISHED".equals(status)
                    && !"FAILED".equals(status)) {
                container.cancelJob(jobId);
            }
            Awaitility.await()
                    .atMost(180, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .until(jobFuture::isDone);
        } catch (Exception e) {
            log.warn("Failed to clean up continuous FTP job {}.", jobId, e);
        }
    }

    private void waitContinuousJobExit(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertEquals("CANCELED", container.getJobStatus(jobId)));
        Awaitility.await()
                .atMost(180, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(jobFuture::isDone);
        try {
            Container.ExecResult execResult = jobFuture.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        } catch (Exception e) {
            throw new RuntimeException("Wait continuous job exit failed.", e);
        }
    }

    private String getFtpUserHomeDir() throws IOException, InterruptedException {
        // Prefer vsftpd local_root as the real filesystem root used by FTP paths in test configs.
        // In some images, FTP users are created as virtual users and may not exist in /etc/passwd.
        try {
            Container.ExecResult confResult =
                    ftpContainer.execInContainer("sh", "-c", "cat /etc/vsftpd/vsftpd.conf");
            if (confResult.getExitCode() == 0 && StringUtils.isNotBlank(confResult.getStdout())) {
                Properties properties = new Properties();
                properties.load(new StringReader(confResult.getStdout()));
                String localRoot = properties.getProperty("local_root");
                if (StringUtils.isNotBlank(localRoot)) {
                    String resolved =
                            localRoot
                                    .trim()
                                    .replace("${FTP_USER}", USERNAME)
                                    .replace("$FTP_USER", USERNAME)
                                    .replace("${USER}", USERNAME)
                                    .replace("$USER", USERNAME);
                    if (StringUtils.isNotBlank(resolved) && containerDirExists(resolved)) {
                        return resolved;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to resolve ftp local_root from vsftpd.conf, fallback to default.", e);
        }

        // Fallback: resolve from /etc/passwd if user exists
        Container.ExecResult homeResult =
                ftpContainer.execInContainer(
                        "sh",
                        "-c",
                        "awk -F: '$1==\""
                                + USERNAME
                                + "\"{print $6}' /etc/passwd 2>/dev/null || true");
        if (homeResult.getExitCode() == 0) {
            String homeDir = homeResult.getStdout() == null ? "" : homeResult.getStdout().trim();
            if (StringUtils.isNotBlank(homeDir) && containerDirExists(homeDir)) {
                return homeDir;
            }
        }

        // Last resort: use default directory used by fauria/vsftpd.
        String defaultUserRoot = "/home/vsftpd/" + USERNAME;
        if (containerDirExists(defaultUserRoot)) {
            log.warn(
                    "Cannot resolve ftp home directory for user: {}, fallback to {}",
                    USERNAME,
                    defaultUserRoot);
            return defaultUserRoot;
        }

        String defaultRoot = "/home/vsftpd";
        if (containerDirExists(defaultRoot)) {
            log.warn(
                    "Cannot resolve ftp home directory for user: {}, fallback to {}",
                    USERNAME,
                    defaultRoot);
            return defaultRoot;
        }

        log.warn(
                "Cannot resolve ftp home directory for user: {}, fallback to {}",
                USERNAME,
                defaultUserRoot);
        return defaultUserRoot;
    }

    private boolean containerDirExists(String path) throws IOException, InterruptedException {
        Container.ExecResult result =
                ftpContainer.execInContainer(
                        "sh", "-c", "test -d '" + path + "' && echo true || echo false");
        return result.getExitCode() == 0
                && StringUtils.equalsIgnoreCase(
                        (result.getStdout() == null ? "" : result.getStdout().trim()), "true");
    }

    private void ensureReadJsonInputFile() throws IOException, InterruptedException {
        // Reset the shared JSON source directory for each template run. The FTP container is
        // reused across engine variants, so stale fake01/fake02 inputs from the multiple-table
        // case can otherwise leak into the single-table assert and inflate the row count.
        deleteFileFromContainer(ftpHomeDir + "/tmp/seatunnel/read/json");
        Container.ExecResult mkdirResult =
                ftpContainer.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + ftpHomeDir
                                + "/tmp/seatunnel/read/json/name=tyrantlucifer/hobby=coding");
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());
        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json",
                ftpHomeDir + "/tmp/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                ftpContainer);
        Container.ExecResult chmodResult =
                ftpContainer.execInContainer(
                        "sh", "-c", "chmod -R 777 " + ftpHomeDir + "/tmp/seatunnel/read");
        Assertions.assertEquals(0, chmodResult.getExitCode(), chmodResult.getStderr());
    }

    /**
     * The multiple-table FTP JSON test reads two logical tables from two table configs. Give each
     * table its own physical source file so one reader never depends on a file already consumed by
     * the other table path. This helper owns {@code tmp/seatunnel/read/json} for the suite and
     * rebuilds it before the multiple-table run.
     */
    private void ensureMultipleTableJsonInputFiles() throws IOException, InterruptedException {
        // Rebuild the multiple-table source tree from scratch so previous template runs never
        // leave extra JSON files behind for the next engine invocation.
        deleteFileFromContainer(ftpHomeDir + "/tmp/seatunnel/read/json");
        copyJsonInputFileTo(
                ftpHomeDir + "/tmp/seatunnel/read/json/fake01/name=tyrantlucifer/hobby=coding");
        copyJsonInputFileTo(
                ftpHomeDir + "/tmp/seatunnel/read/json/fake02/name=tyrantlucifer/hobby=coding");
        Container.ExecResult chmodResult =
                ftpContainer.execInContainer(
                        "sh", "-c", "chmod -R 777 " + ftpHomeDir + "/tmp/seatunnel/read/json");
        Assertions.assertEquals(0, chmodResult.getExitCode(), chmodResult.getStderr());
    }

    /**
     * Copies the canonical JSON fixture into an isolated multiple-table input directory.
     *
     * @param directory destination directory inside the FTP container
     */
    private void copyJsonInputFileTo(String directory) throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                ftpContainer.execInContainer("sh", "-c", "mkdir -p " + directory);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());
        ContainerUtil.copyFileIntoContainers(
                "/json/e2e.json", directory + "/e2e.json", ftpContainer);
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

    @SneakyThrows
    private void deleteFileFromContainer(String path) {
        String command = "rm -rf " + path;
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
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (ftpContainer != null) {
            ftpContainer.close();
        }
    }
}
