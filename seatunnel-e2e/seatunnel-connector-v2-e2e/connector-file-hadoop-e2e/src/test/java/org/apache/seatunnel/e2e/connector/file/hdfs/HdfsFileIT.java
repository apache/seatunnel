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

package org.apache.seatunnel.e2e.connector.file.hdfs;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class HdfsFileIT extends TestSuiteBase implements TestResource {

    private static final String HADOOP_IMAGE = "apache/hadoop:3";

    private GenericContainer<?> nameNode;
    private GenericContainer<?> dataNode;

    @TestContainerExtension
    private final org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory
            extendedFactory = container -> {};

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        nameNode =
                new GenericContainer<>(DockerImageName.parse(HADOOP_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("namenode1")
                        .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-root/dfs/name")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("viewfs/cluster1/core-site.xml"),
                                "/opt/hadoop/etc/hadoop/core-site.xml")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("viewfs/cluster1/hdfs-site.xml"),
                                "/opt/hadoop/etc/hadoop/hdfs-site.xml")
                        .withCommand("sh", "-c", "hdfs namenode -format -force && hdfs namenode")
                        .withExposedPorts(9870, 9000)
                        .waitingFor(
                                Wait.forHttp("/")
                                        .forPort(9870)
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(HADOOP_IMAGE + ":namenode")));

        dataNode =
                new GenericContainer<>(DockerImageName.parse(HADOOP_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("datanode1")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("viewfs/cluster1/core-site.xml"),
                                "/opt/hadoop/etc/hadoop/core-site.xml")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("viewfs/cluster1/hdfs-site.xml"),
                                "/opt/hadoop/etc/hadoop/hdfs-site.xml")
                        .withCommand("hdfs", "datanode")
                        .dependsOn(nameNode)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(HADOOP_IMAGE + ":datanode")));

        Startables.deepStart(Stream.of(nameNode, dataNode)).join();
        Thread.sleep(5000);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (dataNode != null) {
            dataNode.stop();
            log.info("HDFS DataNode stopped");
        }
        if (nameNode != null) {
            nameNode.stop();
            log.info("HDFS NameNode stopped");
        }
    }

    @TestTemplate
    public void testHdfsWrite(TestContainer container) throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult execResult =
                container.executeJob("/fake_to_hdfs_normal.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        org.testcontainers.containers.Container.ExecResult lsResult =
                nameNode.execInContainer("hdfs", "dfs", "-ls", "/normal/output");
        Assertions.assertEquals(0, lsResult.getExitCode(), "Directory /normal/output should exist");
    }

    @TestTemplate
    public void testHdfsRead(TestContainer container) throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult writeResult =
                container.executeJob("/fake_to_hdfs_normal.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());
        org.testcontainers.containers.Container.ExecResult readResult =
                container.executeJob("/hdfs_normal_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testHdfsParquetReadWithFileSplit(TestContainer container)
            throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult writeResult =
                container.executeJob("/fake_to_hdfs_normal.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());
        org.testcontainers.containers.Container.ExecResult readResult =
                container.executeJob("/hdfs_parquet_split_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testHdfsTextReadWithFileSplit(TestContainer container)
            throws IOException, InterruptedException {
        resetSplitTestPath();
        putHdfsSequentialLinesFile("/split/input/test.txt", 1000);

        org.testcontainers.containers.Container.ExecResult readResult =
                container.executeJob("/hdfs_text_split_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testHdfsReadEmptyTextDirectory(TestContainer container)
            throws IOException, InterruptedException {
        nameNode.execInContainer("bash", "-c", "hdfs dfs -rm -r -f /empty/text || true");
        org.testcontainers.containers.Container.ExecResult mkdirResult =
                nameNode.execInContainer("hdfs", "dfs", "-mkdir", "-p", "/empty/text");
        Assertions.assertEquals(0, mkdirResult.getExitCode());

        org.testcontainers.containers.Container.ExecResult readResult =
                container.executeJob("/hdfs_empty_text_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testHdfsReadRecursiveTextDirectory(TestContainer container)
            throws IOException, InterruptedException {
        resetRecursiveTestPath();
        putHdfsClasspathFile("/text/e2e.txt", "/recursive/e2e.txt");
        putHdfsClasspathFile("/text/e2e.txt", "/recursive/subdir/e2e.txt");
        putHdfsClasspathFile("/text/e2e.txt", "/recursive/subdir/deeper/e2e.txt");
        putHdfsClasspathFile("/text/e2e.txt", "/recursive/subdir/deeper/final/e2e.txt");

        org.testcontainers.containers.Container.ExecResult recursiveResult =
                container.executeJob("/hdfs_text_recursive_to_assert.conf");
        Assertions.assertEquals(0, recursiveResult.getExitCode());

        org.testcontainers.containers.Container.ExecResult nonRecursiveResult =
                container.executeJob("/hdfs_text_non_recursive_to_assert.conf");
        Assertions.assertEquals(0, nonRecursiveResult.getExitCode());
    }

    @TestTemplate
    public void testHdfsBinaryUpdateModeDistcp(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        putHdfsFile("/update/src/test.bin", "abc");

        org.testcontainers.containers.Container.ExecResult firstRun =
                container.executeJob("/hdfs_binary_update_distcp.conf");
        Assertions.assertEquals(0, firstRun.getExitCode());
        Assertions.assertEquals("abc", readHdfsFile("/update/dst/test.bin"));

        // Make target newer with same length, distcp strategy should SKIP overwrite.
        putHdfsFile("/update/dst/test.bin", "zzz");
        org.testcontainers.containers.Container.ExecResult secondRun =
                container.executeJob("/hdfs_binary_update_distcp.conf");
        Assertions.assertEquals(0, secondRun.getExitCode());
        Assertions.assertEquals("zzz", readHdfsFile("/update/dst/test.bin"));

        // Change source length, distcp strategy should COPY overwrite.
        putHdfsFile("/update/src/test.bin", "abcd");
        org.testcontainers.containers.Container.ExecResult thirdRun =
                container.executeJob("/hdfs_binary_update_distcp.conf");
        Assertions.assertEquals(0, thirdRun.getExitCode());
        Assertions.assertEquals("abcd", readHdfsFile("/update/dst/test.bin"));
    }

    @TestTemplate
    public void testHdfsBinaryUpdateModeStrictChecksum(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        putHdfsFile("/update/src/test.bin", "abc");

        org.testcontainers.containers.Container.ExecResult firstRun =
                container.executeJob("/hdfs_binary_update_strict_checksum.conf");
        Assertions.assertEquals(0, firstRun.getExitCode());
        Assertions.assertEquals("abc", readHdfsFile("/update/dst/test.bin"));

        // Same length but different content, strict+checksum should COPY overwrite.
        putHdfsFile("/update/dst/test.bin", "zzz");
        org.testcontainers.containers.Container.ExecResult secondRun =
                container.executeJob("/hdfs_binary_update_strict_checksum.conf");
        Assertions.assertEquals(0, secondRun.getExitCode());
        Assertions.assertEquals("abc", readHdfsFile("/update/dst/test.bin"));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testHdfsBinaryUpdateModeContinuousDiscoveryDistcp(TestContainer container)
            throws IOException, InterruptedException {
        resetContinuousTestPath();
        putHdfsFile("/continuous/src/test1.bin", "abc");

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<org.testcontainers.containers.Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/hdfs_binary_update_distcp_continuous.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "abc", readHdfsFile("/continuous/dst/test1.bin")));

        long firstMtimeSeconds = getHdfsFileMtimeSeconds("/continuous/dst/test1.bin");
        Thread.sleep(2500);
        long secondMtimeSeconds = getHdfsFileMtimeSeconds("/continuous/dst/test1.bin");
        Assertions.assertEquals(
                firstMtimeSeconds,
                secondMtimeSeconds,
                "Continuous discovery should skip unchanged files in update mode.");

        putHdfsFile("/continuous/src/test2.bin", "def");
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "def", readHdfsFile("/continuous/dst/test2.bin")));

        org.testcontainers.containers.Container.ExecResult cancelResult =
                container.cancelJob(jobId);
        Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

        org.testcontainers.containers.Container.ExecResult execResult;
        try {
            execResult = jobFuture.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Wait continuous job exit failed.", e);
        }
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        nameNode.execInContainer("bash", "-c", "hdfs dfs -rm -r -f /continuous || true");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Continuous discovery is a long-running job; only run in zeta engine.")
    public void testHdfsBinaryUpdateModeContinuousDiscoveryWithNonRecursiveScan(
            TestContainer container) throws IOException, InterruptedException {
        resetContinuousTestPath();

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<org.testcontainers.containers.Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/hdfs_binary_update_distcp_continuous_non_recursive.conf",
                                        jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        createHdfsDirectories("/continuous/src/subdir");
        putHdfsFile("/continuous/src/root.bin", "root");
        putHdfsFile("/continuous/src/subdir/nested.bin", "nested");

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "root", readHdfsFile("/continuous/dst/root.bin")));

        Thread.sleep(3000);
        Assertions.assertFalse(isHdfsFileExists("/continuous/dst/subdir/nested.bin"));

        org.testcontainers.containers.Container.ExecResult cancelResult =
                container.cancelJob(jobId);
        Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

        org.testcontainers.containers.Container.ExecResult execResult;
        try {
            execResult = jobFuture.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Wait continuous job exit failed.", e);
        }
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        nameNode.execInContainer("bash", "-c", "hdfs dfs -rm -r -f /continuous || true");
    }

    @TestTemplate
    public void testHdfsBinaryUpdateModeDistcpWithNonRecursiveScan(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        createHdfsDirectories("/update/src/subdir", "/update/dst/subdir");
        putHdfsFile("/update/src/root.bin", "root-updated-v2");
        putHdfsFile("/update/src/subdir/nested.bin", "nest-updated-v2");
        putHdfsFile("/update/dst/root.bin", "root-stale-v1");
        putHdfsFile("/update/dst/subdir/nested.bin", "nest-stale-v1");

        org.testcontainers.containers.Container.ExecResult execResult =
                container.executeJob("/hdfs_binary_update_non_recursive_distcp.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals("root-updated-v2", readHdfsFile("/update/dst/root.bin"));
        Assertions.assertEquals("nest-stale-v1", readHdfsFile("/update/dst/subdir/nested.bin"));
    }

    @TestTemplate
    public void testHdfsBinaryUpdateModeStrictChecksumSkipsNestedChangesWithNonRecursiveScan(
            TestContainer container) throws IOException, InterruptedException {
        resetUpdateTestPath();
        createHdfsDirectories("/update/src/subdir", "/update/dst/subdir");
        putHdfsFile("/update/src/root.bin", "root-same-v1");
        putHdfsFile("/update/src/subdir/nested.bin", "nest-new-v1");
        putHdfsFile("/update/dst/root.bin", "root-same-v1");
        putHdfsFile("/update/dst/subdir/nested.bin", "nest-old-v1");

        org.testcontainers.containers.Container.ExecResult execResult =
                container.executeJob("/hdfs_binary_update_non_recursive_strict_checksum.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals("root-same-v1", readHdfsFile("/update/dst/root.bin"));
        Assertions.assertEquals("nest-old-v1", readHdfsFile("/update/dst/subdir/nested.bin"));
    }

    private void resetUpdateTestPath() throws IOException, InterruptedException {
        nameNode.execInContainer("bash", "-c", "hdfs dfs -rm -r -f /update || true");
        org.testcontainers.containers.Container.ExecResult mkdirResult =
                nameNode.execInContainer(
                        "hdfs", "dfs", "-mkdir", "-p", "/update/src", "/update/dst", "/update/tmp");
        Assertions.assertEquals(0, mkdirResult.getExitCode());
    }

    private void resetContinuousTestPath() throws IOException, InterruptedException {
        nameNode.execInContainer("bash", "-c", "hdfs dfs -rm -r -f /continuous || true");
        org.testcontainers.containers.Container.ExecResult mkdirResult =
                nameNode.execInContainer(
                        "hdfs",
                        "dfs",
                        "-mkdir",
                        "-p",
                        "/continuous/src",
                        "/continuous/dst",
                        "/continuous/tmp");
        Assertions.assertEquals(0, mkdirResult.getExitCode());
    }

    private void resetSplitTestPath() throws IOException, InterruptedException {
        nameNode.execInContainer("bash", "-c", "hdfs dfs -rm -r -f /split || true");
        org.testcontainers.containers.Container.ExecResult mkdirResult =
                nameNode.execInContainer("hdfs", "dfs", "-mkdir", "-p", "/split/input");
        Assertions.assertEquals(0, mkdirResult.getExitCode());
    }

    private void resetRecursiveTestPath() throws IOException, InterruptedException {
        nameNode.execInContainer("bash", "-c", "hdfs dfs -rm -r -f /recursive || true");
        org.testcontainers.containers.Container.ExecResult mkdirResult =
                nameNode.execInContainer(
                        "hdfs", "dfs", "-mkdir", "-p", "/recursive/subdir/deeper/final");
        Assertions.assertEquals(0, mkdirResult.getExitCode());
    }

    private void putHdfsFile(String hdfsPath, String content)
            throws IOException, InterruptedException {
        String command = "printf '" + content + "' | hdfs dfs -put -f - " + hdfsPath;
        org.testcontainers.containers.Container.ExecResult putResult =
                nameNode.execInContainer("bash", "-c", command);
        Assertions.assertEquals(0, putResult.getExitCode());
    }

    private void createHdfsDirectories(String... hdfsPaths)
            throws IOException, InterruptedException {
        String command = "hdfs dfs -mkdir -p " + String.join(" ", hdfsPaths);
        org.testcontainers.containers.Container.ExecResult mkdirResult =
                nameNode.execInContainer("bash", "-c", command);
        Assertions.assertEquals(0, mkdirResult.getExitCode());
    }

    private void putHdfsClasspathFile(String resourcePath, String hdfsPath)
            throws IOException, InterruptedException {
        String tempFileName =
                "/tmp/"
                        + java.util.UUID.randomUUID()
                        + "-"
                        + java.nio.file.Paths.get(resourcePath).getFileName();
        ContainerUtil.copyFileIntoContainers(resourcePath, tempFileName, nameNode);
        org.testcontainers.containers.Container.ExecResult putResult =
                nameNode.execInContainer("hdfs", "dfs", "-put", "-f", tempFileName, hdfsPath);
        Assertions.assertEquals(0, putResult.getExitCode());
    }

    private void putHdfsSequentialLinesFile(String hdfsPath, int lineCount)
            throws IOException, InterruptedException {
        String command =
                "i=1; while [ $i -le "
                        + lineCount
                        + " ]; do echo $i; i=$((i+1)); done | hdfs dfs -put -f - "
                        + hdfsPath;
        org.testcontainers.containers.Container.ExecResult putResult =
                nameNode.execInContainer("bash", "-c", command);
        Assertions.assertEquals(0, putResult.getExitCode());
    }

    private String readHdfsFile(String hdfsPath) throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult catResult =
                nameNode.execInContainer("hdfs", "dfs", "-cat", hdfsPath);
        Assertions.assertEquals(0, catResult.getExitCode());
        return catResult.getStdout() == null ? "" : catResult.getStdout().trim();
    }

    private boolean isHdfsFileExists(String hdfsPath) throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult result =
                nameNode.execInContainer("hdfs", "dfs", "-test", "-f", hdfsPath);
        return result.getExitCode() == 0;
    }

    private long getHdfsFileMtimeSeconds(String hdfsPath) throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult statResult =
                nameNode.execInContainer("bash", "-c", "hdfs dfs -stat %Y " + hdfsPath);
        Assertions.assertEquals(0, statResult.getExitCode());
        return Long.parseLong(statResult.getStdout().trim());
    }
}
