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
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

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

    private void resetUpdateTestPath() throws IOException, InterruptedException {
        nameNode.execInContainer("bash", "-c", "hdfs dfs -rm -r -f /update || true");
        org.testcontainers.containers.Container.ExecResult mkdirResult =
                nameNode.execInContainer(
                        "hdfs", "dfs", "-mkdir", "-p", "/update/src", "/update/dst", "/update/tmp");
        Assertions.assertEquals(0, mkdirResult.getExitCode());
    }

    private void putHdfsFile(String hdfsPath, String content)
            throws IOException, InterruptedException {
        String command = "printf '" + content + "' | hdfs dfs -put -f - " + hdfsPath;
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
}
