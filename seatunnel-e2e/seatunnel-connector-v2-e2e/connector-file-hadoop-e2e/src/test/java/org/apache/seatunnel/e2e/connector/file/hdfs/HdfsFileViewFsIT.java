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
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
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
public class HdfsFileViewFsIT extends TestSuiteBase implements TestResource {

    private static final String HADOOP_IMAGE = "apache/hadoop:3";

    private GenericContainer<?> nameNode1;
    private GenericContainer<?> dataNode1;
    private GenericContainer<?> nameNode2;
    private GenericContainer<?> dataNode2;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                container.copyFileToContainer(
                        MountableFile.forClasspathResource("viewfs/core-site.xml"),
                        "/tmp/seatunnel/config/viewfs/core-site.xml");
                log.info("ViewFS core-site.xml copied to container");
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        nameNode1 =
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
                                        DockerLoggerFactory.getLogger(
                                                HADOOP_IMAGE + ":namenode1")));
        dataNode1 =
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
                        .withExposedPorts(9864, 9866, 9867)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(HADOOP_IMAGE + ":datanode1")))
                        .dependsOn(nameNode1);
        nameNode2 =
                new GenericContainer<>(DockerImageName.parse(HADOOP_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("namenode2")
                        .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-root/dfs/name")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("viewfs/cluster2/core-site.xml"),
                                "/opt/hadoop/etc/hadoop/core-site.xml")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("viewfs/cluster2/hdfs-site.xml"),
                                "/opt/hadoop/etc/hadoop/hdfs-site.xml")
                        .withCommand("sh", "-c", "hdfs namenode -format -force && hdfs namenode")
                        .withExposedPorts(9870, 9000)
                        .waitingFor(
                                Wait.forHttp("/")
                                        .forPort(9870)
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                HADOOP_IMAGE + ":namenode2")));
        dataNode2 =
                new GenericContainer<>(DockerImageName.parse(HADOOP_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("datanode2")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("viewfs/cluster2/core-site.xml"),
                                "/opt/hadoop/etc/hadoop/core-site.xml")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("viewfs/cluster2/hdfs-site.xml"),
                                "/opt/hadoop/etc/hadoop/hdfs-site.xml")
                        .withCommand("hdfs", "datanode")
                        .withExposedPorts(9864, 9866, 9867)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(HADOOP_IMAGE + ":datanode2")))
                        .dependsOn(nameNode2);
        Startables.deepStart(Stream.of(nameNode1, dataNode1, nameNode2, dataNode2)).join();
        Thread.sleep(5000);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (dataNode1 != null) {
            dataNode1.stop();
        }
        if (nameNode1 != null) {
            nameNode1.stop();
            log.info("HDFS Cluster 1 stopped");
        }
        if (dataNode2 != null) {
            dataNode2.stop();
        }
        if (nameNode2 != null) {
            nameNode2.stop();
            log.info("HDFS Cluster 2 stopped");
        }
    }

    @TestTemplate
    public void testViewFsWrite(TestContainer container) throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult execResult =
                container.executeJob("/fake_to_hdfs_viewfs.conf");
        Assertions.assertEquals(
                0, execResult.getExitCode(), "SeaTunnel job should complete successfully");

        // Verify files were written to cluster1 via ViewFS mount point /data
        org.testcontainers.containers.Container.ExecResult lsResult =
                nameNode1.execInContainer("hdfs", "dfs", "-ls", "/data/output");
        Assertions.assertEquals(0, lsResult.getExitCode(), "Directory /data/output should exist");
    }

    @TestTemplate
    public void testViewFsRead(TestContainer container) throws IOException, InterruptedException {
        org.testcontainers.containers.Container.ExecResult writeResult =
                container.executeJob("/fake_to_hdfs_viewfs.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());
        org.testcontainers.containers.Container.ExecResult readResult =
                container.executeJob("/hdfs_viewfs_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }
}
