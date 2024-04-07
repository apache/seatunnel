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

package org.apache.seatunnel.e2e.connector.hive;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
@Disabled(
        "[HDFS/COS/OSS/S3] is not available in CI, if you want to run this test, please set up your own environment in the test case file, hadoop_hive_conf_path_local and ip below}")
@Slf4j
public class HiveIT extends TestSuiteBase implements TestResource {
    private static final String HADOOP_HIVE_CONF_PATH_LOCAL =
            "/Users/dailai/software/hadoop-3.3.3/etc/hadoop";
    private static final String HADOOP_HIVE_CONF_PATH_IN_CONTAINER = "/tmp/hadoop";

    private String hiveExeUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar";
    }

    private String hadoopAwsUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.6.5/hadoop-aws-2.6.5.jar";
    }

    private String aliyunSdkOssUrl() {
        return "https://repo1.maven.org/maven2/com/aliyun/oss/aliyun-sdk-oss/3.4.1/aliyun-sdk-oss-3.4.1.jar";
    }

    private String jdomUrl() {
        return "https://repo1.maven.org/maven2/org/jdom/jdom/1.1/jdom-1.1.jar";
    }

    private String hadoopAliyunUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aliyun/3.1.4/hadoop-aliyun-3.1.4.jar";
    }

    private String hadoopCosUrl() {
        return "https://repo1.maven.org/maven2/com/qcloud/cos/hadoop-cos/2.6.5-8.0.2/hadoop-cos-2.6.5-8.0.2.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("sh", "-c", "chmod -R 777 /etc/hosts");
                container.execInContainer("sh", "-c", "echo \"${IP01} hadoop01\" >> /etc/hosts");
                container.execInContainer("sh", "-c", "echo \"${IP02} hadoop02\" >> /etc/hosts");
                container.execInContainer("sh", "-c", "echo \"${IP03} hadoop03\" >> /etc/hosts");
                container.execInContainer("sh", "-c", "echo \"${IP04} hadoop04\" >> /etc/hosts");
                container.execInContainer("sh", "-c", "echo \"${IP05} hadoop05\" >> /etc/hosts");
                container.execInContainer("sh", "-c", "echo \"${IP06} hadoop06\" >> /etc/hosts");
                Assertions.assertTrue(
                        new File(HADOOP_HIVE_CONF_PATH_LOCAL).exists(),
                        HADOOP_HIVE_CONF_PATH_LOCAL + " must exist");
                container.execInContainer(
                        "sh", "-c", "mkdir -p " + HADOOP_HIVE_CONF_PATH_IN_CONTAINER);
                container.execInContainer(
                        "sh", "-c", "chmod -R 777 " + HADOOP_HIVE_CONF_PATH_IN_CONTAINER);
                // Copy local hadoop conf and hive conf to the container
                container.copyFileToContainer(
                        MountableFile.forHostPath(HADOOP_HIVE_CONF_PATH_LOCAL),
                        HADOOP_HIVE_CONF_PATH_IN_CONTAINER);

                // The jar of hive-exec
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Hive/lib && cd /tmp/seatunnel/plugins/Hive/lib && wget "
                                        + hiveExeUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
                // The jar of s3
                Container.ExecResult downloadS3Commands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "cd /tmp/seatunnel/plugins/Hive/lib && wget " + hadoopAwsUrl());
                Assertions.assertEquals(
                        0, downloadS3Commands.getExitCode(), downloadS3Commands.getStderr());
                // The jar of oss
                Container.ExecResult downloadOssCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "cd /tmp/seatunnel/plugins/Hive/lib && wget "
                                        + aliyunSdkOssUrl()
                                        + " && wget "
                                        + jdomUrl()
                                        + " && wget "
                                        + hadoopAliyunUrl());
                Assertions.assertEquals(
                        0, downloadOssCommands.getExitCode(), downloadOssCommands.getStderr());
                // The jar of cos
                Container.ExecResult downloadCosCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "cd /tmp/seatunnel/plugins/Hive/lib && wget " + hadoopCosUrl());
                Assertions.assertEquals(
                        0, downloadCosCommands.getExitCode(), downloadCosCommands.getStderr());
            };

    @Override
    public void startUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}

    private void executeJob(TestContainer container, String job1, String job2)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob(job1);
        Assertions.assertEquals(0, execResult.getExitCode());

        Container.ExecResult readResult = container.executeJob(job2);
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testFakeSinkHiveOnHDFS(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_hdfs.conf", "/hive_on_hdfs_to_assert.conf");
    }

    @TestTemplate
    public void testFakeSinkHiveOnS3(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_s3.conf", "/hive_on_s3_to_assert.conf");
    }

    @TestTemplate
    public void testFakeSinkHiveOnOSS(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_oss.conf", "/hive_on_oss_to_assert.conf");
    }

    @TestTemplate
    public void testFakeSinkHiveOnCos(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_cos.conf", "/hive_on_cos_to_assert.conf");
    }
}
