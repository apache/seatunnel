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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.lifecycle.Startables;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
@Slf4j
public class HiveIT extends TestSuiteBase implements TestResource {
    private static final String CREATE_SQL =
            "CREATE TABLE test_hive_sink_on_hdfs"
                    + "("
                    + "    pk_id  BIGINT,"
                    + "    name   STRING,"
                    + "    score  INT"
                    + ")";

    private static final String HMS_HOST = "hivee2e";
    private static final String HIVE_SERVER_HOST = "hiveserver2e2e";

    private String hiveExeUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar";
    }

    private String libFb303Url() {
        return "https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar";
    }

    private String hadoopAwsUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar";
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

    private HiveContainer hiveServerContainer;
    private HiveContainer hmsContainer;
    private Connection hiveConnection;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("sh", "-c", "chmod -R 777 /etc/hosts");
                // To avoid get a canonical host from a docker DNS server
                container.execInContainer("sh", "-c", "echo `getent hosts hivee2e` >> /etc/hosts");
                // The jar of hive-exec
                Container.ExecResult downloadHiveExeCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p /tmp/seatunnel/lib && cd /tmp/seatunnel/lib && wget "
                                        + hiveExeUrl());
                Assertions.assertEquals(
                        0,
                        downloadHiveExeCommands.getExitCode(),
                        downloadHiveExeCommands.getStderr());
                Container.ExecResult downloadLibFb303Commands =
                        container.execInContainer(
                                "sh", "-c", "cd /tmp/seatunnel/lib && wget " + libFb303Url());
                Assertions.assertEquals(
                        0,
                        downloadLibFb303Commands.getExitCode(),
                        downloadLibFb303Commands.getStderr());
                // The jar of s3
                Container.ExecResult downloadS3Commands =
                        container.execInContainer(
                                "sh", "-c", "cd /tmp/seatunnel/lib && wget " + hadoopAwsUrl());
                Assertions.assertEquals(
                        0, downloadS3Commands.getExitCode(), downloadS3Commands.getStderr());
                // The jar of oss
                Container.ExecResult downloadOssCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "cd /tmp/seatunnel/lib && wget "
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
                                "sh", "-c", "cd /tmp/seatunnel/lib && wget " + hadoopCosUrl());
                Assertions.assertEquals(
                        0, downloadCosCommands.getExitCode(), downloadCosCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        hmsContainer =
                HiveContainer.hmsStandalone().withNetwork(NETWORK).withNetworkAliases(HMS_HOST);
        hmsContainer.setPortBindings(Collections.singletonList("9083:9083"));

        Startables.deepStart(Stream.of(hmsContainer)).join();
        log.info(hmsContainer.getLogs());
        log.info("HMS just started");

        hiveServerContainer =
                HiveContainer.hiveServer()
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HIVE_SERVER_HOST)
                        .withEnv("SERVICE_OPTS", "-Dhive.metastore.uris=thrift://hivee2e:9083")
                        .withEnv("IS_RESUME", "true")
                        .dependsOn(hmsContainer);
        hiveServerContainer.setPortBindings(Collections.singletonList("10000:10000"));

        Startables.deepStart(Stream.of(hiveServerContainer)).join();
        log.info(hiveServerContainer.getLogs());
        log.info("HiveServer2 just started");

        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(10L))
                .untilAsserted(this::initializeConnection);
        prepareTable();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (hmsContainer != null) {
            hmsContainer.close();
        }
        if (hiveServerContainer != null) {
            hiveServerContainer.close();
        }
    }

    private void initializeConnection()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
                    SQLException {
        this.hiveConnection = this.hiveServerContainer.getConnection();
    }

    private void prepareTable() throws Exception {
        log.info(
                String.format(
                        "Databases are %s",
                        this.hmsContainer.createMetaStoreClient().getAllDatabases()));
        try (Statement statement = this.hiveConnection.createStatement()) {
            statement.execute(CREATE_SQL);
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw exception;
        }
    }

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
    @Disabled(
            "[HDFS/COS/OSS/S3] is not available in CI, if you want to run this test, please set up your own environment in the test case file, hadoop_hive_conf_path_local and ip below}")
    public void testFakeSinkHiveOnS3(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_s3.conf", "/hive_on_s3_to_assert.conf");
    }

    @TestTemplate
    @Disabled(
            "[HDFS/COS/OSS/S3] is not available in CI, if you want to run this test, please set up your own environment in the test case file, hadoop_hive_conf_path_local and ip below}")
    public void testFakeSinkHiveOnOSS(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_oss.conf", "/hive_on_oss_to_assert.conf");
    }

    @TestTemplate
    @Disabled(
            "[HDFS/COS/OSS/S3] is not available in CI, if you want to run this test, please set up your own environment in the test case file, hadoop_hive_conf_path_local and ip below}")
    public void testFakeSinkHiveOnCos(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_cos.conf", "/hive_on_cos_to_assert.conf");
    }
}
