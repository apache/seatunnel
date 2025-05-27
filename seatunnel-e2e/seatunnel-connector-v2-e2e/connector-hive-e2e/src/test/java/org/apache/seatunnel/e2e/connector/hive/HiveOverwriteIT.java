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
public class HiveOverwriteIT extends TestSuiteBase implements TestResource {
    private static final String CREATE_SQL =
            "CREATE TABLE test_hive_sink_on_hdfs_overwrite"
                    + "("
                    + "    pk_id  BIGINT,"
                    + "    name   STRING,"
                    + "    score  INT"
                    + ")";

    private static final String HMS_HOST = "metastore";
    private static final String HIVE_SERVER_HOST = "hiveserver2";

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
    private String pluginHiveDir = "/tmp/seatunnel/plugins/Hive/lib";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                // The jar of hive-exec
                Container.ExecResult downloadHiveExeCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p "
                                        + pluginHiveDir
                                        + " && cd "
                                        + pluginHiveDir
                                        + " && wget "
                                        + hiveExeUrl());
                Assertions.assertEquals(
                        0,
                        downloadHiveExeCommands.getExitCode(),
                        downloadHiveExeCommands.getStderr());
                Container.ExecResult downloadLibFb303Commands =
                        container.execInContainer(
                                "sh", "-c", "cd " + pluginHiveDir + " && wget " + libFb303Url());
                Assertions.assertEquals(
                        0,
                        downloadLibFb303Commands.getExitCode(),
                        downloadLibFb303Commands.getStderr());
                // The jar of s3
                Container.ExecResult downloadS3Commands =
                        container.execInContainer(
                                "sh", "-c", "cd " + pluginHiveDir + " && wget " + hadoopAwsUrl());
                Assertions.assertEquals(
                        0, downloadS3Commands.getExitCode(), downloadS3Commands.getStderr());
                // The jar of oss
                Container.ExecResult downloadOssCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "cd "
                                        + pluginHiveDir
                                        + " && wget "
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
                                "sh", "-c", "cd " + pluginHiveDir + " && wget " + hadoopCosUrl());
                Assertions.assertEquals(
                        0, downloadCosCommands.getExitCode(), downloadCosCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        hmsContainer =
                HiveContainer.hmsStandalone()
                        .withCreateContainerCmdModifier(cmd -> cmd.withName(HMS_HOST))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HMS_HOST);
        hmsContainer.setPortBindings(Collections.singletonList("9083:9083"));

        Startables.deepStart(Stream.of(hmsContainer)).join();
        log.info("HMS just started");

        hiveServerContainer =
                HiveContainer.hiveServer()
                        .withNetwork(NETWORK)
                        .withCreateContainerCmdModifier(cmd -> cmd.withName(HIVE_SERVER_HOST))
                        .withNetworkAliases(HIVE_SERVER_HOST)
                        .withFileSystemBind("/tmp/data", "/opt/hive/data")
                        .withEnv("SERVICE_OPTS", "-Dhive.metastore.uris=thrift://metastore:9083")
                        .withEnv("IS_RESUME", "true")
                        .dependsOn(hmsContainer);
        hiveServerContainer.setPortBindings(Collections.singletonList("10004:10000"));

        Startables.deepStart(Stream.of(hiveServerContainer)).join();
        log.info("HiveServer2 just started");
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(10L))
                .pollInterval(Duration.ofSeconds(3L))
                .untilAsserted(this::initializeConnection);
        prepareTable();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (hmsContainer != null) {
            log.info(hmsContainer.execInContainer("cat", "/tmp/hive/hive.log").getStdout());
            hmsContainer.close();
        }
        if (hiveServerContainer != null) {
            log.info(hiveServerContainer.execInContainer("cat", "/tmp/hive/hive.log").getStdout());
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

    /**
     * Tests the Hive sink connector with overwrite mode functionality. This test validates the data
     * insertion and overwrite capabilities of the Hive connector through a series of operations:
     *
     * <p>1. First insertion: Inserts 3 records into the target Hive table (table contains 3
     * records) 2. Second insertion: Appends 2 more records (table contains 5 records) 3. Third
     * insertion: Uses overwrite mode to insert 1 record (table now contains only 1 record, previous
     * data is overwritten)
     *
     * <p>Each operation is followed by an assertion job to verify the expected data state.
     *
     * @param container The test container that provides the execution environment
     * @throws IOException If an I/O error occurs during job execution
     * @throws InterruptedException If the job execution is interrupted
     */
    @TestTemplate
    public void testFakeSinkHiveOverwrite(TestContainer container)
            throws IOException, InterruptedException {
        //  Inserts 3 rows of data into the target table, resulting in the table having 3 rows.
        Container.ExecResult execResult1 =
                container.executeJob("/overwrite/fake_to_hive_overwrite_1.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());

        Container.ExecResult readResult1 =
                container.executeJob("/overwrite/hive_to_assert_overwrite_1.conf");
        Assertions.assertEquals(0, readResult1.getExitCode());

        Container.ExecResult execResult2 =
                container.executeJob("/overwrite/fake_to_hive_overwrite_2.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());

        Container.ExecResult readResult2 =
                container.executeJob("/overwrite/hive_to_assert_overwrite_2.conf");
        Assertions.assertEquals(0, readResult2.getExitCode());

        Container.ExecResult execResult3 =
                container.executeJob("/overwrite/fake_to_hive_overwrite_3.conf");
        Assertions.assertEquals(0, execResult3.getExitCode());

        Container.ExecResult readResult3 =
                container.executeJob("/overwrite/hive_to_assert_overwrite_3.conf");
        Assertions.assertEquals(0, readResult3.getExitCode());
    }
}
