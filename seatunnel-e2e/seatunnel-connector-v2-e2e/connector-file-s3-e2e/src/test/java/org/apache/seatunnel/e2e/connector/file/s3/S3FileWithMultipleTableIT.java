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

package org.apache.seatunnel.e2e.connector.file.s3;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason =
                "1.The apache-compress version is not compatible with apache-poi. 2.Spark Engine is not compatible with commons-net")
@Slf4j
public class S3FileWithMultipleTableIT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> s3Container;

    private static final String MINIO_IMAGE = "minio/minio:latest";

    private static final int S3_PORT = 9000;

    public static final String S3_SDK_DOWNLOAD =
            "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar";
    public static final String HADOOP_S3_DOWNLOAD =
            "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar";

    private String endpoint;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        s3Container =
                new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
                        .withNetwork(NETWORK)
                        .withExposedPorts(S3_PORT)
                        .withLogConsumer(new Slf4jLogConsumer(log))
                        .withEnv("MINIO_ROOT_USER", "myuser")
                        .withEnv("MINIO_ROOT_PASSWORD", "mypassword")
                        .withCommand("server", "/data")
                        .waitingFor(Wait.forLogMessage(".*", 1));

        List<String> portBind = new ArrayList<>();
        portBind.add("9000:9000");
        s3Container.setPortBindings(portBind);
        s3Container.start();

        endpoint = "http://" + s3Container.getHost() + ":" + s3Container.getMappedPort(9000);
    }

    @Override
    public void tearDown() throws Exception {
        if (s3Container != null) {
            s3Container.close();
        }
    }

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/s3/lib && cd /tmp/seatunnel/plugins/s3/lib && curl -O "
                                        + S3_SDK_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/plugins/s3/lib && curl -O "
                                        + HADOOP_S3_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/lib && curl -O " + S3_SDK_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/lib && curl -O " + HADOOP_S3_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    /** Copy data files to s3 */
    @TestTemplate
    public void addTestFiles(TestContainer container) throws IOException, InterruptedException {
        // Copy test files to s3
        S3Utils s3Utils = new S3Utils(endpoint);
        try {
            s3Utils.uploadTestFiles(
                    "/json/e2e.json",
                    "test/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                    true);
            s3Utils.uploadTestFiles(
                    "/text/e2e.txt",
                    "test/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                    true);
            s3Utils.uploadTestFiles(
                    "/excel/e2e.xlsx",
                    "test/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                    true);
            s3Utils.uploadTestFiles(
                    "/orc/e2e.orc",
                    "test/seatunnel/read/orc/name=tyrantlucifer/hobby=coding/e2e.orc",
                    true);
            s3Utils.uploadTestFiles(
                    "/parquet/e2e.parquet",
                    "test/seatunnel/read/parquet/name=tyrantlucifer/hobby=coding/e2e.parquet",
                    true);
            s3Utils.createDir("tmp/fake_empty");
        } finally {
            s3Utils.close();
        }
    }

    @TestTemplate
    public void testFakeToS3FileInMultipleTableMode_text(TestContainer testContainer)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(testContainer);
        helper.execute("/text/fake_to_s3_file_with_multiple_table.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_excel(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/excel/s3_excel_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_json(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/json/s3_file_json_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_orc(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/orc/s3_file_orc_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_parquet(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/parquet/s3_file_parquet_to_assert_with_multipletable.conf");
    }

    @TestTemplate
    public void testS3FileReadAndWriteInMultipleTableMode_text(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/text/s3_file_text_to_assert_with_multipletable.conf");
    }
}
