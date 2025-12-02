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

import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3FileWithFilterIT extends SeaTunnelContainer {
    private GenericContainer<?> s3Container;

    private static final String MINIO_IMAGE = "minio/minio:RELEASE.2024-06-13T22-53-53Z";

    private static final int S3_PORT = 9000;

    private static final String S3_CONTAINER_HOST = "s3";

    protected static final String AWS_SDK_DOWNLOAD =
            "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar";
    protected static final String HADOOP_AWS_DOWNLOAD =
            "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar";

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        s3Container =
                new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
                        .withNetwork(NETWORK)
                        .withExposedPorts(S3_PORT)
                        .withNetworkAliases(S3_CONTAINER_HOST)
                        .withCreateContainerCmdModifier(
                                cmd ->
                                        cmd.withPortBindings(
                                                new PortBinding(
                                                        Ports.Binding.bindPort(S3_PORT),
                                                        new ExposedPort(S3_PORT))))
                        .withLogConsumer(new Slf4jLogConsumer(log))
                        .withEnv("MINIO_ROOT_USER", "minioadmin")
                        .withEnv("MINIO_ROOT_PASSWORD", "minioadmin")
                        .withCommand("server", "/data")
                        .waitingFor(Wait.forLogMessage(".*", 1));
        s3Container.start();

        super.startUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (s3Container != null) {
            s3Container.close();
        }
    }

    @Override
    protected String[] buildStartCommand() {
        return new String[] {
            "bash",
            "-c",
            "wget -P "
                    + SEATUNNEL_HOME
                    + "lib "
                    + AWS_SDK_DOWNLOAD
                    + " &&"
                    + "wget -P "
                    + SEATUNNEL_HOME
                    + "lib "
                    + HADOOP_AWS_DOWNLOAD
                    + " &&"
                    + ContainerUtil.adaptPathForWin(
                            Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString())
        };
    }

    @Test
    public void testS3ToAssertForJsonFilter() throws IOException, InterruptedException {

        // Copy test files to s3
        S3Utils.uploadTestFiles(
                "/json/e2e.json",
                "/test/seatunnel/read/filter/json/name=tyrantlucifer/hobby=codin/e2e.json",
                true);

        S3Utils.uploadTestFiles(
                "/json/e2e.json",
                "/test/seatunnel/read/filter/json2025/name=tyrantlucifer/hobby=codin/e2e.json",
                true);

        S3Utils.uploadTestFiles(
                "/text/e2e.txt",
                "/test/seatunnel/read/filter/json2025/name=tyrantlucifer/hobby=codin/e2e_2025.txt",
                true);

        S3Utils.uploadTestFiles(
                "/json/e2e.json",
                "/test/seatunnel/read/filter/json2024/name=tyrantlucifer/hobby=codin/e2e_2024.json",
                true);

        S3Utils.uploadTestFiles(
                "/text/e2e.txt",
                "/test/seatunnel/read/filter/text/name=tyrantlucifer/hobby=codin/e2e.txt",
                true);
        // -----filter based on the file directory at the same time, the expression needs to start
        Container.ExecResult execPathResult =
                executeJob("/json/s3_to_access_for_json_path_filter.conf");
        Assertions.assertEquals(0, execPathResult.getExitCode());

        // -------filter based on file names, just simply write the regular file names--------
        Container.ExecResult execNameResult =
                executeJob("/json/s3_to_access_for_json_name_filter.conf");
        Assertions.assertEquals(0, execNameResult.getExitCode());
    }
}
