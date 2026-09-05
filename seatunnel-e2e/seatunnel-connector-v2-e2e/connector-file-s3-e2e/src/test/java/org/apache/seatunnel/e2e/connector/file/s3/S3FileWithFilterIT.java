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
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.hadoop.fs.s3a.S3AFileSystem;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * MinIO-based S3 E2E test suite for connector-file-s3, covering:
 *
 * <ul>
 *   <li>file filter by path/name pattern
 *   <li>logical file split (enable_file_split/file_split_size) for parallel read
 * </ul>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class S3FileWithFilterIT extends SeaTunnelContainer {
    private GenericContainer<?> s3Container;

    private static final String MINIO_IMAGE = "minio/minio:RELEASE.2024-06-13T22-53-53Z";

    private static final int S3_PORT = 9000;

    private static final String S3_CONTAINER_HOST = "s3";

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        s3Container =
                new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
                        .withNetwork(NETWORK)
                        .withExposedPorts(S3_PORT)
                        .withNetworkAliases(S3_CONTAINER_HOST)
                        .withLogConsumer(new Slf4jLogConsumer(log))
                        .withEnv("MINIO_ROOT_USER", "minioadmin")
                        .withEnv("MINIO_ROOT_PASSWORD", "minioadmin")
                        .withCommand("server", "/data")
                        .waitingFor(Wait.forLogMessage(".*", 1));
        s3Container.start();
        S3Utils.initialize(
                String.format(
                        "http://%s:%s", s3Container.getHost(), s3Container.getMappedPort(S3_PORT)));

        super.startUp();
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> server)
            throws IOException, InterruptedException {
        super.executeExtraCommands(server);
        DependencyJar.staged("aws-java-sdk-bundle.jar").addTo(server, SEATUNNEL_HOME + "lib");
        DependencyJar.of(S3AFileSystem.class).addTo(server, SEATUNNEL_HOME + "lib");
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
        if (s3Container != null) {
            s3Container.close();
        }
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

    @Test
    public void testS3FileTextEnableSplitToAssert() throws IOException, InterruptedException {
        S3Utils.uploadTestFiles(
                "/text/e2e_split_with_header.txt",
                "/test/seatunnel/read/split/text/e2e_split_with_header.txt",
                true);
        Container.ExecResult execResult =
                executeJob("/text/s3_file_text_enable_split_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @Test
    public void testS3BinaryUpdateModeContinuousDiscovery()
            throws IOException, InterruptedException {
        S3Utils.deletePrefix("/continuous/");
        S3Utils.uploadContent("/continuous/src/test1.bin", "abc");

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return executeJob(
                                        "/binary/s3_file_binary_update_distcp_continuous.conf",
                                        jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertContinuousJobIsRunning(jobFuture);
                                Assertions.assertTrue(
                                        S3Utils.objectExists("/continuous/dst/test1.bin"));
                                Assertions.assertEquals(
                                        "abc", S3Utils.readContent("/continuous/dst/test1.bin"));
                            });

            S3Utils.uploadContent("/continuous/src/test2.bin", "def");
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertContinuousJobIsRunning(jobFuture);
                                Assertions.assertTrue(
                                        S3Utils.objectExists("/continuous/dst/test2.bin"));
                                Assertions.assertEquals(
                                        "def", S3Utils.readContent("/continuous/dst/test2.bin"));
                            });
        } finally {
            Container.ExecResult cancelResult = cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
        }

        try {
            Container.ExecResult execResult = jobFuture.get(120, TimeUnit.SECONDS);
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        } catch (Exception e) {
            throw new RuntimeException("Wait continuous S3 job exit failed.", e);
        } finally {
            S3Utils.deletePrefix("/continuous/");
        }
    }

    private static void assertContinuousJobIsRunning(
            CompletableFuture<Container.ExecResult> jobFuture) {
        if (!jobFuture.isDone()) {
            return;
        }
        Container.ExecResult result = jobFuture.join();
        Assertions.fail(
                "Continuous S3 job exited before cancellation. exitCode="
                        + result.getExitCode()
                        + ", stderr="
                        + result.getStderr());
    }
}
