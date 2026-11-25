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

package org.apache.seatunnel.e2e.connector.lance;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ReadOptions;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

/**
 * Regression test for the Lance sink s3 credentials bug (issue #13431): credentials configured in
 * {@code lance.write.storage.options} must be propagated to every remote dataset operation (the
 * probe open of an existing dataset, the re-open after create, the fragment write and the re-open
 * after each flush), not only to dataset creation. The fixture provides the credentials ONLY
 * through {@code lance.write.storage.options}; neither the SeaTunnel container nor the test JVM
 * carries any AWS_* environment variable, so with unfixed code the jobs below fail at an
 * unconfigured open or fragment path and this test fails with them.
 *
 * <p>Readback is the acceptance assertion: the test JVM opens the dataset through the Lance SDK
 * with the configured storage options and asserts the exact committed row count, so a JNI
 * initialization failure fails the test instead of weakening it. The MinIO object listing after the
 * count is only a supplementary layout check.
 *
 * <p>Verified against the Lance API/driver versions pinned by connector-lance: lance-core 0.33.0,
 * lance-namespace 0.0.14 and opendal 0.48.0. Local-filesystem jobs are covered by {@link LanceIT}
 * and keep working unchanged.
 */
@Slf4j
@DisabledOnContainer(
        value = {
            TestContainerId.FLINK_1_13,
            TestContainerId.FLINK_1_14,
            TestContainerId.FLINK_1_15,
            TestContainerId.FLINK_1_16,
            TestContainerId.FLINK_1_17,
            TestContainerId.FLINK_1_18,
            TestContainerId.SPARK_2_4,
            TestContainerId.SPARK_3_3
        },
        type = {},
        disabledReason = "Lance connector does not support Flink and lower than Spark 3.4 yet")
@DisabledOnOs(OS.WINDOWS)
public class LanceS3IT extends TestSuiteBase implements TestResource {

    private static final String DATASET_CONF = "/lance/fake_to_lance_s3.conf";
    private static final String BUCKET = "lance-s3-it";
    private static final int ROWS_PER_JOB = 100;

    // Docker Hub's minio/minio repository no longer serves anonymous/unauthenticated pulls
    // ("pull access denied ... repository does not exist or may require 'docker login'"). The
    // old quay.io/minio/minio repository is also unavailable. Use a digest-pinned public mirror
    // of MinIO RELEASE.2025-04-22T22-12-26Z, the same image used by HudiSparkS3MultiTableIT.
    private static final String MINIO_DOCKER_IMAGE =
            "ghcr.io/teableio/minio@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e";
    private static final String MINIO_HOST = "minio";
    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER_NAME = "minio";
    private static final String MINIO_USER_PASSWORD = "miniominio";

    private MinIOContainer minioContainer;
    private MinioClient minioClient;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                // The regression only proves option propagation if the job container has no
                // ambient AWS credentials to fall back on.
                Container.ExecResult envCheck =
                        container.execInContainer("sh", "-c", "printenv | grep -c '^AWS_' || true");
                Assertions.assertEquals(
                        "0",
                        envCheck.getStdout().trim(),
                        "the job container must not carry ambient AWS_* credentials");
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        minioContainer =
                // MinIOContainer validates its image name is a recognized substitute for
                // "minio/minio"; the community mirror needs an explicit compatibility
                // declaration or Testcontainers rejects it with IllegalStateException.
                new MinIOContainer(
                                DockerImageName.parse(MINIO_DOCKER_IMAGE)
                                        .asCompatibleSubstituteFor("minio/minio"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MINIO_HOST)
                        .withUserName(MINIO_USER_NAME)
                        .withPassword(MINIO_USER_PASSWORD)
                        .withExposedPorts(MINIO_PORT);
        minioContainer.start();

        minioClient =
                MinioClient.builder()
                        .endpoint(minioContainer.getS3URL())
                        .credentials(MINIO_USER_NAME, MINIO_USER_PASSWORD)
                        .build();
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
        Assertions.assertTrue(
                minioClient.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build()));
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (minioContainer != null) {
            minioContainer.close();
        }
    }

    @TestTemplate
    public void testFreshDatasetWriteAndReadback(TestContainer container) throws Exception {
        String datasetDir = datasetDir(container, "lance_s3_fresh_table");
        executeWriteJob(container, datasetDir);
        assertRowCountEventually(datasetDir, ROWS_PER_JOB);
    }

    @TestTemplate
    public void testExistingDatasetReopenAndAppend(TestContainer container) throws Exception {
        String datasetDir = datasetDir(container, "lance_s3_append_table");
        executeWriteJob(container, datasetDir);
        executeWriteJob(container, datasetDir);
        // Exactly ROWS_PER_JOB * 2: a duplicated append would surface as more rows, a failed
        // reopen as fewer (or as a failed job above).
        assertRowCountEventually(datasetDir, ROWS_PER_JOB * 2L);
    }

    private void executeWriteJob(TestContainer container, String datasetDir)
            throws IOException, InterruptedException {
        Container.ExecResult writeResult =
                container.executeJob(
                        DATASET_CONF, Collections.singletonList("dataset_dir=" + datasetDir));
        if (writeResult.getExitCode() != 0) {
            log.error("Job execution failed with exit code: {}", writeResult.getExitCode());
            log.error("STDOUT: {}", writeResult.getStdout());
            log.error("STDERR: {}", writeResult.getStderr());
            log.error("Container logs: {}", container.getServerLogs());
        }
        Assertions.assertEquals(
                0,
                writeResult.getExitCode(),
                "Job execution failed. STDOUT: "
                        + writeResult.getStdout()
                        + "\nSTDERR: "
                        + writeResult.getStderr()
                        + "\nContainer logs: "
                        + container.getServerLogs());
    }

    private String datasetDir(TestContainer container, String tableName) {
        return container.identifier().name().toLowerCase(Locale.ROOT).replace('_', '-')
                + "/"
                + tableName;
    }

    private void assertRowCountEventually(String datasetDir, long expectedRows) throws Exception {
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expectedRows, countRows(datasetDir)));
        assertDatasetObjectsExist(datasetDir);
    }

    private long countRows(String datasetDir) {
        String datasetUri = "s3://" + BUCKET + "/" + datasetDir;
        ReadOptions readOptions =
                new ReadOptions.Builder()
                        .setStorageOptions(storageOptions(minioContainer.getS3URL()))
                        .build();
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(allocator, datasetUri, readOptions)) {
            return dataset.countRows();
        }
    }

    // The job conf talks to MinIO over the shared container network (http://minio:9000); this
    // JVM reaches it through the host-mapped endpoint instead.
    private Map<String, String> storageOptions(String endpointUrl) {
        Map<String, String> options = new HashMap<>();
        options.put("aws_endpoint_url", endpointUrl);
        options.put("aws_access_key_id", MINIO_USER_NAME);
        options.put("aws_secret_access_key", MINIO_USER_PASSWORD);
        options.put("aws_default_region", "us-east-1");
        options.put("allow_http", "true");
        return options;
    }

    private void assertDatasetObjectsExist(String datasetDir) throws Exception {
        boolean manifest = false;
        boolean data = false;
        for (Result<Item> result :
                minioClient.listObjects(
                        ListObjectsArgs.builder()
                                .bucket(BUCKET)
                                .recursive(true)
                                .prefix(datasetDir + "/")
                                .build())) {
            Item item = result.get();
            String name = item.objectName();
            if (name.endsWith(".manifest")) {
                manifest = true;
            }
            if (name.contains("/data/") && item.size() > 0) {
                data = true;
            }
        }
        Assertions.assertTrue(manifest, "no manifest object under " + datasetDir);
        Assertions.assertTrue(data, "no data object under " + datasetDir);
    }
}
