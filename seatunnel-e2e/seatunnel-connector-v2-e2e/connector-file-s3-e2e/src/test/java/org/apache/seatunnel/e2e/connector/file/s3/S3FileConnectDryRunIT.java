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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the source factory metadata contract against MinIO without submitting a job. */
@Timeout(60)
public class S3FileConnectDryRunIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "minio/minio:RELEASE.2024-06-13T22-53-53Z";
    private static final String BUCKET = "dry-run-events";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private GenericContainer<?> minio;
    private AmazonS3 admin;
    private String endpoint;

    @BeforeAll
    @Override
    public void startUp() {
        minio =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                        .withCommand("server", "/data")
                        .withExposedPorts(9000)
                        .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000))
                        .withStartupTimeout(Duration.ofMinutes(2))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));
        minio.start();
        endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
        admin =
                AmazonS3ClientBuilder.standard()
                        .withCredentials(
                                new AWSStaticCredentialsProvider(
                                        new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                        .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                        .withPathStyleAccessEnabled(true)
                        .build();
        admin.createBucket(BUCKET);
        admin.putObject(BUCKET, "events/data.json", "{\"event_id\":1}");
        admin.createBucket("dry-run-empty");
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (admin != null) {
                admin.shutdown();
            }
        } finally {
            if (minio != null) {
                minio.stop();
            }
        }
    }

    @Test
    void shouldValidateObjectAndPrefixWithoutChangingStoredData() throws Exception {
        String etag = admin.getObjectMetadata(BUCKET, "events/data.json").getETag();
        validate(sourceConfig(endpoint, BUCKET, "/events/data.json"));
        validate(sourceConfig(endpoint, BUCKET, "/events"));
        assertEquals(etag, admin.getObjectMetadata(BUCKET, "events/data.json").getETag());
        assertEquals("{\"event_id\":1}", admin.getObjectAsString(BUCKET, "events/data.json"));
        assertEquals(1, admin.listObjectsV2(BUCKET).getKeyCount());
    }

    @Test
    void shouldRejectMissingBatchPrefix() {
        FileNotFoundException failure =
                assertThrows(
                        FileNotFoundException.class,
                        () -> validate(sourceConfig(endpoint, BUCKET, "/future")));
        assertTrue(failure.getMessage().contains("batch source path"));
    }

    @Test
    void shouldAcceptEmptyContinuousPrefix() {
        Map<String, Object> config = sourceConfig(endpoint, BUCKET, "/future");
        config.put("discovery_mode", "continuous");
        assertDoesNotThrow(() -> validate(config));
    }

    @Test
    void shouldAcceptAccessibleEmptyBucketRoot() {
        assertDoesNotThrow(() -> validate(sourceConfig(endpoint, "dry-run-empty", "/")));
    }

    @Test
    void shouldRejectMissingBucketInContinuousMode() {
        Map<String, Object> config = sourceConfig(endpoint, "dry-run-missing", "/future");
        config.put("discovery_mode", "continuous");
        AmazonS3Exception failure = assertThrows(AmazonS3Exception.class, () -> validate(config));
        assertEquals(404, failure.getStatusCode());
        assertEquals("NoSuchBucket", failure.getErrorCode());
    }

    @Test
    void shouldRejectIncorrectCredentials() {
        Map<String, Object> config = sourceConfig(endpoint, BUCKET, "/events/data.json");
        config.put("secret_key", "incorrect-test-secret");
        AmazonS3Exception failure = assertThrows(AmazonS3Exception.class, () -> validate(config));
        assertEquals(403, failure.getStatusCode());
        assertTrue(failure.getMessage().contains("403"));
    }

    static Map<String, Object> sourceConfig(String endpoint, String bucket, String path) {
        Map<String, Object> config = new HashMap<>();
        config.put("path", path);
        config.put("file_format_type", "json");
        config.put("bucket", "s3a://" + bucket);
        config.put("fs.s3a.endpoint", endpoint);
        config.put(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        config.put("access_key", ACCESS_KEY);
        config.put("secret_key", SECRET_KEY);
        config.put("parse_partition_from_path", false);
        config.put(
                "hadoop_s3_properties",
                Collections.singletonMap("fs.s3a.path.style.access", "true"));
        config.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("event_id", "bigint")));
        return config;
    }

    static void validate(Map<String, Object> config) throws Exception {
        ClassLoader classLoader = S3FileConnectDryRunIT.class.getClassLoader();
        TableSourceFactory factory =
                FactoryUtil.discoverFactory(classLoader, TableSourceFactory.class, "S3File");
        assertTrue(factory instanceof SupportSourceDryRunValidation);
        SupportSourceDryRunValidation validator = (SupportSourceDryRunValidation) factory;
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(ReadonlyConfig.fromMap(config), classLoader);
        validator.validateConnectionForDryRun(context, validator.inferSchemaForDryRun(context));
    }
}
