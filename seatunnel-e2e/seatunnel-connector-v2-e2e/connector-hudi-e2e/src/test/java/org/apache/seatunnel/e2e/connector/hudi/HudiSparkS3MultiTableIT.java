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

package org.apache.seatunnel.e2e.connector.hudi;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MinIOContainer;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@Slf4j
public class HudiSparkS3MultiTableIT extends TestSuiteBase implements TestResource {

    private static final String MINIO_DOCKER_IMAGE = "minio/minio:RELEASE.2024-06-13T22-53-53Z";
    private static final String HOST = "minio";
    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER_NAME = "minio";
    private static final String MINIO_USER_PASSWORD = "miniominio";
    private static final String BUCKET = "hudi";

    private MinIOContainer container;
    private MinioClient minioClient;

    private static final String DATABASE_1 = "st1";
    private static final String TABLE_NAME_1 = "st_test_1";
    private static final String DATABASE_2 = "default";
    private static final String TABLE_NAME_2 = "st_test_2";
    private static final String DOWNLOAD_PATH = "/tmp/seatunnel/";

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        container =
                new MinIOContainer(MINIO_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withUserName(MINIO_USER_NAME)
                        .withPassword(MINIO_USER_PASSWORD)
                        .withExposedPorts(MINIO_PORT);
        container.start();

        String s3URL = container.getS3URL();

        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.connectTimeout(10, TimeUnit.SECONDS);
        builder.readTimeout(10, TimeUnit.SECONDS);
        // configuringClient
        minioClient =
                MinioClient.builder()
                        .endpoint(s3URL)
                        .credentials(container.getUserName(), container.getPassword())
                        .httpClient(builder.build())
                        .build();

        // create bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());

        BucketExistsArgs existsArgs = BucketExistsArgs.builder().bucket(BUCKET).build();
        Assertions.assertTrue(minioClient.bucketExists(existsArgs));
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (container != null) {
            container.close();
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {TestContainerId.SPARK_2_4},
            type = {EngineType.FLINK, EngineType.SEATUNNEL},
            disabledReason =
                    "The hadoop version in current flink image is not compatible with the aws version and default container of seatunnel not support s3.")
    public void testS3MultiWrite(TestContainer container) throws IOException, InterruptedException {
        container.copyFileToContainer("/hudi/core-site.xml", "/tmp/seatunnel/config/core-site.xml");
        Container.ExecResult textWriteResult = container.executeJob("/hudi/s3_fake_to_hudi.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", LocalFileSystem.DEFAULT_FS);
        given().ignoreExceptions()
                .pollDelay(5, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .await()
                .atMost(300, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy hudi to local
                            // copy hudi to local
                            Path inputPath1 = null;
                            Path inputPath2 = null;
                            try {
                                inputPath1 =
                                        new Path(
                                                MinIoUtils.downloadNewestCommitFile(
                                                        minioClient,
                                                        BUCKET,
                                                        String.format(
                                                                "%s/%s/", DATABASE_1, TABLE_NAME_1),
                                                        DOWNLOAD_PATH));
                                log.info(
                                        "download from s3 success, the parquet file is at: {}",
                                        inputPath1);
                                inputPath2 =
                                        new Path(
                                                MinIoUtils.downloadNewestCommitFile(
                                                        minioClient,
                                                        BUCKET,
                                                        String.format(
                                                                "%s/%s/", DATABASE_2, TABLE_NAME_2),
                                                        DOWNLOAD_PATH));
                                log.info(
                                        "download from s3 success, the parquet file is at: {}",
                                        inputPath2);
                                ParquetReader<Group> reader1 =
                                        ParquetReader.builder(new GroupReadSupport(), inputPath1)
                                                .withConf(configuration)
                                                .build();
                                ParquetReader<Group> reader2 =
                                        ParquetReader.builder(new GroupReadSupport(), inputPath2)
                                                .withConf(configuration)
                                                .build();

                                long rowCount1 = 0;
                                long rowCount2 = 0;
                                // Read data and count rows
                                while (reader1.read() != null) {
                                    rowCount1++;
                                }
                                // Read data and count rows
                                while (reader2.read() != null) {
                                    rowCount2++;
                                }
                                Assertions.assertEquals(100, rowCount1);
                                Assertions.assertEquals(240, rowCount2);
                            } finally {
                                if (inputPath1 != null) {
                                    FileUtils.deleteFile(inputPath1.toUri().getPath());
                                }
                                if (inputPath2 != null) {
                                    FileUtils.deleteFile(inputPath2.toUri().getPath());
                                }
                            }
                        });
    }
}
