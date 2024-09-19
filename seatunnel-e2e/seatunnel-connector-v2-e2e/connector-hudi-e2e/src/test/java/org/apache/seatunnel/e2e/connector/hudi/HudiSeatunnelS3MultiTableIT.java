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
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.engine.e2e.SeaTunnelContainer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MinIOContainer;

import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@Slf4j
public class HudiSeatunnelS3MultiTableIT extends SeaTunnelContainer {

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

    protected static final String AWS_SDK_DOWNLOAD =
            "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar";
    protected static final String HADOOP_AWS_DOWNLOAD =
            "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar";

    @Override
    @BeforeAll
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

        // configuringClient
        minioClient =
                MinioClient.builder()
                        .endpoint(s3URL)
                        .credentials(container.getUserName(), container.getPassword())
                        .build();

        // create bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());

        BucketExistsArgs existsArgs = BucketExistsArgs.builder().bucket(BUCKET).build();
        Assertions.assertTrue(minioClient.bucketExists(existsArgs));
        super.startUp();
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

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
        if (container != null) {
            container.close();
        }
    }

    @Test
    public void testS3MultiWrite() throws IOException, InterruptedException {
        copyFileToContainer("/core-site.xml", "/tmp/seatunnel/config/core-site.xml");
        Container.ExecResult textWriteResult = executeSeaTunnelJob("/s3_fake_to_hudi.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", LocalFileSystem.DEFAULT_FS);
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy hudi to local
                            Path inputPath1 =
                                    downloadNewestCommitFile(DATABASE_1 + "/" + TABLE_NAME_1 + "/");
                            Path inputPath2 =
                                    downloadNewestCommitFile(DATABASE_2 + "/" + TABLE_NAME_2 + "/");
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
                            FileUtils.deleteFile(inputPath1.toUri().getPath());
                            FileUtils.deleteFile(inputPath2.toUri().getPath());
                            Assertions.assertEquals(100, rowCount1);
                            Assertions.assertEquals(240, rowCount2);
                        });
    }

    public Path downloadNewestCommitFile(String pathPrefix) throws IOException {
        Iterable<Result<Item>> listObjects =
                minioClient.listObjects(
                        ListObjectsArgs.builder().bucket(BUCKET).prefix(pathPrefix).build());
        String newestCommitFileabsolutePath = "";
        String newestCommitFileName = "";
        long newestCommitTime = 0L;
        for (Result<Item> listObject : listObjects) {
            Item item;
            try {
                item = listObject.get();
            } catch (Exception e) {
                throw new IOException("List minio file error.", e);
            }
            if (item.isDir() || !item.objectName().endsWith(".parquet")) {
                continue;
            }
            long fileCommitTime =
                    Long.parseLong(
                            item.objectName()
                                    .substring(
                                            item.objectName().lastIndexOf("_") + 1,
                                            item.objectName().lastIndexOf(".parquet")));
            if (fileCommitTime > newestCommitTime) {
                newestCommitFileabsolutePath = item.objectName();
                newestCommitFileName =
                        newestCommitFileabsolutePath.substring(
                                item.objectName().lastIndexOf("/") + 1);
                newestCommitTime = fileCommitTime;
            }
        }
        try {
            minioClient.downloadObject(
                    DownloadObjectArgs.builder()
                            .bucket(BUCKET)
                            .object(newestCommitFileabsolutePath)
                            .filename(DOWNLOAD_PATH + newestCommitFileName)
                            .build());
        } catch (Exception e) {
            log.error("Download file from minio error.");
        }
        return new Path(DOWNLOAD_PATH + newestCommitFileName);
    }
}
