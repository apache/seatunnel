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

package org.apache.seatunnel.e2e.connector.iceberg.s3;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MinIOContainer;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {EngineType.FLINK, EngineType.SEATUNNEL},
        disabledReason =
                "Needs hadoop-aws,aws-java-sdk jar for flink, spark2.4. For the seatunnel engine, it crashes on seatunnel-hadoop3-3.1.4-uber.jar.")
@Slf4j
public class IcebergSourceIT extends TestSuiteBase implements TestResource {

    public static final String HADOOP_AWS_DOWNLOAD =
            "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar";
    public static final String AWS_SDK_DOWNLOAD =
            "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Iceberg/lib && cd /tmp/seatunnel/plugins/Iceberg/lib && curl -O "
                                        + HADOOP_AWS_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/plugins/Iceberg/lib && curl -O "
                                        + AWS_SDK_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    private static final String MINIO_DOCKER_IMAGE = "minio/minio:RELEASE.2024-06-13T22-53-53Z";
    private static final String HOST = "minio";
    private static final int MINIO_PORT = 9000;

    private static final TableIdentifier TABLE =
            TableIdentifier.of(Namespace.of("database1"), "source");
    private static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.optional(1, "f1", Types.LongType.get()),
                    Types.NestedField.optional(2, "f2", Types.BooleanType.get()),
                    Types.NestedField.optional(3, "f3", Types.IntegerType.get()),
                    Types.NestedField.optional(4, "f4", Types.LongType.get()),
                    Types.NestedField.optional(5, "f5", Types.FloatType.get()),
                    Types.NestedField.optional(6, "f6", Types.DoubleType.get()),
                    Types.NestedField.optional(7, "f7", Types.DateType.get()),
                    Types.NestedField.optional(8, "f8", Types.TimeType.get()),
                    Types.NestedField.optional(9, "f9", Types.TimestampType.withZone()),
                    Types.NestedField.optional(10, "f10", Types.TimestampType.withoutZone()),
                    Types.NestedField.optional(11, "f11", Types.StringType.get()),
                    Types.NestedField.optional(12, "f12", Types.FixedType.ofLength(10)),
                    Types.NestedField.optional(13, "f13", Types.BinaryType.get()),
                    Types.NestedField.optional(14, "f14", Types.DecimalType.of(19, 9)),
                    Types.NestedField.optional(
                            15, "f15", Types.ListType.ofOptional(100, Types.IntegerType.get())),
                    Types.NestedField.optional(
                            16,
                            "f16",
                            Types.MapType.ofOptional(
                                    200, 300, Types.StringType.get(), Types.IntegerType.get())),
                    Types.NestedField.optional(
                            17,
                            "f17",
                            Types.StructType.of(
                                    Types.NestedField.required(
                                            400, "f17_a", Types.StringType.get()))));

    private static final String CATALOG_NAME = "seatunnel";
    private static final IcebergCatalogType CATALOG_TYPE = HADOOP;

    private static String BUCKET = "test-bucket";
    private static String REGION = "us-east-1";

    private static final String CATALOG_DIR = "/tmp/seatunnel/iceberg/s3/";
    private static final String WAREHOUSE = "s3a://" + BUCKET + CATALOG_DIR;
    private static Catalog CATALOG;

    private MinIOContainer container;
    private MinioClient minioClient;
    private Configuration configuration;

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        container =
                new MinIOContainer(MINIO_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(MINIO_PORT);

        container.start();

        String s3URL = container.getS3URL();

        // configuringClient
        minioClient =
                MinioClient.builder()
                        .endpoint(s3URL)
                        .credentials(container.getUserName(), container.getPassword())
                        .region(REGION)
                        .build();

        // create bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).region(REGION).build());

        BucketExistsArgs existsArgs = BucketExistsArgs.builder().bucket(BUCKET).build();
        Assertions.assertTrue(minioClient.bucketExists(existsArgs));

        configuration = initializeConfiguration();

        initializeIcebergTable();
        batchInsertData();
    }

    private Configuration initializeConfiguration() {
        Configuration conf = new Configuration();
        Map<String, String> hadoopProps = getHadoopProps();
        hadoopProps.forEach((key, value) -> conf.set(key, value));
        return conf;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (container != null) {
            container.stop();
        }
    }

    @TestTemplate
    public void testIcebergSource(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/iceberg/iceberg_source.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private void initializeIcebergTable() {
        Map<String, Object> configs = new HashMap<>();

        // add catalog properties
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", CATALOG_TYPE.getType());
        catalogProps.put("warehouse", WAREHOUSE);

        configs.put(CommonConfig.KEY_CATALOG_NAME.key(), CATALOG_NAME);

        configs.put(CommonConfig.CATALOG_PROPS.key(), catalogProps);

        configs.put(CommonConfig.HADOOP_PROPS.key(), getHadoopProps());
        configs.put(CommonConfig.KEY_TABLE.key(), TABLE.toString());

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configs);
        CATALOG = new IcebergCatalogLoader(new SourceConfig(readonlyConfig)).loadCatalog();
        if (!CATALOG.tableExists(TABLE)) {
            CATALOG.createTable(TABLE, SCHEMA);
        }
    }

    private Map<String, String> getHadoopProps() {
        Map<String, String> hadoopProps = new HashMap<>();
        hadoopProps.put("fs.s3a.path.style.access", "true");
        hadoopProps.put("fs.s3a.connection.ssl.enabled", "false");
        hadoopProps.put("fs.s3a.connection.timeout", "3000");
        hadoopProps.put("fs.s3a.impl.disable.cache", "true");
        hadoopProps.put("fs.s3a.attempts.maximum", "1");
        hadoopProps.put(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        hadoopProps.put("fs.s3a.endpoint", container.getS3URL());
        hadoopProps.put("fs.s3a.access.key", container.getUserName());
        hadoopProps.put("fs.s3a.secret.key", container.getPassword());
        hadoopProps.put("fs.defaultFS", "s3a://" + BUCKET);
        return hadoopProps;
    }

    private void batchInsertData() {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("f1", Long.valueOf(0));
        record.setField("f2", true);
        record.setField("f3", Integer.MAX_VALUE);
        record.setField("f4", Long.MAX_VALUE);
        record.setField("f5", Float.MAX_VALUE);
        record.setField("f6", Double.MAX_VALUE);
        record.setField("f7", LocalDate.now());
        record.setField("f8", LocalTime.now());
        record.setField("f9", OffsetDateTime.now());
        record.setField("f10", LocalDateTime.now());
        record.setField("f11", "test");
        record.setField("f12", "abcdefghij".getBytes());
        record.setField("f13", ByteBuffer.wrap("test".getBytes()));
        record.setField("f14", new BigDecimal("1000000000.000000001"));
        record.setField("f15", Arrays.asList(Integer.MAX_VALUE));
        record.setField("f16", Collections.singletonMap("key", Integer.MAX_VALUE));
        Record structRecord = GenericRecord.create(SCHEMA.findField("f17").type().asStructType());
        structRecord.setField("f17_a", "test");
        record.setField("f17", structRecord);

        Table table = CATALOG.loadTable(TABLE);
        FileAppenderFactory appenderFactory = new GenericAppenderFactory(SCHEMA);
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            records.add(record.copy("f1", Long.valueOf(i)));
            if (i % 10 == 0) {
                String externalFilePath =
                        String.format(CATALOG_DIR + "external_file/datafile_%s.avro", i);
                FileAppender<Record> fileAppender =
                        appenderFactory.newAppender(
                                Files.localOutput(externalFilePath),
                                FileFormat.fromFileName(externalFilePath));
                try (FileAppender<Record> fileAppenderCloseable = fileAppender) {
                    fileAppenderCloseable.addAll(records);
                    records.clear();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                uploadObject(externalFilePath);

                HadoopInputFile inputFile =
                        HadoopInputFile.fromLocation(getS3Output(externalFilePath), configuration);
                Assertions.assertTrue(inputFile.exists());

                DataFile datafile =
                        DataFiles.builder(PartitionSpec.unpartitioned())
                                .withInputFile(inputFile)
                                .withMetrics(fileAppender.metrics())
                                .build();
                table.newAppend().appendFile(datafile).commit();
            }
        }
    }

    private String getS3Output(String externalFilePath) {
        return "s3a://" + BUCKET + externalFilePath;
    }

    private void uploadObject(String externalFilePath) {
        try {
            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(BUCKET)
                            .object(externalFilePath)
                            .filename(externalFilePath)
                            .build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
