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

package org.apache.seatunnel.e2e.connector.kafka;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently E2E only supports Seatunnel engine")
@Slf4j
public class KafkaKerberosIT extends TestSuiteBase implements TestResource {

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";
    private static final String KERBEROS_IMAGE_NAME = "zhangshenghang/kerberos-server:1.0";

    // The hostname is uniformly set to lowercase letters to prevent errors during Kerberos
    // authentication
    private static final String KAFKA_HOST = "kafkacluster";
    private static final String BOOTSTRAP_SERVERS = KAFKA_HOST + ":9092";

    private KafkaProducer<byte[], byte[]> producer;

    private GenericContainer<?> kafkaContainer;
    private GenericContainer<?> kerberosContainer;

    private final SeaTunnelRowType SEATUNNEL_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {
                        "id",
                        "c_map",
                        "c_array",
                        "c_string",
                        "c_boolean",
                        "c_tinyint",
                        "c_smallint",
                        "c_int",
                        "c_bigint",
                        "c_float",
                        "c_double",
                        "c_decimal",
                        "c_bytes",
                        "c_date",
                        "c_timestamp"
                    },
                    new SeaTunnelDataType[] {
                        BasicType.LONG_TYPE,
                        new MapType(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
                        ArrayType.BYTE_ARRAY_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.BOOLEAN_TYPE,
                        BasicType.BYTE_TYPE,
                        BasicType.SHORT_TYPE,
                        BasicType.INT_TYPE,
                        BasicType.LONG_TYPE,
                        BasicType.FLOAT_TYPE,
                        BasicType.DOUBLE_TYPE,
                        new DecimalType(2, 1),
                        PrimitiveByteArrayType.INSTANCE,
                        LocalTimeType.LOCAL_DATE_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE
                    });

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        System.setProperty(
                "java.security.krb5.conf",
                ContainerUtil.getResourcesFile("/kerberos/krb5_local.conf").getPath());
        System.setProperty(
                "java.security.auth.login.config",
                ContainerUtil.getResourcesFile("/kerberos/kafka_server_jaas.conf").getPath());

        kerberosContainer =
                new GenericContainer<>(KERBEROS_IMAGE_NAME)
                        .withNetwork(NETWORK)
                        .withExposedPorts(88, 749)
                        .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kerberos"))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KERBEROS_IMAGE_NAME)));
        kerberosContainer.setPortBindings(Arrays.asList("88/udp:88/udp", "749:749"));
        Startables.deepStart(Stream.of(kerberosContainer)).join();
        log.info("Kerberos just started");

        kerberosContainer.execInContainer(
                "bash",
                "-c",
                "kadmin.local -q \"addprinc -randkey kafka/kafkacluster@EXAMPLE.COM\"");
        kerberosContainer.execInContainer(
                "bash",
                "-c",
                "kadmin.local -q \"xst -k /tmp/kafka.keytab kafka/kafkacluster@EXAMPLE.COM\"");

        // test.keytab verify unprivileged keytab usage
        kerberosContainer.execInContainer(
                "bash",
                "-c",
                "kadmin.local -q \"addprinc -randkey test/kafkacluster@EXAMPLE.COM\"");
        kerberosContainer.execInContainer(
                "bash",
                "-c",
                "kadmin.local -q \"xst -k /tmp/test.keytab test/kafkacluster@EXAMPLE.COM\"");

        given().ignoreExceptions()
                .await()
                .atMost(30, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1L))
                .untilAsserted(
                        () -> {
                            kerberosContainer.copyFileFromContainer(
                                    "/tmp/kafka.keytab", "/tmp/kafka.keytab");
                            kerberosContainer.copyFileFromContainer(
                                    "/tmp/test.keytab", "/tmp/test.keytab");
                        });

        kafkaContainer =
                new GenericContainer<>(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withFileSystemBind(
                                ContainerUtil.getResourcesFile("/kerberos/kafka_server_jaas.conf")
                                        .getPath(),
                                "/etc/kafka/kafka_server_jaas.conf")
                        .withFileSystemBind(
                                ContainerUtil.getResourcesFile("/kerberos/krb5.conf").getPath(),
                                "/etc/krb5.conf")
                        .withExposedPorts(9092, 2181)
                        .withFileSystemBind(
                                ContainerUtil.getResourcesFile("/kerberos/kafka.properties")
                                        .getPath(),
                                "/etc/kafka/kafka.properties")
                        .withFileSystemBind("/tmp/kafka.keytab", "/tmp/kafka.keytab")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)))
                        .withCommand(
                                "bash",
                                "-c",
                                FileUtils.readFileToStr(
                                        ContainerUtil.getResourcesFile("/kerberos/start.sh")
                                                .toPath()));
        kafkaContainer.setPortBindings(Arrays.asList("9092:9092", "2181:2181"));
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        log.info("Kafka container started");

        // Add Hosts, local connection kerberos kafka use
        appendToHosts("127.0.0.1", "kafkacluster");

        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initKafkaProducer);
    }

    private void initKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");
        producer = new KafkaProducer<>(props);
    }

    private Properties kafkaConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "seatunnel-kafka-sink-group");
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");
        return props;
    }

    private static void appendToHosts(String ip, String hostname) {
        try {
            String entry = String.format("%s %s", ip, hostname);
            ProcessBuilder processBuilder =
                    new ProcessBuilder("sudo", "sh", "-c", "echo '" + entry + "' >> /etc/hosts");
            processBuilder.redirectErrorStream(true);

            Process process = processBuilder.start();

            int exitCode = process.waitFor();
            if (exitCode == 0) {
                log.info("Successfully added to /etc/hosts: {}", entry);
            } else {
                log.error("Failed to add to /etc/hosts: {}", entry);
            }
        } catch (Exception e) {
            log.error("Failed to add to /etc/hosts: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    public void testKerberosWithoutPermission(TestContainer container)
            throws IOException, InterruptedException {
        container.copyFileToContainer("/kerberos/krb5.conf", "/etc/krb5.conf");
        container.copyAbsolutePathToContainer("/tmp/test.keytab", "/tmp/kafka.keytab");

        Container.ExecResult execResult =
                container.executeJob("/kerberos/kafka_sink_fake_to_kafka_kerberos.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
        Assertions.assertTrue(
                execResult
                        .getStderr()
                        .contains(
                                "Could not login: the client is being asked for a password, but the Kafka client code does not currently support obtaining a password from the user."));
    }

    @TestTemplate
    public void testNotKerberosConfig(TestContainer container)
            throws IOException, InterruptedException {
        String jobId = "123456";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/kerberos/kafka_sink_with_not_kerberos.conf", jobId);
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
        // step 1. Verify whether Kafka has authentication failure logs
        Awaitility.given()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        kafkaContainer
                                                .execInContainer(
                                                        "bash",
                                                        "-c",
                                                        "tail /var/log/kafka/server.log")
                                                .getStdout()
                                                .matches(
                                                        "(?s).*Failed authentication with /.*? \\(Unexpected Kafka request of type METADATA during SASL handshake.*")));

        container.cancelJob(jobId);

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            String jobStatus = container.getJobStatus(String.valueOf(jobId));
                            Assertions.assertEquals("CANCELED", jobStatus);
                        });

        // step 2. Verify that the program outputs retry logs
        Awaitility.given()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        container
                                                .getServerLogs()
                                                .contains(
                                                        "Cancelled in-flight INIT_PRODUCER_ID request with correlation id")));
    }

    @TestTemplate
    public void testSinkKafkaWithKerberos(TestContainer container)
            throws IOException, InterruptedException {
        container.copyFileToContainer("/kerberos/krb5.conf", "/etc/krb5.conf");
        container.copyAbsolutePathToContainer("/tmp/kafka.keytab", "/tmp/kafka.keytab");

        Container.ExecResult execResult =
                container.executeJob("/kerberos/kafka_sink_fake_to_kafka_kerberos.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String topicName = "test_topic";
        Map<String, String> data = getKafkaConsumerData(topicName);
        ObjectMapper objectMapper = new ObjectMapper();
        String key = data.keySet().iterator().next();
        ObjectNode objectNode = objectMapper.readValue(key, ObjectNode.class);
        Assertions.assertTrue(objectNode.has("c_map"));
        Assertions.assertTrue(objectNode.has("c_string"));
        Assertions.assertEquals(10, data.size());
    }

    @TestTemplate
    public void testSourceKafkaWithKerberos(TestContainer container)
            throws IOException, InterruptedException {
        container.copyFileToContainer("/kerberos/krb5.conf", "/etc/krb5.conf");
        container.copyAbsolutePathToContainer("/tmp/kafka.keytab", "/tmp/kafka.keytab");

        TextSerializationSchema serializer =
                TextSerializationSchema.builder()
                        .seaTunnelRowType(SEATUNNEL_ROW_TYPE)
                        .delimiter(",")
                        .build();
        generateTestData(
                row ->
                        new ProducerRecord<>(
                                "test_topic_with_kerberos", null, serializer.serialize(row)),
                0,
                100);
        Container.ExecResult execResult =
                container.executeJob("/kerberos/kafka_source_to_assert_with_kerberos.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private void generateTestData(KafkaIT.ProducerRecordConverter converter, int start, int end) {
        try {
            for (int i = start; i < end; i++) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    Long.valueOf(i),
                                    Collections.singletonMap("key", Short.parseShort("1")),
                                    new Byte[] {Byte.parseByte("1")},
                                    "string",
                                    Boolean.FALSE,
                                    Byte.parseByte("1"),
                                    Short.parseShort("1"),
                                    Integer.parseInt("1"),
                                    Long.parseLong("1"),
                                    Float.parseFloat("1.1"),
                                    Double.parseDouble("1.1"),
                                    BigDecimal.valueOf(11, 1),
                                    "test".getBytes(),
                                    LocalDate.of(2024, 1, 1),
                                    LocalDateTime.of(2024, 1, 1, 12, 59, 23)
                                });
                ProducerRecord<byte[], byte[]> producerRecord = converter.convert(row);
                producer.send(producerRecord).get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        producer.flush();
    }

    private Map<String, String> getKafkaConsumerData(String topicName) {
        Map<String, String> data = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
            consumer.subscribe(Arrays.asList(topicName));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(topicName, 0)));
            Long endOffset = offsets.entrySet().iterator().next().getValue();
            Long lastProcessedOffset = -1L;

            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        data.put(record.key(), record.value());
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
        return data;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }
}
