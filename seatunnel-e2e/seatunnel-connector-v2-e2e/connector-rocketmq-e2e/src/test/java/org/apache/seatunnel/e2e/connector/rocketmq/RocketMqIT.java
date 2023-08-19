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

package org.apache.seatunnel.e2e.connector.rocketmq;

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
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqAdminUtil;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqBaseConfiguration;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.SchemaFormat;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.engine.common.Constant;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.seatunnel.e2e.connector.rocketmq.RocketMqContainer.NAMESRV_PORT;

@Slf4j
public class RocketMqIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "apache/rocketmq:4.9.4";
    private static final String ROCKETMQ_GROUP = "SeaTunnel-rocketmq-group";
    private static final String HOST = "rocketmq-e2e";
    private static final SchemaFormat DEFAULT_FORMAT = SchemaFormat.JSON;
    private static final String DEFAULT_FIELD_DELIMITER = ",";
    private static final SeaTunnelRowType SEATUNNEL_ROW_TYPE =
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
    private RocketMqContainer rocketMqContainer;
    private DefaultMQProducer producer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.rocketMqContainer =
                new RocketMqContainer(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        rocketMqContainer.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", NAMESRV_PORT, NAMESRV_PORT)));
        rocketMqContainer.start();
        log.info("RocketMq container started");
        initProducer();
        log.info("Write 100 records to topic test_topic_source");
        DefaultSeaTunnelRowSerializer serializer =
                new DefaultSeaTunnelRowSerializer(
                        "test_topic_source",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER);
        generateTestData(row -> serializer.serializeRow(row), "test_topic_source", 0, 100);
    }

    @SneakyThrows
    private void initProducer() {
        this.producer = new DefaultMQProducer();
        this.producer.setNamesrvAddr(rocketMqContainer.getNameSrvAddr());
        this.producer.setInstanceName(UUID.randomUUID().toString());
        this.producer.setProducerGroup(ROCKETMQ_GROUP);
        this.producer.setLanguage(LanguageCode.JAVA);
        this.producer.setSendMsgTimeout(15000);
        this.producer.start();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (this.producer != null) {
            this.producer.shutdown();
        }
        if (this.rocketMqContainer != null) {
            this.rocketMqContainer.close();
        }
    }

    @TestTemplate
    public void testSinkRocketMq(TestContainer container) throws IOException, InterruptedException {

        Container.ExecResult execResult =
                container.executeJob("/rocketmq-sink_fake_to_rocketmq.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String topicName = "test_topic";
        Map<String, String> data = getRocketMqConsumerData(topicName);
        ObjectMapper objectMapper = new ObjectMapper();
        String key = data.keySet().iterator().next();
        ObjectNode objectNode = objectMapper.readValue(key, ObjectNode.class);
        Assertions.assertTrue(objectNode.has("c_map"));
        Assertions.assertTrue(objectNode.has("c_string"));
        Assertions.assertEquals(10, data.size());
    }

    @TestTemplate
    public void testTextFormatSinkRocketMq(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/rocketmq-text-sink_fake_to_rocketmq.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        String topicName = "test_text_topic";
        Map<String, String> data = getRocketMqConsumerData(topicName);
        Assertions.assertEquals(10, data.size());
    }

    @TestTemplate
    public void testSourceRocketMqTextToConsole(TestContainer container)
            throws IOException, InterruptedException {
        DefaultSeaTunnelRowSerializer serializer =
                new DefaultSeaTunnelRowSerializer(
                        "test_topic_text",
                        SEATUNNEL_ROW_TYPE,
                        SchemaFormat.TEXT,
                        DEFAULT_FIELD_DELIMITER);
        generateTestData(row -> serializer.serializeRow(row), "test_topic_text", 0, 100);
        Container.ExecResult execResult =
                container.executeJob("/rocketmq-source_text_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceRocketMqJsonToConsole(TestContainer container)
            throws IOException, InterruptedException {
        DefaultSeaTunnelRowSerializer serializer =
                new DefaultSeaTunnelRowSerializer(
                        "test_topic_json",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER);
        generateTestData(row -> serializer.serializeRow(row), "test_topic_json", 0, 100);
        Container.ExecResult execResult =
                container.executeJob("/rocketmq-source_json_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testRocketMqLatestToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/rocketmq/rocketmq_source_latest_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testRocketMqEarliestToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/rocketmq/rocketmq_source_earliest_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testRocketMqSpecificOffsetsToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/rocketmq/rocketmq_source_specific_offsets_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testRocketMqTimestampToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/rocketmq/rocketmq_source_timestamp_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceRocketMqStartConfig(TestContainer container)
            throws IOException, InterruptedException {
        DefaultSeaTunnelRowSerializer serializer =
                new DefaultSeaTunnelRowSerializer(
                        "test_topic_group",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER);
        generateTestData(row -> serializer.serializeRow(row), "test_topic_group", 100, 150);
        testRocketMqGroupOffsetsToConsole(container);
    }

    public void testRocketMqGroupOffsetsToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/rocketmq/rocketmq_source_group_offset_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @SneakyThrows
    private void generateTestData(
            ProducerRecordConverter converter, String topic, int start, int end) {
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
                                LocalDate.now(),
                                LocalDateTime.now()
                            });
            Message message = converter.convert(row);
            producer.send(message, new MessageQueue(topic, RocketMqContainer.BROKER_NAME, 0));
        }
    }

    private Map<String, String> getRocketMqConsumerData(String topicName) {
        Map<String, String> data = new HashMap<>();
        try {
            DefaultLitePullConsumer consumer =
                    RocketMqAdminUtil.initDefaultLitePullConsumer(newConfiguration(), false);
            consumer.start();
            // assign
            Map<MessageQueue, TopicOffset> queueOffsets =
                    RetryUtils.retryWithException(
                            () -> {
                                return RocketMqAdminUtil.offsetTopics(
                                                newConfiguration(), Lists.newArrayList(topicName))
                                        .get(0);
                            },
                            new RetryUtils.RetryMaterial(
                                    Constant.OPERATION_RETRY_TIME,
                                    false,
                                    exception -> exception instanceof RocketMqConnectorException,
                                    Constant.OPERATION_RETRY_SLEEP));
            consumer.assign(queueOffsets.keySet());
            // seek to offset
            Map<MessageQueue, Long> currentOffsets =
                    RocketMqAdminUtil.currentOffsets(
                            newConfiguration(),
                            Lists.newArrayList(topicName),
                            queueOffsets.keySet());
            for (MessageQueue mq : queueOffsets.keySet()) {
                long currentOffset =
                        currentOffsets.containsKey(mq)
                                ? currentOffsets.get(mq)
                                : queueOffsets.get(mq).getMinOffset();
                consumer.seek(mq, currentOffset);
            }
            while (true) {
                List<MessageExt> messages = consumer.poll(5000);
                if (messages.isEmpty()) {
                    break;
                }
                for (MessageExt message : messages) {
                    data.put(
                            message.getKeys(),
                            new String(message.getBody(), StandardCharsets.UTF_8));
                    consumer.getOffsetStore()
                            .updateConsumeOffsetToBroker(
                                    new MessageQueue(
                                            message.getTopic(),
                                            message.getBrokerName(),
                                            message.getQueueId()),
                                    message.getQueueOffset(),
                                    false);
                }
                consumer.commitSync();
            }
            if (consumer != null) {
                consumer.shutdown();
            }
            log.info("Consumer {} data total {}", topicName, data.size());
            // consumer.commitSync() only submits the offset to the broker, and NameServer scans the
            // broker to update the offset every 10 seconds
            Thread.sleep(20 * 1000);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return data;
    }

    public RocketMqBaseConfiguration newConfiguration() {
        return RocketMqBaseConfiguration.newBuilder()
                .groupId(ROCKETMQ_GROUP)
                .aclEnable(false)
                .namesrvAddr(rocketMqContainer.getNameSrvAddr())
                .batchSize(10)
                .build();
    }

    interface ProducerRecordConverter {
        Message convert(SeaTunnelRow row);
    }
}
