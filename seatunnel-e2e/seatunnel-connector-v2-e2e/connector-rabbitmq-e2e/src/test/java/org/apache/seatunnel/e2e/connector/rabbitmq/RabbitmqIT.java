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

package org.apache.seatunnel.e2e.connector.rabbitmq;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

@Slf4j
public class RabbitmqIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "rabbitmq:3-management";
    // For the single-sink test we used QUEUE_NAME and SINK_QUEUE_NAME;
    // For multi-sink, we assume two target queues:
    private static final String SOURCE_QUEUE_NAME = "test";
    private static final String SINK_QUEUE_NAME_1 = "rabbitmq_sink_1";
    private static final String SINK_QUEUE_NAME_2 = "rabbitmq_sink_2";
    private static final String HOST = "rabbitmq-e2e";
    private static final int PORT = 5672;
    private static final String USERNAME = "guest";
    private static final String PASSWORD = "guest";
    private static final Boolean DURABLE = true;
    private static final Boolean EXCLUSIVE = false;
    private static final Boolean AUTO_DELETE = false;

    // Test dataset used for serialization (for single-sink test)
    private static final Pair<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET =
            generateTestDataSet();
    private static final JsonSerializationSchema JSON_SERIALIZATION_SCHEMA =
            new JsonSerializationSchema(TEST_DATASET.getKey());

    private GenericContainer<?> rabbitmqContainer;
    private Connection connection;
    private RabbitmqClient rabbitmqClient;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.rabbitmqContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(PORT, 15672)
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        Startables.deepStart(Stream.of(rabbitmqContainer)).join();
        log.info("RabbitMQ container started");
        this.initRabbitMQ();
    }

    /**
     * In the single-sink test, we write data to the source queue. For the multi-sink test, your
     * configuration may use a FakeSource with multiple table configs. Here we provide the same
     * source initialization so that both tests have data.
     */
    private void initSourceData() throws IOException, InterruptedException {
        List<SeaTunnelRow> rows = TEST_DATASET.getValue();
        // Write one of the rows to the source queue (this is just for demonstration)
        for (int i = 0; i < rows.size(); i++) {
            rabbitmqClient.write(
                    new String(JSON_SERIALIZATION_SCHEMA.serialize(rows.get(1)))
                            .getBytes(StandardCharsets.UTF_8));
        }
    }

    private static Pair<SeaTunnelRowType, List<SeaTunnelRow>> generateTestDataSet() {

        SeaTunnelRowType rowType =
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

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                Long.valueOf(1),
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
            rows.add(row);
        }
        return Pair.of(rowType, rows);
    }

    private void initRabbitMQ() {
        try {
            RabbitmqConfig config = new RabbitmqConfig();
            config.setHost(rabbitmqContainer.getHost());
            config.setPort(rabbitmqContainer.getFirstMappedPort());
            config.setQueueName(SOURCE_QUEUE_NAME);
            config.setVirtualHost("/");
            config.setUsername(USERNAME);
            config.setPassword(PASSWORD);
            config.setDurable(DURABLE);
            config.setExclusive(EXCLUSIVE);
            config.setAutoDelete(AUTO_DELETE);
            rabbitmqClient = new RabbitmqClient(config);
        } catch (Exception e) {
            throw new RuntimeException("init Rabbitmq error", e);
        }
    }

    /** Initializes a sink RabbitMQ client for the specified queue. */
    private RabbitmqClient initSinkRabbitMQ(String queueName) {
        try {
            RabbitmqConfig config = new RabbitmqConfig();
            config.setHost(rabbitmqContainer.getHost());
            config.setPort(rabbitmqContainer.getFirstMappedPort());
            config.setQueueName(queueName);
            config.setVirtualHost("/");
            config.setUsername(USERNAME);
            config.setPassword(PASSWORD);
            config.setDurable(DURABLE);
            config.setExclusive(EXCLUSIVE);
            config.setAutoDelete(AUTO_DELETE);
            return new RabbitmqClient(config);
        } catch (Exception e) {
            throw new RuntimeException("init Rabbitmq error", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        rabbitmqContainer.close();
    }

    /** Existing single-sink test. */
    @TestTemplate
    public void testRabbitMQ(TestContainer container) throws Exception {
        // send data to source queue before executeJob start in every testContainer
        initSourceData();

        // Initialize consumer client for the single sink queue.
        RabbitmqClient sinkRabbitmqClient = initSinkRabbitMQ(SINK_QUEUE_NAME_1);
        Set<String> resultSet = new HashSet<>();
        Handover handover = new Handover<>();
        DefaultConsumer consumer = sinkRabbitmqClient.getQueueingConsumer(handover);
        sinkRabbitmqClient.getChannel().basicConsume(SINK_QUEUE_NAME_1, true, consumer);

        // Execute the job using the single-sink configuration file.
        Container.ExecResult execResult = container.executeJob("/rabbitmq-to-rabbitmq.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // Poll for messages.
        for (int i = 0; i < 5; i++) {
            Optional<Delivery> deliveryOptional = handover.pollNext();
            if (deliveryOptional.isPresent()) {
                Delivery delivery = deliveryOptional.get();
                byte[] body = delivery.getBody();
                resultSet.add(new String(body));
            }
        }
        sinkRabbitmqClient.close();

        // Assert that data was written.
        Assertions.assertFalse(resultSet.isEmpty());
    }

    /**
     * New multi-sink test. This test executes a job that writes to multiple RabbitMQ queues (one
     * per table). It then consumes messages from each sink queue (e.g. "rabbitmq_sink_1" and
     * "rabbitmq_sink_2") and asserts that each received expected data.
     */
    @TestTemplate
    public void testRabbitMQMultiSink(TestContainer container) throws Exception {
        // Initialize sink clients for both sink queues.
        RabbitmqClient sinkClient1 = initSinkRabbitMQ(SINK_QUEUE_NAME_1);
        RabbitmqClient sinkClient2 = initSinkRabbitMQ(SINK_QUEUE_NAME_2);

        Handover handover1 = new Handover<>();
        Handover handover2 = new Handover<>();
        DefaultConsumer consumer1 = sinkClient1.getQueueingConsumer(handover1);
        DefaultConsumer consumer2 = sinkClient2.getQueueingConsumer(handover2);
        sinkClient1.getChannel().basicConsume(SINK_QUEUE_NAME_1, true, consumer1);
        sinkClient2.getChannel().basicConsume(SINK_QUEUE_NAME_2, true, consumer2);

        // Execute the job with the multi-sink configuration file.
        Container.ExecResult execResult = container.executeJob("/rabbitmq-to-rabbitmq-multi.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // Poll messages from both queues.
        Set<String> resultSet1 = new HashSet<>();
        Set<String> resultSet2 = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Optional<Object> deliveryOpt1 = handover1.pollNext();
            deliveryOpt1.ifPresent(
                    obj -> {
                        Delivery delivery = (Delivery) obj;
                        resultSet1.add(new String(delivery.getBody(), StandardCharsets.UTF_8));
                    });
            Optional<Object> deliveryOpt2 = handover2.pollNext();
            deliveryOpt2.ifPresent(
                    obj -> {
                        Delivery delivery = (Delivery) obj;
                        resultSet2.add(new String(delivery.getBody(), StandardCharsets.UTF_8));
                    });
        }

        sinkClient1.close();
        sinkClient2.close();

        // Assert that data is received from both sink queues.
        Assertions.assertFalse(
                resultSet1.isEmpty(), "Queue " + SINK_QUEUE_NAME_1 + " should not be empty");
        Assertions.assertFalse(
                resultSet2.isEmpty(), "Queue " + SINK_QUEUE_NAME_2 + " should not be empty");
    }
}
