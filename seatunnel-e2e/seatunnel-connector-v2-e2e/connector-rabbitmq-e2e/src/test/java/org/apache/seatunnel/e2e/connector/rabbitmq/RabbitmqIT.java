/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.DeliveryMessage;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class RabbitmqIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "rabbitmq:3-management";
    private static final String HOST = "rabbitmq-e2e";
    private static final int PORT = 5672;
    private static final String USERNAME = "guest";
    private static final String PASSWORD = "guest";
    private static final Boolean DURABLE = true;
    private static final Boolean EXCLUSIVE = false;
    private static final Boolean AUTO_DELETE = false;

    private static final Pair<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET =
            generateTestDataSet();
    private static final JsonSerializationSchema JSON_SERIALIZATION_SCHEMA =
            new JsonSerializationSchema(TEST_DATASET.getKey());

    private GenericContainer<?> rabbitmqContainer;
    Connection connection;

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
        log.info("rabbitmq container started");
    }

    private void initSourceData(RabbitmqClient rabbitmqClient)
            throws IOException, InterruptedException {
        List<SeaTunnelRow> rows = TEST_DATASET.getValue();
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

    private RabbitmqClient getRabbitmqClient(String queueName) {
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

    @TestTemplate
    public void testRabbitMQ(TestContainer container) throws Exception {
        final String sourceQueueName = "test";
        final String sinkQueueName = "test1";

        RabbitmqClient sourceClient = this.getRabbitmqClient(sourceQueueName);
        // Explicitly declare source queue before writing to avoid message drop
        sourceClient
                .getChannel()
                .queueDeclare(sourceQueueName, DURABLE, EXCLUSIVE, AUTO_DELETE, null);

        // send data to source queue before executeJob start in every testContainer
        initSourceData(sourceClient);
        Thread.sleep(3000);

        // init consumer client before executeJob start in every testContainer
        RabbitmqClient sinkRabbitmqClient = getRabbitmqClient(sinkQueueName);

        // Explicitly declare sink queue before trying to consume from it,
        // to avoid 404 NOT_FOUND channel errors.
        sinkRabbitmqClient
                .getChannel()
                .queueDeclare(sinkQueueName, DURABLE, EXCLUSIVE, AUTO_DELETE, null);

        // Use BlockingQueue instead of Handover to match the new optimized connector architecture
        BlockingQueue<DeliveryMessage> queue = new LinkedBlockingQueue<>();
        DefaultConsumer consumer = sinkRabbitmqClient.getQueueingConsumer(queue, sinkQueueName);

        // Start consuming BEFORE executing the job.
        // This ensures the test consumer is ready to catch messages even if TestContainer finishes
        // instantly.
        sinkRabbitmqClient.getChannel().basicConsume(sinkQueueName, true, consumer);

        // assert execute Job code
        Container.ExecResult execResult = container.executeJob("/rabbitmq-to-rabbitmq.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        HashSet<Object> resultSet = new HashSet<>();
        // consume data when every testContainer finished
        // try to poll five times
        for (int i = 0; i < 10; i++) {
            DeliveryMessage msg = queue.poll(15, TimeUnit.SECONDS);
            if (msg != null && msg.getDelivery() != null) {
                byte[] body = msg.getDelivery().getBody();
                String content = new String(body);
                resultSet.add(content);
            }
        }
        // close to prevent rabbitmq client consumer in the next TestContainer to consume
        sinkRabbitmqClient.close();
        sourceClient.close();

        // assert source and sink data
        Assertions.assertTrue(resultSet.size() > 0);
        // Verify against the test dataset
        Assertions.assertTrue(
                resultSet.contains(
                        new String(
                                JSON_SERIALIZATION_SCHEMA.serialize(
                                        TEST_DATASET.getValue().get(1)))));
    }

    @TestTemplate
    public void testRabbitMQUSingDefaultConfig(TestContainer container) throws Exception {
        final String sourceQueueName = "test2_0";
        final String sinkQueueName = "test2_1";

        RabbitmqClient sourceClient = this.getRabbitmqClient(sourceQueueName);
        // Explicitly declare source queue
        sourceClient
                .getChannel()
                .queueDeclare(sourceQueueName, DURABLE, EXCLUSIVE, AUTO_DELETE, null);

        // send data to source queue before executeJob start in every testContainer
        initSourceData(sourceClient);

        // init consumer client before executeJob start in every testContainer
        RabbitmqClient sinkRabbitmqClient = getRabbitmqClient(sinkQueueName);

        // Explicitly declare sink queue BEFORE trying to consume to prevent 404 error
        sinkRabbitmqClient
                .getChannel()
                .queueDeclare(sinkQueueName, DURABLE, EXCLUSIVE, AUTO_DELETE, null);

        BlockingQueue<DeliveryMessage> queue = new LinkedBlockingQueue<>();
        DefaultConsumer consumer = sinkRabbitmqClient.getQueueingConsumer(queue, sinkQueueName);

        // Pre-start consumption to prevent message loss in fast-finishing Batch jobs
        sinkRabbitmqClient.getChannel().basicConsume(sinkQueueName, true, consumer);

        Container.ExecResult execResult =
                container.executeJob("/rabbitmq-to-rabbitmq-using-default-config.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        sinkRabbitmqClient.close();
        sourceClient.close();
    }

    /**
     * End-to-end test for the Multi-Table feature of the RabbitMQ Source Connector. * This test
     * verifies that the connector can simultaneously read from multiple RabbitMQ queues, apply
     * different schemas to the incoming JSON messages based on the queue they originated from, and
     * correctly route them as distinct tables to the downstream sinks.
     */
    @TestTemplate
    public void testRabbitMQMultiTableE2E(TestContainer container) throws Exception {
        // Define distinct schemas for two different tables/queues.
        // This ensures we test the connector's ability to handle heterogeneous data streams.
        SeaTunnelRowType type1 =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});
        SeaTunnelRowType type2 =
                new SeaTunnelRowType(
                        new String[] {"id", "age"},
                        new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.INT_TYPE});

        String queue1 = "multi_table_1";
        String queue2 = "multi_table_2";

        // Pre-populate the RabbitMQ broker with synthetic test data.
        // We send 10 records to each unique RabbitMQ queue using their respective schemas.
        sendData(queue1, type1, 10);
        sendData(queue2, type2, 10);

        // Wait briefly to ensure all messages are fully persisted and available in the RabbitMQ
        // broker
        // before the SeaTunnel job starts consuming.
        Thread.sleep(5000);

        // Execute the SeaTunnel synchronization job.
        // The job uses a multi-table configuration to consume from both queues simultaneously.
        // Note: The actual data validation (e.g., checking row counts, non-null fields)
        // is handled entirely by the 'Assert' sink defined inside the 'rabbitmq_multitable.conf'
        // file.
        Container.ExecResult execResult = container.executeJob("/rabbitmq_multitable.conf");

        // Validate that the SeaTunnel engine finished the job successfully without any exceptions.
        Assertions.assertEquals(
                0, execResult.getExitCode(), "The SeaTunnel job should finish with exit code 0.");
    }

    /**
     * Helper utility method to generate and publish synthetic JSON test data to a specific RabbitMQ
     * queue. * It establishes a direct connection to the RabbitMQ test container, explicitly
     * declares the target queue (to prevent messages from being dropped if the queue doesn't exist
     * yet), and dynamically generates field values based on the provided {@link SeaTunnelRowType}.
     *
     * @param queueName The target RabbitMQ queue to publish messages to.
     * @param rowType The schema used to generate and serialize the data.
     * @param count The number of messages to generate and send.
     */
    private void sendData(String queueName, SeaTunnelRowType rowType, int count) throws Exception {
        // Use the safe wrapper that already knows how to connect to the active container
        RabbitmqClient rabbitmqClient = getRabbitmqClient(queueName);

        try {
            // Explicitly declare the queue before writing.
            rabbitmqClient
                    .getChannel()
                    .queueDeclare(queueName, DURABLE, EXCLUSIVE, AUTO_DELETE, null);

            JsonSerializationSchema serializer = new JsonSerializationSchema(rowType);

            for (int i = 0; i < count; i++) {
                Object[] fields = new Object[rowType.getTotalFields()];
                fields[0] = (long) i;

                String fieldName = rowType.getFieldNames()[1];
                if ("name".equals(fieldName)) {
                    fields[1] = "user_" + i;
                } else if ("age".equals(fieldName)) {
                    fields[1] = 20 + i;
                }

                byte[] message = serializer.serialize(new SeaTunnelRow(fields));

                rabbitmqClient
                        .getChannel()
                        .basicPublish(
                                "",
                                queueName,
                                com.rabbitmq.client.MessageProperties.PERSISTENT_TEXT_PLAIN,
                                message);
            }
            log.info("Successfully sent {} messages to queue {}", count, queueName);
        } finally {
            // Always close the client to prevent connection leaks
            rabbitmqClient.close();
        }
    }
}
