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
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import scala.Tuple2;

@Slf4j
public class RabbitmqIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "library/rabbitmq:3.11";
    private static final String HOST = "rabbitmq-e2e";
    private static final int PORT = 5672;
    private static final String QUEUE_NAME = "test";
    private static final String USERNAME = "guest";
    private static final String PASSWORD = "guest";

    private static final Tuple2<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET = generateTestDataSet();

    private GenericContainer<?> rabbitmqContainer;
    Connection connection = null;
    Channel channel = null;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.rabbitmqContainer = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases(HOST)
                .withExposedPorts(PORT)
                .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                .waitingFor(new HostPortWaitStrategy()
                        .withStartupTimeout(Duration.ofMinutes(2)));
        Startables.deepStart(Stream.of(rabbitmqContainer)).join();
        log.info("rabbitmq container started");
        this.initRabbitMQ();
        this.initSourceData();
    }

    private void initSourceData() throws IOException {
        JsonSerializationSchema jsonSerializationSchema = new JsonSerializationSchema(TEST_DATASET._1());
        List<SeaTunnelRow> rows = TEST_DATASET._2();
        for (int i = 0; i < rows.size(); i++) {
            channel.basicPublish("", "", null, new String(jsonSerializationSchema.serialize(rows.get(i))).getBytes(StandardCharsets.UTF_8));
        }
    }

    private static Tuple2<SeaTunnelRowType, List<SeaTunnelRow>> generateTestDataSet() {

        SeaTunnelRowType rowType = new SeaTunnelRowType(
                new String[]{
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
                new SeaTunnelDataType[]{
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
                }
        );

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row = new SeaTunnelRow(
                 new Object[]{
                     Long.valueOf(i),
                     Collections.singletonMap("key", Short.parseShort("1")),
                     new Byte[]{Byte.parseByte("1")},
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
        return Tuple2.apply(rowType, rows);
    }

    private void initRabbitMQ() {

        try {
            System.out.println(rabbitmqContainer.getHost());
            System.out.println(rabbitmqContainer.getFirstMappedPort());

            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(rabbitmqContainer.getHost());
            connectionFactory.setPort(rabbitmqContainer.getFirstMappedPort());
            connectionFactory.setUsername(USERNAME);
            connectionFactory.setPassword(PASSWORD);
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        } catch (Exception e) {
            throw new RuntimeException("init Rabbitmq error", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
        rabbitmqContainer.close();
    }

    @TestTemplate
    public void testRabbitMQ(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/rabbitmq-to-rabbitmq.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

    }
}
