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

package org.apache.seatunnel.e2e.connector.redis;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.*;
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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
public class RedisClusterIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "redis:latest";
    private static final String HOST = "redis-e2e";
    private static final String PASSWORD = "SeaTunnel";

    private static final Pair<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET =
            generateTestDataSet();

    private GenericContainer<?> redisContainer;

    private Jedis jedis;

    @BeforeAll
    @Override
    public void startUp() {
        this.redisContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(60453, 60454, 60455, 60456)
                        .waitingFor(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofSeconds(30))
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .withCommand(String.format("redis-server --requirepass %s", PASSWORD))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        Startables.deepStart(Stream.of(redisContainer)).join();
        log.info("Redis container started");
        this.initJedis();
        this.initSourceData();
    }

    private void initSourceData() {
        JsonSerializationSchema jsonSerializationSchema =
                new JsonSerializationSchema(TEST_DATASET.getKey());
        List<SeaTunnelRow> rows = TEST_DATASET.getValue();
        for (int i = 0; i < rows.size(); i++) {
            jedis.set("key_test" + i, new String(jsonSerializationSchema.serialize(rows.get(i))));
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
                            new MapType<>(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
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
        for (int i = 0; i < 100; i++) {
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
            rows.add(row);
        }
        return Pair.of(rowType, rows);
    }

    private void initJedis() {
        Jedis jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.auth(PASSWORD);
        jedis.ping();
        this.jedis = jedis;
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (Objects.nonNull(jedis)) {
            jedis.close();
        }

        if (Objects.nonNull(redisContainer)) {
            redisContainer.close();
        }
    }

    @TestTemplate
    public void testRedisCluster(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/redis-to-redis-cluster.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, jedis.llen("key_list"));
        // Clear data to prevent data duplication in the next TestContainer
        jedis.del("key_list");
        Assertions.assertEquals(0, jedis.llen("key_list"));
    }
}
