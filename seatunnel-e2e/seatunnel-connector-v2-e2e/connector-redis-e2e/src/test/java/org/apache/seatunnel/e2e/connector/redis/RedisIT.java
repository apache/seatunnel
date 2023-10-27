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

import lombok.extern.slf4j.Slf4j;
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
public class RedisIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "redis:latest";
    private static final String HOST = "redis-e2e";
    private static final int PORT = 6379;
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
                        .withExposedPorts(PORT)
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
        // db_1 init data
        jedis.select(1);
        for (int i = 0; i < rows.size(); i++) {
            jedis.set("key_test" + i, new String(jsonSerializationSchema.serialize(rows.get(i))));
        }
        // db_num backup
        jedis.select(0);
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
    public void testRedis(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/redis-to-redis.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, jedis.llen("key_list"));
        // Clear data to prevent data duplication in the next TestContainer
        jedis.del("key_list");
        Assertions.assertEquals(0, jedis.llen("key_list"));
    }

    @TestTemplate
    public void testRedisWithExpire(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/redis-to-redis-expire.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, jedis.llen("key_list"));
        // Clear data to prevent data duplication in the next TestContainer
        Thread.sleep(60 * 1000);
        Assertions.assertEquals(0, jedis.llen("key_list"));
    }

    @TestTemplate
    public void restRedisDbNum(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/redis-to-redis-by-db-num.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        jedis.select(2);
        Assertions.assertEquals(100, jedis.llen("db_test"));
        jedis.del("db_test");
        jedis.select(0);
    }
}
