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
package org.apache.seatunnel.connectors.seatunnel.redis;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisContainerInfo;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.sink.RedisSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions.CONNECTOR_IDENTITY;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class RedisTemplateTest {

    protected String host;
    protected int port;
    protected String password;
    protected String imageName;
    protected Jedis jedis;
    protected GenericContainer<?> redisContainer;

    @BeforeAll
    public void startUp() {
        initContainerInfo();
        Network NETWORK =
                Network.builder()
                        .createNetworkCmdModifier(
                                cmd -> cmd.withName("SEATUNNEL-" + UUID.randomUUID()))
                        .enableIpv6(false)
                        .build();

        this.redisContainer =
                new GenericContainer<>(DockerImageName.parse(imageName))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(host)
                        .withExposedPorts(port)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(imageName)))
                        .withCommand(String.format("redis-server --requirepass %s", password))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));

        Startables.deepStart(Stream.of(redisContainer)).join();
        log.info("Redis container started");
        this.initJedis();
        this.initSourceData();
    }

    protected void initSourceData() {}

    protected abstract RedisContainerInfo getRedisContainerInfo();

    private void initJedis() {
        Jedis jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.auth(password);
        jedis.ping();
        this.jedis = jedis;
    }

    protected void initContainerInfo() {
        RedisContainerInfo redisContainerInfo = getRedisContainerInfo();
        this.host = redisContainerInfo.getHost();
        this.port = redisContainerInfo.getPort();
        this.password = redisContainerInfo.getPassword();
        this.imageName = redisContainerInfo.getImageName();
    }

    @AfterAll
    public void tearDown() {
        if (Objects.nonNull(jedis)) {
            jedis.close();
        }
        redisContainer.close();
    }

    @Test
    public void testFakeToRedisDeleteHashTest() throws IOException {
        String key = "hash_check";
        Map<String, Object> otherParams = new HashMap<>();
        otherParams.put("hash_key_field", "id");
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                getCatalogTable(0, key),
                getDefaultReadonlyConfig(RedisDataType.HASH, key, otherParams),
                new RedisSinkFactory(),
                Arrays.asList(
                        getSeaTunnelRowInsert1(),
                        getSeaTunnelRowInsert2(),
                        getSeaTunnelRowInsert3(),
                        getSeaTunnelRowUpdateBefore(),
                        getSeaTunnelRowUpdateAfter(),
                        getSeaTunnelRowDelete()));
        Assertions.assertEquals(2, jedis.hlen(key));
        jedis.del(key);
    }

    @Test
    public void testFakeToRedisDeleteKeyTest() throws IOException {
        String key = "key_check:{id}";
        Map<String, Object> otherParams = new HashMap<>();
        otherParams.put("support_custom_key", true);
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                getCatalogTable(0, key),
                getDefaultReadonlyConfig(RedisDataType.KEY, key, otherParams),
                new RedisSinkFactory(),
                Arrays.asList(
                        getSeaTunnelRowInsert1(),
                        getSeaTunnelRowInsert2(),
                        getSeaTunnelRowInsert3(),
                        getSeaTunnelRowUpdateBefore(),
                        getSeaTunnelRowUpdateAfter(),
                        getSeaTunnelRowDelete()));
        int count = 0;
        for (int i = 1; i <= 3; i++) {
            String data = jedis.get("key_check:" + i);
            if (data != null) {
                count++;
            }
        }
        Assertions.assertEquals(2, count);
        for (int i = 1; i <= 3; i++) {
            jedis.del("key_check:" + i);
        }
    }

    @Test
    public void testFakeToRedisDeleteListTest() throws IOException {
        String key = "list_check";
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                getCatalogTable(0, key),
                getDefaultReadonlyConfig(RedisDataType.LIST, key, new HashMap<>()),
                new RedisSinkFactory(),
                Arrays.asList(
                        getSeaTunnelRowInsert1(),
                        getSeaTunnelRowInsert2(),
                        getSeaTunnelRowInsert3(),
                        getSeaTunnelRowUpdateBefore(),
                        getSeaTunnelRowUpdateAfter(),
                        getSeaTunnelRowDelete()));
        Assertions.assertEquals(2, jedis.llen(key));
        jedis.del(key);
    }

    @Test
    public void testFakeToRedisDeleteSetTest() throws IOException {
        String key = "set_check";
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                getCatalogTable(0, key),
                getDefaultReadonlyConfig(RedisDataType.SET, key, new HashMap<>()),
                new RedisSinkFactory(),
                Arrays.asList(
                        getSeaTunnelRowInsert1(),
                        getSeaTunnelRowInsert2(),
                        getSeaTunnelRowInsert3(),
                        getSeaTunnelRowUpdateBefore(),
                        getSeaTunnelRowUpdateAfter(),
                        getSeaTunnelRowDelete()));
        Assertions.assertEquals(2, jedis.scard(key));
        jedis.del(key);
    }

    @Test
    public void testFakeToToRedisDeleteZSetTest() throws IOException {
        String key = "zset_check";
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                getCatalogTable(0, key),
                getDefaultReadonlyConfig(RedisDataType.ZSET, key, new HashMap<>()),
                new RedisSinkFactory(),
                Arrays.asList(
                        getSeaTunnelRowInsert1(),
                        getSeaTunnelRowInsert2(),
                        getSeaTunnelRowInsert3(),
                        getSeaTunnelRowUpdateBefore(),
                        getSeaTunnelRowUpdateAfter(),
                        getSeaTunnelRowDelete()));
        Assertions.assertEquals(2, jedis.zcard(key));
        jedis.del(key);
    }

    private ReadonlyConfig getDefaultReadonlyConfig(
            RedisDataType dataType, String key, Map<String, Object> otherParams) {
        Map<String, Object> map = new HashMap<>(otherParams);
        map.put("host", redisContainer.getHost());
        map.put("port", redisContainer.getFirstMappedPort());
        map.put("db_num", 0);
        map.put("auth", password);
        map.put("key", key);
        map.put("data_type", dataType.name());
        map.put("batch_size", 33);
        return ReadonlyConfig.fromMap(map);
    }

    private SeaTunnelRow getSeaTunnelRowInsert1() {
        return new SeaTunnelRow(
                new Object[] {
                    1,
                    true,
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    4.3f,
                    5.3d,
                    BigDecimal.valueOf(6.3).setScale(1),
                    "NEW",
                    LocalDateTime.parse("2020-02-02T02:02:02")
                });
    }

    private SeaTunnelRow getSeaTunnelRowInsert2() {
        return new SeaTunnelRow(
                new Object[] {
                    2,
                    true,
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    4.3f,
                    5.3d,
                    BigDecimal.valueOf(6.3).setScale(1),
                    "NEW",
                    LocalDateTime.parse("2020-02-02T02:02:02")
                });
    }

    private SeaTunnelRow getSeaTunnelRowInsert3() {
        return new SeaTunnelRow(
                new Object[] {
                    3,
                    true,
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    4.3f,
                    5.3d,
                    BigDecimal.valueOf(6.3).setScale(1),
                    "NEW",
                    LocalDateTime.parse("2020-02-02T02:02:02")
                });
    }

    private SeaTunnelRow getSeaTunnelRowUpdateBefore() {
        final SeaTunnelRow seaTunnelRow =
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            true,
                            (byte) 1,
                            (short) 2,
                            3,
                            4L,
                            4.3f,
                            5.3d,
                            BigDecimal.valueOf(6.3).setScale(1),
                            "NEW",
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.UPDATE_BEFORE);
        return seaTunnelRow;
    }

    private SeaTunnelRow getSeaTunnelRowUpdateAfter() {
        final SeaTunnelRow seaTunnelRow =
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            true,
                            (byte) 2,
                            (short) 2,
                            3,
                            4L,
                            4.3f,
                            5.3d,
                            BigDecimal.valueOf(6.3).setScale(1),
                            "NEW",
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.UPDATE_AFTER);
        return seaTunnelRow;
    }

    private SeaTunnelRow getSeaTunnelRowDelete() {
        final SeaTunnelRow seaTunnelRow =
                new SeaTunnelRow(
                        new Object[] {
                            2,
                            true,
                            (byte) 1,
                            (short) 2,
                            3,
                            4L,
                            4.3f,
                            5.3d,
                            BigDecimal.valueOf(6.3).setScale(1),
                            "NEW",
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.DELETE);
        return seaTunnelRow;
    }

    private CatalogTable getCatalogTable(Integer dbNum, String key) {
        return CatalogTable.of(
                TableIdentifier.of(CONNECTOR_IDENTITY, dbNum.toString(), key),
                getTableSchema(),
                new HashMap<>(),
                new ArrayList<>(),
                "");
    }

    private TableSchema getTableSchema() {
        return new TableSchema(getColumns(), null, null);
    }

    private List<Column> getColumns() {
        List<Column> columns = new ArrayList<>();
        columns.add(new PhysicalColumn("id", BasicType.INT_TYPE, 32L, 0, true, "", ""));
        columns.add(new PhysicalColumn("val_bool", BasicType.BOOLEAN_TYPE, 1L, 0, true, "", ""));
        columns.add(new PhysicalColumn("val_int8", BasicType.BYTE_TYPE, 8L, 0, true, "", ""));
        columns.add(new PhysicalColumn("val_int16", BasicType.SHORT_TYPE, 16L, 0, true, "", ""));
        columns.add(new PhysicalColumn("val_int32", BasicType.INT_TYPE, 32L, 0, true, "", ""));
        columns.add(new PhysicalColumn("val_int64", BasicType.LONG_TYPE, 64L, 0, true, "", ""));
        columns.add(new PhysicalColumn("val_float", BasicType.FLOAT_TYPE, 32L, 0, true, "", ""));
        columns.add(new PhysicalColumn("val_double", BasicType.DOUBLE_TYPE, 64L, 0, true, "", ""));
        columns.add(
                new PhysicalColumn("val_decimal", new DecimalType(16, 1), 16L, 1, true, "", ""));
        columns.add(new PhysicalColumn("val_string", BasicType.STRING_TYPE, 0L, 0, true, "", ""));
        columns.add(
                new PhysicalColumn(
                        "val_unixtime_micros",
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        64L,
                        6,
                        true,
                        "",
                        ""));
        return columns;
    }
}
