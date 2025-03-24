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
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
public abstract class RedisTestCaseTemplateIT extends TestSuiteBase implements TestResource {

    private String host;
    private int port;
    private String password;

    private String imageName;

    private Pair<SeaTunnelRowType, List<SeaTunnelRow>> testDateSet;

    private GenericContainer<?> redisContainer;

    private Jedis jedis;

    @BeforeAll
    @Override
    public void startUp() {
        initContainerInfo();
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

    private void initContainerInfo() {
        RedisContainerInfo redisContainerInfo = getRedisContainerInfo();
        this.host = redisContainerInfo.getHost();
        this.port = redisContainerInfo.getPort();
        this.password = redisContainerInfo.getPassword();
        this.imageName = redisContainerInfo.getImageName();
        this.testDateSet = generateTestDataSet();
    }

    private void initSourceData() {
        JsonSerializationSchema jsonSerializationSchema =
                new JsonSerializationSchema(testDateSet.getKey());
        List<SeaTunnelRow> rows = testDateSet.getValue();
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

    protected Pair<SeaTunnelRowType, List<SeaTunnelRow>> generateTestDataSet() {
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
        jedis.auth(password);
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
    public void testRedisDbNum(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/redis-to-redis-by-db-num.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        jedis.select(2);
        Assertions.assertEquals(100, jedis.llen("db_test"));
        jedis.del("db_test");
        jedis.select(0);
    }

    @TestTemplate
    public void testScanStringTypeWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        String keyPrefix = "string_test";
        for (int i = 0; i < 1000; i++) {
            jedis.set(keyPrefix + i, "val");
        }
        Container.ExecResult execResult = container.executeJob("/scan-string-to-redis.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> list = jedis.lrange("string_test_list", 0, -1);
        Assertions.assertEquals(1000, list.size());
        jedis.del("string_test_list");
        for (int i = 0; i < 1000; i++) {
            jedis.del(keyPrefix + i);
        }
    }

    @TestTemplate
    public void testScanListTypeWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        String keyPrefix = "list-test-read";
        for (int i = 0; i < 100; i++) {
            String list = keyPrefix + i;
            for (int j = 0; j < 10; j++) {
                jedis.lpush(list, "val" + j);
            }
        }
        Container.ExecResult execResult =
                container.executeJob("/scan-list-test-read-to-redis-list-test-check.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> list = jedis.lrange("list-test-check", 0, -1);
        Assertions.assertEquals(1000, list.size());
        jedis.del("list-test-check");
        for (int i = 0; i < 100; i++) {
            String delKey = keyPrefix + i;
            jedis.del(delKey);
        }
    }

    @TestTemplate
    public void testScanSetTypeWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        String setKeyPrefix = "key-test-set";
        for (int i = 0; i < 100; i++) {
            String setKey = setKeyPrefix + i;
            for (int j = 0; j < 10; j++) {
                jedis.sadd(setKey, j + "");
            }
        }
        Container.ExecResult execResult =
                container.executeJob("/scan-set-to-redis-list-set-check.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> list = jedis.lrange("list-set-check", 0, -1);
        Assertions.assertEquals(1000, list.size());
        jedis.del("list-set-check");
        for (int i = 0; i < 100; i++) {
            String setKey = setKeyPrefix + i;
            jedis.del(setKey);
        }
    }

    @TestTemplate
    public void testScanHashTypeWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        String hashKeyPrefix = "key-test-hash";
        for (int i = 0; i < 100; i++) {
            String setKey = hashKeyPrefix + i;
            Map<String, String> map = new HashMap<>();
            map.put("name", "fuyoujie");
            jedis.hset(setKey, map);
        }
        Container.ExecResult execResult =
                container.executeJob("/scan-hash-to-redis-list-hash-check.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> list = jedis.lrange("list-hash-check", 0, -1);
        Assertions.assertEquals(100, list.size());
        jedis.del("list-hash-check");
        for (int i = 0; i < 100; i++) {
            String hashKey = hashKeyPrefix + i;
            jedis.del(hashKey);
        }
        for (int i = 0; i < 100; i++) {
            String hashKey = hashKeyPrefix + i;
            for (int j = 0; j < 10; j++) {
                jedis.del(hashKey);
            }
        }
    }

    @TestTemplate
    public void testScanZsetTypeWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        String zSetKeyPrefix = "key-test-zset";
        for (int i = 0; i < 100; i++) {
            String key = zSetKeyPrefix + i;
            for (int j = 0; j < 10; j++) {
                jedis.zadd(key, 1, j + "");
            }
        }
        Container.ExecResult execResult =
                container.executeJob("/scan-zset-to-redis-list-zset-check.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> list = jedis.lrange("list-zset-check", 0, -1);
        Assertions.assertEquals(1000, list.size());
        jedis.del("list-zset-check");
        for (int i = 0; i < 100; i++) {
            String key = zSetKeyPrefix + i;
            jedis.del(key);
        }
    }

    @TestTemplate
    public void testMultipletableRedisSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-multipletableredissink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        jedis.select(3);
        Assertions.assertEquals(2, jedis.llen("key_multi_list"));
        jedis.del("key_multi_list");
        jedis.select(0);
    }

    @TestTemplate
    public void testCustomKeyWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/redis-to-redis-custom-key.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        int count = 0;
        for (int i = 0; i < 100; i++) {
            String data = jedis.get("custom-key-check:" + i);
            if (data != null) {
                count++;
            }
        }
        Assertions.assertEquals(100, count);
        for (int i = 0; i < 100; i++) {
            jedis.del("custom-key-check:" + i);
        }
    }

    @TestTemplate
    public void testCustomValueForStringWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/redis-to-redis-custom-value-for-key.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        int count = 0;
        for (int i = 0; i < 100; i++) {
            String data = jedis.get("custom-value-check:" + i);
            if (data != null) {
                Assertions.assertEquals("string", data);
                count++;
            }
        }
        Assertions.assertEquals(100, count);
        for (int i = 0; i < 100; i++) {
            jedis.del("custom-value-check:" + i);
        }
    }

    @TestTemplate
    public void testCustomValueForListWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/redis-to-redis-custom-value-for-list.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> list = jedis.lrange("custom-value-check-list", 0, -1);
        Assertions.assertEquals(100, list.size());
        jedis.del("custom-value-check-list");
    }

    @TestTemplate
    public void testCustomValueForSetWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/redis-to-redis-custom-value-for-set.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        long amount = jedis.scard("custom-value-check-set");
        Assertions.assertEquals(100, amount);
        jedis.del("custom-value-check-set");
    }

    @TestTemplate
    public void testCustomValueForZSetWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/redis-to-redis-custom-value-for-zset.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        long amount = jedis.zcard("custom-value-check-zset");
        Assertions.assertEquals(100, amount);
        jedis.del("custom-value-check-zset");
    }

    @TestTemplate
    public void testCustomHashKeyAndValueWriteRedis(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/redis-to-redis-custom-hash-key-and-value.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        long amount = jedis.hlen("custom-hash-check");
        Assertions.assertEquals(100, amount);
        for (int i = 0; i < 100; i++) {
            Assertions.assertEquals("string", jedis.hget("custom-hash-check", String.valueOf(i)));
        }
        jedis.del("custom-hash-check");
    }

    @TestTemplate
    public void testFakeToRedisDeleteHashTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-redis-test-delete-hash.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(2, jedis.hlen("hash_check"));
        jedis.del("hash_check");
    }

    @TestTemplate
    public void testFakeToRedisDeleteKeyTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-redis-test-delete-key.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
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

    @TestTemplate
    public void testFakeToRedisDeleteListTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-redis-test-delete-list.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(2, jedis.llen("list_check"));
        jedis.del("list_check");
    }

    @TestTemplate
    public void testFakeToRedisDeleteSetTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-redis-test-delete-set.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(2, jedis.scard("set_check"));
        jedis.del("set_check");
    }

    @TestTemplate
    public void testFakeToToRedisDeleteZSetTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-redis-test-delete-zset.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(2, jedis.zcard("zset_check"));
        jedis.del("zset_check");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Only support for seatunnel")
    @DisabledOnOs(OS.WINDOWS)
    public void testFakeToRedisInRealTimeTest(TestContainer container)
            throws IOException, InterruptedException {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/fake-to-redis-test-in-real-time.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(3, jedis.llen("list_check"));
                        });
        jedis.del("list_check");
        // Get the task id
        Container.ExecResult execResult = container.executeBaseCommand(new String[] {"-l"});
        String regex = "(\\d+)\\s+";
        Pattern pattern = Pattern.compile(regex);
        List<String> runningJobId =
                Arrays.stream(execResult.getStdout().toString().split("\n"))
                        .filter(s -> s.contains("fake-to-redis-test-in-real-time"))
                        .map(
                                s -> {
                                    Matcher matcher = pattern.matcher(s);
                                    return matcher.find() ? matcher.group(1) : null;
                                })
                        .filter(jobId -> jobId != null)
                        .collect(Collectors.toList());
        Assertions.assertEquals(1, runningJobId.size());
        // Verify that the status is Running
        for (String jobId : runningJobId) {
            Container.ExecResult execResult1 =
                    container.executeBaseCommand(new String[] {"-j", jobId});
            String stdout = execResult1.getStdout();
            ObjectNode jsonNodes = JsonUtils.parseObject(stdout);
            Assertions.assertEquals(jsonNodes.get("jobStatus").asText(), "RUNNING");
        }
        // Execute cancellation task
        String[] batchCancelCommand =
                Stream.concat(Arrays.stream(new String[] {"-can"}), runningJobId.stream())
                        .toArray(String[]::new);
        Assertions.assertEquals(0, container.executeBaseCommand(batchCancelCommand).getExitCode());

        // Verify whether the cancellation is successful
        for (String jobId : runningJobId) {
            Container.ExecResult execResult1 =
                    container.executeBaseCommand(new String[] {"-j", jobId});
            String stdout = execResult1.getStdout();
            ObjectNode jsonNodes = JsonUtils.parseObject(stdout);
            Assertions.assertEquals(jsonNodes.get("jobStatus").asText(), "CANCELED");
        }
    }

    @TestTemplate
    public void testFakeToRedisNormalKeyIsNullTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-redis-test-normal-key-is-null.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        int count = 0;
        String data = jedis.get("");
        if (data != null) {
            count++;
            jedis.del("");
        }
        for (int i = 2; i <= 3; i++) {
            data = jedis.get("NEW" + i);
            if (data != null) {
                count++;
                jedis.del("NEW" + i);
            }
        }
        Assertions.assertEquals(2, count);
    }

    @TestTemplate
    public void testFakeToRedisCustomKeyIsNullTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-redis-test-custom-key-is-null.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        int count = 0;
        String data = jedis.get("key_check:");
        if (data != null) {
            count++;
            jedis.del("key_check:");
        }
        for (int i = 2; i <= 3; i++) {
            data = jedis.get("key_check:NEW" + i);
            if (data != null) {
                count++;
                jedis.del("key_check:NEW" + i);
            }
        }
        Assertions.assertEquals(2, count);
    }

    @TestTemplate
    public void testFakeToRedisOtherTypeValueIsNullTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/fake-to-redis-test-custom-value-when-other-type-is-null.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(2, jedis.llen("list_check"));
        jedis.del("list_check");
    }

    @TestTemplate
    public void testFakeToRedisHashTypeKeyIsNullTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/fake-to-redis-test-custom-value-when-hash-key-is-null.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(2, jedis.hlen("hash_check"));
        jedis.del("hash_check");
    }

    @TestTemplate
    public void testFakeToRedisHashTypeValueIsNullTest(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/fake-to-redis-test-custom-value-when-hash-value-is-null.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(2, jedis.hlen("hash_check"));
        jedis.del("hash_check");
    }

    public abstract RedisContainerInfo getRedisContainerInfo();
}
