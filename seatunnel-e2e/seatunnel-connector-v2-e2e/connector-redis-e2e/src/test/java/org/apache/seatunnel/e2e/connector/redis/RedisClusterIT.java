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
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisContainerInfo;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerTcpProxy;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.alibaba.dcm.DnsCacheManipulator;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
@ResourceLock("redis-cluster-e2e")
public class RedisClusterIT extends TestSuiteBase implements TestResource {

    private static final int REDIS_CLUSTER_SIZE = 3;

    private GenericContainer<?>[] redisClusterNodes;
    private JedisCluster jedisCluster;
    private RedisContainerInfo redisContainerInfo =
            new RedisContainerInfo("redis-cluster-e2e", 6379, "SeaTunnel", "redis:7");

    private static final int[] REDIS_PORTS = {6379, 6380, 6381};
    private static final int[] REDIS_BUS_PORTS = {16379, 16380, 16381};

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        setupRedisContainer();
        createRedisCluster();
        waitForRedisClusterReady();
        initJedisCluster();
        initSourceData();
    }

    private void setupRedisContainer() throws IOException {
        redisClusterNodes = new GenericContainer[REDIS_CLUSTER_SIZE];

        for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
            String nodeName = "redis-cluster-" + i;
            int redisPort = REDIS_PORTS[i];
            int busPort = REDIS_BUS_PORTS[i];

            String redisCommand =
                    String.format(
                            "redis-server --cluster-enabled yes --port %d --protected-mode no "
                                    + "--bind 0.0.0.0 --cluster-announce-hostname %s "
                                    + "--cluster-preferred-endpoint-type hostname "
                                    + "--cluster-announce-port %d "
                                    + "--cluster-announce-bus-port %d --requirepass %s",
                            redisPort,
                            nodeName,
                            redisPort,
                            busPort,
                            redisContainerInfo.getPassword());

            redisClusterNodes[i] =
                    new GenericContainer<>(DockerImageName.parse(redisContainerInfo.getImageName()))
                            .withNetwork(NETWORK)
                            .withNetworkAliases(nodeName)
                            .withExposedPorts(redisPort, busPort)
                            .withLogConsumer(
                                    new Slf4jLogConsumer(
                                            DockerLoggerFactory.getLogger(
                                                    redisContainerInfo.getImageName())))
                            .withCommand("sh", "-c", redisCommand)
                            .waitingFor(
                                    new HostPortWaitStrategy()
                                            .withStartupTimeout(Duration.ofMinutes(2)));
        }

        Startables.deepStart(Stream.of(redisClusterNodes)).join();
        List<ContainerTcpProxy.PortMapping> portMappings = new ArrayList<>();
        for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
            portMappings.add(
                    ContainerTcpProxy.PortMapping.of(
                            REDIS_PORTS[i],
                            redisClusterNodes[i].getHost(),
                            redisClusterNodes[i].getMappedPort(REDIS_PORTS[i])));
        }
        ContainerTcpProxy proxy = startContainerTcpProxy(portMappings);
        for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
            DnsCacheManipulator.setDnsCache("redis-cluster-" + i, proxy.getLoopbackAddress());
        }
        log.info("Redis cluster nodes started with ports: {}", Arrays.toString(REDIS_PORTS));
    }

    private void createRedisCluster() {
        try {
            StringBuilder clusterCreateCmd =
                    new StringBuilder(
                            "redis-cli --cluster create --cluster-replicas 0 --cluster-yes ");

            for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
                clusterCreateCmd
                        .append("redis-cluster-")
                        .append(i)
                        .append(":")
                        .append(REDIS_PORTS[i])
                        .append(" ");
            }

            clusterCreateCmd.append("-a ").append(redisContainerInfo.getPassword());

            log.info("Creating cluster with command: {}", clusterCreateCmd);

            Container.ExecResult result =
                    redisClusterNodes[0].execInContainer("sh", "-c", clusterCreateCmd.toString());

            if (result.getExitCode() != 0) {
                throw new RuntimeException("Failed to create Redis cluster: " + result.getStderr());
            }

            log.info("Redis cluster created, waiting for slot assignment via CLUSTER INFO...");
        } catch (Exception e) {
            throw new RuntimeException("Error creating Redis cluster", e);
        }
    }

    private void waitForRedisClusterReady() {
        log.info("Waiting for Redis cluster to be ready...");
        Awaitility.await("Redis cluster slots to become ready")
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(
                        () -> {
                            for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
                                Container.ExecResult result =
                                        redisClusterNodes[i].execInContainer(
                                                "redis-cli",
                                                "-p",
                                                String.valueOf(REDIS_PORTS[i]),
                                                "-a",
                                                redisContainerInfo.getPassword(),
                                                "cluster",
                                                "info");
                                String output = result.getStdout().trim();
                                if (!output.contains("cluster_state:ok")
                                        || !output.contains("cluster_slots_ok:16384")) {
                                    return false;
                                }
                            }
                            return true;
                        });
        log.info("Redis cluster is fully ready (all slots assigned)");
    }

    private void initJedisCluster() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();

        for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
            jedisClusterNodes.add(new HostAndPort("redis-cluster-" + i, REDIS_PORTS[i]));
        }

        ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();

        try {
            this.jedisCluster =
                    new JedisCluster(
                            jedisClusterNodes,
                            10000,
                            10000,
                            3,
                            redisContainerInfo.getPassword(),
                            poolConfig);

            log.info("JedisCluster initialized successfully");

        } catch (Exception e) {
            log.error("Failed to create JedisCluster", e);
            throw e;
        }
    }

    private void initSourceData() {
        JsonSerializationSchema jsonSerializationSchema =
                new JsonSerializationSchema(generateTestDataSet().getKey());
        List<SeaTunnelRow> rows = generateTestDataSet().getValue();

        for (int i = 0; i < rows.size(); i++) {
            jedisCluster.set(
                    "key_test" + i, new String(jsonSerializationSchema.serialize(rows.get(i))));
        }

        log.info("Initialized {} test records in Redis cluster", rows.size());
    }

    /** Initialize cluster multi-table source data. */
    private void initClusterMultiTableSourceData() {
        JsonSerializationSchema jsonSerializationSchema =
                new JsonSerializationSchema(generateTestDataSet().getKey());
        List<SeaTunnelRow> rows = generateTestDataSet().getValue();

        // Prepare cluster user data (40 records)
        for (int i = 0; i < 40; i++) {
            SeaTunnelRow row = rows.get(i % rows.size());
            String json = new String(jsonSerializationSchema.serialize(row));
            jedisCluster.set("cluster:user:" + i, json);
        }

        // Prepare cluster order data (30 records)
        for (int i = 0; i < 30; i++) {
            SeaTunnelRow row = rows.get(i % rows.size());
            String json = new String(jsonSerializationSchema.serialize(row));
            jedisCluster.set("cluster:order:" + i, json);
        }

        log.info("Initialized cluster multi-table source data: 40 user records, 30 order records");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (jedisCluster != null) {
            try {
                jedisCluster.close();

                log.info("JedisCluster closed successfully");
            } catch (Exception e) {
                log.warn("Error closing JedisCluster", e);
            }
        }

        if (redisClusterNodes != null) {
            for (GenericContainer<?> container : redisClusterNodes) {
                if (container != null) {
                    try {
                        container.close();
                    } catch (Exception e) {
                        log.warn("Error stopping container", e);
                    }
                }
            }
        }
        for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
            DnsCacheManipulator.removeDnsCache("redis-cluster-" + i);
        }
    }

    @TestTemplate
    public void testRedisClusterScan(TestContainer container)
            throws IOException, InterruptedException {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/cluster-redis-to-redis-scan.conf");
            Assertions.assertEquals(0, execResult.getExitCode());

            long amount = jedisCluster.scard("key_set");
            Assertions.assertEquals(100, amount);
        } finally {
            jedisCluster.del("key_set");
            Assertions.assertFalse(jedisCluster.exists("key_set"));
        }
    }

    @TestTemplate
    public void testRedisClusterCustomValueWithKeyType(TestContainer container)
            throws IOException, InterruptedException {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/cluster-redis-to-redis-type-key.conf");
            Assertions.assertEquals(0, execResult.getExitCode());

            int count = 0;
            for (int i = 0; i < 100; i++) {
                String data = jedisCluster.get("cluster-key-value-check-" + i);
                if (data != null) {
                    Assertions.assertEquals("string", data);
                    count++;
                }
            }
            Assertions.assertEquals(100, count);
        } finally {
            for (int i = 0; i < 100; i++) {
                jedisCluster.del("cluster-key-value-check-" + i);
            }
        }
    }

    @TestTemplate
    public void testRedisClusterCustomValueWithSetType(TestContainer container)
            throws IOException, InterruptedException {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/cluster-redis-to-redis-type-set.conf");
            Assertions.assertEquals(0, execResult.getExitCode());

            long amount = jedisCluster.scard("cluster-set-value-check");
            Assertions.assertEquals(100, amount);
        } finally {
            jedisCluster.del("cluster-set-value-check");
        }
    }

    @TestTemplate
    public void testRedisClusterCustomValueWithListType(TestContainer container)
            throws IOException, InterruptedException {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/cluster-redis-to-redis-type-list.conf");
            Assertions.assertEquals(0, execResult.getExitCode());

            List<String> items = jedisCluster.lrange("cluster-list-value-check", 0, -1);
            Set<String> unique = new HashSet<>(items);

            Assertions.assertEquals(100, unique.size());
        } finally {
            jedisCluster.del("cluster-list-value-check");
        }
    }

    @TestTemplate
    public void testRedisClusterCustomValueWithZSetType(TestContainer container)
            throws IOException, InterruptedException {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/cluster-redis-to-redis-type-zset.conf");
            Assertions.assertEquals(0, execResult.getExitCode());

            long amount = jedisCluster.zcard("cluster-zset-value-check");
            Assertions.assertEquals(100, amount);
        } finally {
            jedisCluster.del("cluster-zset-value-check");
        }
    }

    @TestTemplate
    public void testRedisClusterCustomValueWithHashType(TestContainer container)
            throws IOException, InterruptedException {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/cluster-redis-to-redis-type-hash.conf");
            Assertions.assertEquals(0, execResult.getExitCode());

            long amount = jedisCluster.hlen("cluster-hash-value-check");
            Assertions.assertEquals(100, amount);
            for (int i = 0; i < 100; i++) {
                Assertions.assertEquals(
                        "string", jedisCluster.hget("cluster-hash-value-check", String.valueOf(i)));
            }
        } finally {
            jedisCluster.del("cluster-hash-value-check");
        }
    }

    @TestTemplate
    public void testClusterMultipleTableRedisSource(TestContainer container)
            throws IOException, InterruptedException {
        // Prepare cluster multi-table source data
        initClusterMultiTableSourceData();

        try {
            // Execute job
            Container.ExecResult execResult =
                    container.executeJob("/cluster-scan-multitable-to-redis.conf");
            Assertions.assertEquals(
                    0,
                    execResult.getExitCode(),
                    "Cluster multi-table job should complete successfully");

            // Verify user table results (40 records)
            long userCount = jedisCluster.llen("cluster-multitable-cluster_user_table");
            Assertions.assertEquals(40, userCount, "Cluster user table should have 40 records");

            // Verify order table results (30 records)
            long orderCount = jedisCluster.llen("cluster-multitable-cluster_order_table");
            Assertions.assertEquals(30, orderCount, "Cluster order table should have 30 records");
        } finally {
            // Clean up source data
            for (int i = 0; i < 40; i++) {
                jedisCluster.del("cluster:user:" + i);
            }
            for (int i = 0; i < 30; i++) {
                jedisCluster.del("cluster:order:" + i);
            }

            // Clean up result data
            jedisCluster.del("cluster-multitable-cluster_user_table");
            jedisCluster.del("cluster-multitable-cluster_order_table");
        }
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
}
