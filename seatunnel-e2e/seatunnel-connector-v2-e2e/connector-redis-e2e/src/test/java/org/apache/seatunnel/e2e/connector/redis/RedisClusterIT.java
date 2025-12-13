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
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Slf4j
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
    public void startUp() {
        setupRedisContainer();
        createRedisCluster();
        waitForRedisClusterReady();
        initJedisCluster();
        initSourceData();
    }

    private void setupRedisContainer() {
        redisClusterNodes = new GenericContainer[REDIS_CLUSTER_SIZE];

        for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
            String nodeName = "redis-cluster-" + (i + 1);
            int redisPort = REDIS_PORTS[i];
            int busPort = REDIS_BUS_PORTS[i];

            // Get the host machine's IP address
            String hostIp = getHostIpAddress();
            String redisCommand =
                    String.format(
                            "redis-server --cluster-enabled yes --port %d --protected-mode no "
                                    + "--bind 0.0.0.0 --cluster-announce-ip %s --cluster-announce-port %d "
                                    + "--cluster-announce-bus-port %d --requirepass %s",
                            redisPort,
                            hostIp,
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

            // Set the fixed port mapping
            redisClusterNodes[i].setPortBindings(
                    Arrays.asList(redisPort + ":" + redisPort, busPort + ":" + busPort));
        }

        Startables.deepStart(Stream.of(redisClusterNodes)).join();
        log.info("Redis cluster nodes started with ports: {}", Arrays.toString(REDIS_PORTS));
    }

    private void createRedisCluster() {
        try {
            String hostIp = getHostIpAddress();
            StringBuilder clusterCreateCmd =
                    new StringBuilder(
                            "redis-cli --cluster create --cluster-replicas 0 --cluster-yes ");

            for (int port : REDIS_PORTS) {
                clusterCreateCmd.append(hostIp).append(":").append(port).append(" ");
            }

            clusterCreateCmd.append("-a ").append(redisContainerInfo.getPassword());

            log.info("Creating cluster with command: {}", clusterCreateCmd);

            Container.ExecResult result =
                    redisClusterNodes[0].execInContainer("sh", "-c", clusterCreateCmd.toString());

            // Wait for the cluster to be created
            Thread.sleep(5000);

            if (result.getExitCode() != 0) {
                throw new RuntimeException("Failed to create Redis cluster: " + result.getStderr());
            }

            log.info("Redis cluster created successfully");
        } catch (Exception e) {
            throw new RuntimeException("Error creating Redis cluster", e);
        }
    }

    private void waitForRedisClusterReady() {
        log.info("Waiting for Redis cluster to be ready...");

        int maxRetries = 10;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                boolean allReady = true;

                for (int i = 0; i < REDIS_CLUSTER_SIZE; i++) {
                    Container.ExecResult result =
                            redisClusterNodes[i].execInContainer(
                                    "redis-cli",
                                    "-p",
                                    String.valueOf(REDIS_PORTS[i]),
                                    "-a",
                                    redisContainerInfo.getPassword(),
                                    "ping");

                    if (!"PONG".equals(result.getStdout().trim())) {
                        allReady = false;
                        break;
                    }
                }

                if (allReady) {
                    log.info("All Redis nodes are ready after {} attempts", retryCount + 1);
                    return;
                }

            } catch (Exception e) {
                log.debug(
                        "Redis readiness check failed, attempt {}: {}",
                        retryCount + 1,
                        e.getMessage());
            }

            retryCount++;
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Redis cluster failed to become ready within timeout");
    }

    private void initJedisCluster() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();

        String hostIp = getHostIpAddress();
        for (int port : REDIS_PORTS) {
            jedisClusterNodes.add(new HostAndPort(hostIp, port));
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
            Assertions.assertEquals(0, jedisCluster.llen("key_set"));
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

    private String getHostIpAddress() {
        String ip = "";
        try {
            Enumeration<NetworkInterface> networkInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress() && inetAddress instanceof Inet4Address) {
                        ip = inetAddress.getHostAddress();
                    }
                }
            }
        } catch (SocketException ex) {
            ex.printStackTrace();
        }
        return ip;
    }
}
