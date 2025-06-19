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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
public class RedisMasterAndSlaveIT extends TestSuiteBase implements TestResource {
    private static RedisContainerInfo masterContainerInfo;
    private static RedisContainerInfo slaveContainerInfo;
    private static GenericContainer<?> master;
    private static GenericContainer<?> slave;
    private Jedis slaveJedis;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        masterContainerInfo =
                new RedisContainerInfo("redis-e2e-master", 6379, "SeaTunnel", "redis:7");
        master =
                new GenericContainer<>(DockerImageName.parse(masterContainerInfo.getImageName()))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(masterContainerInfo.getHost())
                        .withExposedPorts(masterContainerInfo.getPort())
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                masterContainerInfo.getImageName())))
                        .withCommand(
                                String.format(
                                        "redis-server --requirepass %s",
                                        masterContainerInfo.getPassword()))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        master.start();
        log.info("Redis master container started");

        slaveContainerInfo =
                new RedisContainerInfo("redis-e2e-slave", 6379, "SeaTunnel", "redis:7");
        slave =
                new GenericContainer<>(DockerImageName.parse(slaveContainerInfo.getImageName()))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(slaveContainerInfo.getHost())
                        .withExposedPorts(slaveContainerInfo.getPort())
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                slaveContainerInfo.getImageName())))
                        .withCommand(
                                String.format(
                                        "redis-server --requirepass %s --slaveof %s %s --masterauth %s",
                                        slaveContainerInfo.getPassword(),
                                        masterContainerInfo.getHost(),
                                        masterContainerInfo.getPort(),
                                        masterContainerInfo.getPassword()))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        slave.start();
        log.info("Redis slave container started");
        Startables.deepStart(Stream.of(master, slave)).join();
        this.initSlaveJedis();
    }

    private void initSlaveJedis() {
        Jedis jedis = new Jedis(slave.getHost(), slave.getFirstMappedPort());
        jedis.auth(slaveContainerInfo.getPassword());
        jedis.ping();
        this.slaveJedis = jedis;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (Objects.nonNull(slaveJedis)) {
            slaveJedis.close();
        }

        if (Objects.nonNull(slave)) {
            slave.close();
        }
        if (Objects.nonNull(master)) {
            master.close();
        }
    }

    @TestTemplate
    public void testWriteKeyToReadOnlyRedis(TestContainer container) {
        try {
            container.executeJob("/fake-to-redis-test-readonly-key.conf");
        } catch (Exception e) {
            String containerLogs = container.getServerLogs();
            Assertions.assertTrue(
                    containerLogs.contains("redis.clients.jedis.exceptions.JedisDataException"));
        }
        Assertions.assertEquals(null, slaveJedis.get("key_check"));
    }

    @TestTemplate
    public void testWriteListToReadOnlyRedis(TestContainer container) {
        try {
            container.executeJob("/fake-to-redis-test-readonly-list.conf");
        } catch (Exception e) {
            String containerLogs = container.getServerLogs();
            Assertions.assertTrue(
                    containerLogs.contains("redis.clients.jedis.exceptions.JedisDataException"));
        }
        Assertions.assertEquals(0, slaveJedis.llen("list_check"));
    }

    @TestTemplate
    public void testWriteSetToReadOnlyRedis(TestContainer container) {
        try {
            container.executeJob("/fake-to-redis-test-readonly-set.conf");
        } catch (Exception e) {
            String containerLogs = container.getServerLogs();
            Assertions.assertTrue(
                    containerLogs.contains("redis.clients.jedis.exceptions.JedisDataException"));
        }
        Assertions.assertEquals(0, slaveJedis.scard("set_check"));
    }

    @TestTemplate
    public void testWriteZSetToReadOnlyRedis(TestContainer container) {
        try {
            container.executeJob("/fake-to-redis-test-readonly-zset.conf");
        } catch (Exception e) {
            String containerLogs = container.getServerLogs();
            Assertions.assertTrue(
                    containerLogs.contains("redis.clients.jedis.exceptions.JedisDataException"));
        }
        Assertions.assertEquals(0, slaveJedis.zcard("zset_check"));
    }

    @TestTemplate
    public void testWriteHashToReadOnlyRedis(TestContainer container) {
        try {
            container.executeJob("/fake-to-redis-test-readonly-hash.conf");
        } catch (Exception e) {
            String containerLogs = container.getServerLogs();
            Assertions.assertTrue(
                    containerLogs.contains("redis.clients.jedis.exceptions.JedisDataException"));
        }
        Assertions.assertEquals(0, slaveJedis.hlen("hash_check"));
    }
}
