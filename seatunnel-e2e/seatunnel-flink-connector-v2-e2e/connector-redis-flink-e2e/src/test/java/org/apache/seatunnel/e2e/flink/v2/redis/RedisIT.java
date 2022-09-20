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

package org.apache.seatunnel.e2e.flink.v2.redis;

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class RedisIT extends FlinkContainer {
    private static final String REDIS_IMAGE = "redis:latest";
    private static final String REDIS_CONTAINER_HOST = "flink_e2e_redis";
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private GenericContainer<?> redisContainer;
    private Jedis jedis;

    @BeforeEach
    public void startRedisContainer() {
        redisContainer = new GenericContainer<>(REDIS_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(REDIS_CONTAINER_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log));
        redisContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", REDIS_PORT, REDIS_PORT)));
        Startables.deepStart(Stream.of(redisContainer)).join();
        log.info("Redis container started");
        given().ignoreExceptions()
            .await()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(this::initJedis);
        this.generateTestData();
    }

    private void initJedis() {
        jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        jedis.connect();
    }

    private void generateTestData() {
        jedis.set("key_test", "test");
        jedis.set("key_test1", "test1");
        jedis.set("key_test2", "test2");
        jedis.set("key_test3", "test3");
        jedis.set("key_test4", "test4");
    }

    @Test
    public void testRedisSourceAndSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/redis/redis_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(5, jedis.llen("key_list"));
    }

    @AfterEach
    public void close() {
        jedis.close();
        redisContainer.close();
    }
}
