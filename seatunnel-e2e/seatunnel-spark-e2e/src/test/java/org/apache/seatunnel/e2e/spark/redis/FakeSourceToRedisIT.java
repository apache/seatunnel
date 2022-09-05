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

package org.apache.seatunnel.e2e.spark.redis;

import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.spark.SparkContainer;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class FakeSourceToRedisIT extends SparkContainer {
    private static final String REDIS_IMAGE = "redis:latest";
    private static final String REDIS_CONTAINER_HOST = "spark_e2e_redis";
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
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initJedis);
    }

    private void initJedis() {
        jedis = new Jedis(REDIS_HOST, REDIS_PORT);
    }

    @Test
    public void testFakeSourceToRedisSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/jdbc/fakesource_to_redis.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @AfterEach
    public void close() {
        super.close();
        jedis.close();
        redisContainer.close();
    }
}
