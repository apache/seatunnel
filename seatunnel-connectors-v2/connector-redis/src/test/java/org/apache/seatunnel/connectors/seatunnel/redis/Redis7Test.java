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
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.JedisWrapper;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisContainerInfo;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.GenericContainer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "There is no docker environment on the windows test system")
public class Redis7Test extends RedisTemplateTest {

    @Test
    public void namedUserAuthentication() {
        String user = "seatunnel_named";
        // Sharing the default user's password must not silently select the default identity.
        jedis.aclSetUser(
                user,
                "reset",
                "on",
                ">" + password,
                "~auth:*",
                "+select",
                "+info",
                "+get",
                "+set",
                "+acl|whoami");
        List<String> aclBefore = jedis.aclList();
        try {
            RedisParameters parameters = connectionParameters(user, password);
            parameters.setDbNum(2);
            try (Jedis connection = parameters.buildJedis()) {
                Assertions.assertEquals(user, connection.aclWhoAmI());
                connection.set("auth:selected", "value");
                Assertions.assertThrows(
                        JedisDataException.class, () -> connection.set("outside:auth", "value"));
                Assertions.assertThrows(
                        JedisDataException.class, () -> connection.aclSetUser("unexpected"));
            }
            jedis.select(2);
            Assertions.assertEquals("value", jedis.get("auth:selected"));
            jedis.del("auth:selected");
            jedis.select(0);
            RedisClient client = parameters.buildRedisClient();
            try {
                Assertions.assertEquals(7, parameters.getRedisVersion());
            } finally {
                client.close();
            }
            Assertions.assertEquals(aclBefore, jedis.aclList());
            jedis.aclSetUser(user, "resetpass", ">named-password");
            try (Jedis connection = connectionParameters(user, "named-password").buildJedis()) {
                Assertions.assertEquals(user, connection.aclWhoAmI());
            }
        } finally {
            jedis.select(0);
            jedis.aclDelUser(user);
        }
    }

    @Test
    public void namedUserMissingBlankAndWrongPassword() {
        String user = "seatunnel_nopass";
        jedis.aclSetUser(user, "reset", "on", "nopass", "+select", "+acl|whoami");
        try {
            for (String auth : new String[] {null, "", "  "}) {
                try (Jedis connection = connectionParameters(user, auth).buildJedis()) {
                    Assertions.assertEquals(user, connection.aclWhoAmI());
                }
            }
            jedis.aclSetUser(user, "resetpass", ">named-password");
            for (String auth : new String[] {null, "", "  ", "wrong-password"}) {
                Assertions.assertThrows(
                        JedisDataException.class,
                        () -> connectionParameters(user, auth).buildJedis());
            }
            jedis.aclSetUser(user, "resetpass", ">  ");
            try (Jedis connection = connectionParameters(user, "  ").buildJedis()) {
                Assertions.assertEquals(user, connection.aclWhoAmI());
            }
        } finally {
            jedis.aclDelUser(user);
        }
    }

    @Test
    public void legacyDefaultUserAuthentication() {
        for (String user : new String[] {null, "", "  "}) {
            try (Jedis connection = connectionParameters(user, password).buildJedis()) {
                Assertions.assertEquals("default", connection.aclWhoAmI());
            }
        }
        jedis.aclSetUser("default", "nopass");
        try {
            for (String user : new String[] {null, "", "  "}) {
                for (String auth : new String[] {null, "", "  "}) {
                    try (Jedis connection = connectionParameters(user, auth).buildJedis()) {
                        Assertions.assertEquals("default", connection.aclWhoAmI());
                    }
                }
            }
            List<String> aclBefore = jedis.aclList();
            Assertions.assertThrows(
                    JedisDataException.class,
                    () -> connectionParameters("nonexistent", null).buildJedis());
            Assertions.assertEquals(aclBefore, jedis.aclList());
        } finally {
            jedis.aclSetUser("default", "resetpass", ">" + password);
        }
    }

    @Test
    public void failedInitializationClosesConnections() throws InterruptedException {
        String user = "seatunnel_denied";
        jedis.aclSetUser(user, "reset", "on", ">named-password", "+select");
        try {
            assertConnectionCountUnchanged(() -> connectionParameters(user, "wrong").buildJedis());
            RedisParameters parameters = connectionParameters(user, "named-password");
            parameters.setDbNum(-1);
            assertConnectionCountUnchanged(parameters::buildJedis);
            parameters.setDbNum(0);
            assertConnectionCountUnchanged(parameters::buildRedisClient);
            jedis.aclSetUser(user, "-select");
            assertConnectionCountUnchanged(parameters::buildJedis);
        } finally {
            jedis.aclDelUser(user);
        }
    }

    private void assertConnectionCountUnchanged(Runnable connect) throws InterruptedException {
        int connections = jedis.clientList().split("\n").length;
        Assertions.assertThrows(JedisDataException.class, connect::run);
        awaitCondition(() -> connections == jedis.clientList().split("\n").length);
    }

    @Test
    public void namedClusterAuthentication() throws InterruptedException, UnknownHostException {
        // A real cluster-enabled server owns all slots; no external cluster is required.
        try (GenericContainer<?> cluster =
                new GenericContainer<>("redis:7")
                        .withExposedPorts(6379)
                        .withCommand(
                                "redis-server",
                                "--cluster-enabled",
                                "yes",
                                "--save",
                                "",
                                "--appendonly",
                                "no")) {
            cluster.start();
            try (Jedis admin = new Jedis(cluster.getHost(), cluster.getFirstMappedPort())) {
                admin.configSet(
                        "cluster-announce-ip",
                        InetAddress.getByName(cluster.getHost()).getHostAddress());
                admin.configSet("cluster-announce-port", cluster.getFirstMappedPort().toString());
                admin.clusterAddSlots(IntStream.range(0, 16384).toArray());
                awaitCondition(() -> admin.clusterInfo().contains("cluster_state:ok"));
                admin.aclSetUser(
                        "seatunnel_cluster",
                        "reset",
                        "on",
                        ">named-password",
                        "~auth:*",
                        "+cluster|slots",
                        "+info",
                        "+get",
                        "+set",
                        "+acl|whoami");
                RedisParameters parameters =
                        connectionParameters("seatunnel_cluster", "named-password");
                parameters.setMode(RedisBaseOptions.RedisMode.CLUSTER);
                parameters.setRedisNodes(
                        Collections.singletonList(
                                cluster.getHost() + ":" + cluster.getFirstMappedPort()));
                List<String> aclBefore = admin.aclList();
                try (JedisWrapper connection = (JedisWrapper) parameters.buildJedis()) {
                    for (String node : connection.getClusterNodes().keySet()) {
                        Assertions.assertEquals(
                                "seatunnel_cluster", connection.getJedis(node).aclWhoAmI());
                    }
                    connection.set("auth:cluster", "value");
                    Assertions.assertEquals("value", connection.get("auth:cluster"));
                }
                RedisClient client = parameters.buildRedisClient();
                client.close();
                Assertions.assertEquals(aclBefore, admin.aclList());

                parameters.setUser("");
                parameters.setAuth("");
                assertClusterIdentity(parameters, "default");
                admin.aclSetUser("default", "resetpass", ">named-password");
                parameters.setAuth("named-password");
                assertClusterIdentity(parameters, "default");
                parameters.setUser("seatunnel_cluster");
                assertClusterIdentity(parameters, "seatunnel_cluster");

                parameters.setAuth("wrong-password");
                int beforeAuthenticationFailure = admin.clientList().split("\n").length;
                Assertions.assertThrows(JedisDataException.class, parameters::buildJedis);
                awaitCondition(
                        () -> beforeAuthenticationFailure == admin.clientList().split("\n").length);
                parameters.setAuth("named-password");
                admin.aclSetUser("seatunnel_cluster", "-info");
                int connections = admin.clientList().split("\n").length;
                Assertions.assertThrows(RuntimeException.class, parameters::buildRedisClient);
                awaitCondition(() -> connections == admin.clientList().split("\n").length);

                admin.aclSetUser("seatunnel_cluster", "nopass");
                for (String auth : new String[] {null, "", "  "}) {
                    parameters.setAuth(auth);
                    assertClusterIdentity(parameters, "seatunnel_cluster");
                }
            }
        }
    }

    private void assertClusterIdentity(RedisParameters parameters, String user) {
        try (JedisWrapper connection = (JedisWrapper) parameters.buildJedis()) {
            for (String node : connection.getClusterNodes().keySet()) {
                Assertions.assertEquals(user, connection.getJedis(node).aclWhoAmI());
            }
        }
    }

    private void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        Assertions.assertTrue(condition.getAsBoolean());
    }

    private RedisParameters connectionParameters(String user, String auth) {
        Map<String, Object> config = new HashMap<>();
        config.put("host", redisContainer.getHost());
        config.put("port", redisContainer.getFirstMappedPort());
        if (user != null) {
            config.put("user", user);
        }
        if (auth != null) {
            config.put("auth", auth);
        }
        RedisParameters parameters = new RedisParameters();
        parameters.buildConnectionConfig(ReadonlyConfig.fromMap(config));
        return parameters;
    }

    @Override
    public RedisContainerInfo getRedisContainerInfo() {
        return new RedisContainerInfo("redis-e2e", 6379, "SeaTunnel", "redis:7");
    }
}
