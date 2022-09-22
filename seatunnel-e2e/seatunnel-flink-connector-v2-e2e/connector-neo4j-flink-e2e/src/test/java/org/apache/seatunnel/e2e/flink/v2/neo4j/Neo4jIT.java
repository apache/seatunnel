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

package org.apache.seatunnel.e2e.flink.v2.neo4j;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class Neo4jIT extends FlinkContainer {

    private static final String CONTAINER_IMAGE = "neo4j:latest";
    private static final String CONTAINER_HOST = "neo4j_host";
    private static final int CONTAINER_PORT = 7687;
    private static final String CONTAINER_NEO4J_USERNAME = "neo4j";
    private static final String CONTAINER_NEO4J_PASSWORD = "1234";
    private static final URI CONTAINER_URI = URI.create("neo4j://localhost:" + CONTAINER_PORT);

    private GenericContainer<?> container;
    private Driver neo4jDriver;
    private Session neo4jSession;

    @BeforeAll
    public void init() {
        DockerImageName imageName = DockerImageName.parse(CONTAINER_IMAGE);
        container = new GenericContainer<>(imageName)
            .withNetwork(NETWORK)
            .withNetworkAliases(CONTAINER_HOST)
            .withExposedPorts(CONTAINER_PORT)
            .withEnv("NEO4J_AUTH", CONTAINER_NEO4J_USERNAME + "/" + CONTAINER_NEO4J_PASSWORD)
            .withLogConsumer(new Slf4jLogConsumer(log));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", CONTAINER_PORT, CONTAINER_PORT)));
        Startables.deepStart(Stream.of(container)).join();
        log.info("container started");
        Awaitility.given().ignoreExceptions()
            .await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(this::initConnection);

    }

    private void initConnection() {
        neo4jDriver = GraphDatabase.driver(CONTAINER_URI, AuthTokens.basic(CONTAINER_NEO4J_USERNAME, CONTAINER_NEO4J_PASSWORD));
        neo4jSession = neo4jDriver.session(SessionConfig.forDatabase("neo4j"));
    }

    @Test
    public void testSink() throws IOException, InterruptedException {
        // when
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/neo4j/fake_to_neo4j.conf");

        // then
        Assertions.assertEquals(0, execResult.getExitCode());

        final Stream<Record> recordStream = neo4jSession.run("MATCH (a:Person) RETURN a.name, a.age").stream();
        Assertions.assertTrue(recordStream.findAny().isPresent());
        Assertions.assertTrue(recordStream.anyMatch(record -> record.get("a.age").asInt() > 0));

    }

    @Test
    public void testSource() throws IOException, InterruptedException {
        // given
        neo4jSession.run("CREATE (a:Person {name: 'foo', age: 10})");
        // when
        final Container.ExecResult execResult = executeSeaTunnelFlinkJob("/neo4j/neo4j_to_assert.conf");
        // then
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @AfterEach
    public void cleanUp() {
        if (neo4jSession != null) {
            neo4jSession.run("MATCH (n) DETACH DELETE n");
        }
    }

    @AfterAll
    public void close() {
        if (neo4jSession != null) {
            neo4jSession.close();
        }
        if (neo4jDriver != null) {
            neo4jDriver.close();
        }
        if (container != null) {
            container.close();
        }
    }
}
