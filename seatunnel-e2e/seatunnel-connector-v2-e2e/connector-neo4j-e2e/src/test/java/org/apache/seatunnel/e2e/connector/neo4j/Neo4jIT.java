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

package org.apache.seatunnel.e2e.connector.neo4j;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class Neo4jIT extends TestSuiteBase implements TestResource {

    private static final String CONTAINER_IMAGE = "neo4j:latest";
    private static final String CONTAINER_HOST = "neo4j-host";
    private static final int HTTP_PORT = 7474;
    private static final int BOLT_PORT = 7687;
    private static final String CONTAINER_NEO4J_USERNAME = "neo4j";
    private static final String CONTAINER_NEO4J_PASSWORD = "1234";
    private static final URI CONTAINER_URI = URI.create("neo4j://localhost:" + BOLT_PORT);

    private GenericContainer<?> container;
    private Driver neo4jDriver;
    private Session neo4jSession;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        DockerImageName imageName = DockerImageName.parse(CONTAINER_IMAGE);
        container = new GenericContainer<>(imageName)
            .withNetwork(NETWORK)
            .withNetworkAliases(CONTAINER_HOST)
            .withExposedPorts(HTTP_PORT, BOLT_PORT)
            .withEnv("NEO4J_AUTH", CONTAINER_NEO4J_USERNAME + "/" + CONTAINER_NEO4J_PASSWORD)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(CONTAINER_IMAGE)));
        container.setPortBindings(
            Lists.newArrayList(String.format("%s:%s", HTTP_PORT, HTTP_PORT),
                String.format("%s:%s", BOLT_PORT, BOLT_PORT)));
        Startables.deepStart(Stream.of(container)).join();
        log.info("container started");
        Awaitility.given().ignoreExceptions()
            .await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(this::initConnection);
    }

    private void initConnection() {
        neo4jDriver =
            GraphDatabase.driver(CONTAINER_URI, AuthTokens.basic(CONTAINER_NEO4J_USERNAME, CONTAINER_NEO4J_PASSWORD));
        neo4jSession = neo4jDriver.session(SessionConfig.forDatabase("neo4j"));
    }

    @TestTemplate
    public void test(TestContainer container) throws IOException, InterruptedException {
        // clean test data before test
        final Result checkExists = neo4jSession.run("MATCH (tt:TestTest) RETURN tt");
        if (checkExists.hasNext()) {
            neo4jSession.run("MATCH (tt:TestTest) delete tt");
        }

        final Result checkExistsT = neo4jSession.run("MATCH (t:Test) RETURN t");
        if (checkExistsT.hasNext()) {
            neo4jSession.run("MATCH (t:Test) delete t");
        }

        // given
        neo4jSession.run(
            "CREATE (t:Test {string:'foo', boolean:true, long:2147483648, double:1.7976931348623157E308, " +
                "byteArray:$byteArray, date:date('2022-10-07'), localTime:localtime('20:04:00'), localDateTime:localdatetime('2022-10-07T20:04:00'), " +
                "list:[0, 1], int:2147483647, float:$float})",
            parameters("byteArray", new byte[]{(byte) 1}, "float", Float.MAX_VALUE)
        );
        // when
        Container.ExecResult execResult = container.executeJob("/neo4j/neo4j_to_neo4j.conf");
        // then
        Assertions.assertEquals(0, execResult.getExitCode());

        final Result result = neo4jSession.run("MATCH (tt:TestTest) RETURN tt");
        final Node tt = result.single().get("tt").asNode();

        assertEquals("foo", tt.get("string").asString());
        assertTrue(tt.get("boolean").asBoolean());
        assertEquals(2147483648L, tt.get("long").asLong());
        assertEquals(Double.MAX_VALUE, tt.get("double").asDouble());
        assertArrayEquals(new byte[]{(byte) 1}, tt.get("byteArray").asByteArray());
        assertEquals(LocalDate.parse("2022-10-07"), tt.get("date").asLocalDate());
        assertEquals(LocalDateTime.parse("2022-10-07T20:04:00"), tt.get("localDateTime").asLocalDateTime());
        final ArrayList<Integer> expectedList = new ArrayList<>();
        expectedList.add(0);
        expectedList.add(1);
        assertTrue(tt.get("list").asList(Value::asInt).containsAll(expectedList));
        assertEquals(2147483647, tt.get("int").asInt());
        assertEquals(2147483647, tt.get("mapValue").asInt());
        assertEquals(Float.MAX_VALUE, tt.get("float").asFloat());
    }

    @AfterAll
    @Override
    public void tearDown() {
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
