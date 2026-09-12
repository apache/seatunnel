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

package org.apache.seatunnel.e2e.connector.nebulagraph;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NebulaGraphIT extends TestSuiteBase implements TestResource {

    private static final String VERSION = "v3.8.0";

    private GenericContainer<?> metad;
    private GenericContainer<?> storaged;
    private GenericContainer<?> graphd;
    private NebulaPool adminPool;
    private Session adminSession;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        metad =
                new GenericContainer<>(DockerImageName.parse("vesoft/nebula-metad:" + VERSION))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("metad0")
                        .withExposedPorts(19559)
                        .withCommand(
                                "--meta_server_addrs=metad0:9559",
                                "--local_ip=metad0",
                                "--ws_ip=metad0",
                                "--port=9559",
                                "--ws_http_port=19559",
                                "--data_path=/data/meta",
                                "--logtostderr=true")
                        .waitingFor(Wait.forHttp("/status").forPort(19559))
                        .withStartupTimeout(Duration.ofMinutes(3));
        storaged =
                new GenericContainer<>(DockerImageName.parse("vesoft/nebula-storaged:" + VERSION))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("storaged0")
                        .withCommand(
                                "--meta_server_addrs=metad0:9559",
                                "--local_ip=storaged0",
                                "--ws_ip=storaged0",
                                "--port=9779",
                                "--ws_http_port=19779",
                                "--data_path=/data/storage",
                                "--logtostderr=true");
        graphd =
                new GenericContainer<>(DockerImageName.parse("vesoft/nebula-graphd:" + VERSION))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("graphd")
                        .withExposedPorts(9669, 19669)
                        .withCommand(
                                "--meta_server_addrs=metad0:9559",
                                "--local_ip=graphd",
                                "--ws_ip=graphd",
                                "--port=9669",
                                "--ws_http_port=19669",
                                "--logtostderr=true")
                        .waitingFor(Wait.forHttp("/status").forPort(19669))
                        .withStartupTimeout(Duration.ofMinutes(3));

        metad.start();
        // Storaged waits for host registration, so it cannot use an HTTP startup wait here.
        storaged.start();
        graphd.start();

        adminPool = new NebulaPool();
        NebulaPoolConfig poolConfig = new NebulaPoolConfig().setMaxConnSize(1).setTimeout(30000);
        assertTrue(
                adminPool.init(
                        Arrays.asList(
                                new HostAddress(graphd.getHost(), graphd.getMappedPort(9669))),
                        poolConfig));
        adminSession = adminPool.getSession("root", "nebula", false);
        execute("ADD HOSTS \"storaged0\":9779");

        Awaitility.await()
                .atMost(90, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(() -> execute("SHOW HOSTS").toString().contains("ONLINE"));

        execute("CREATE SPACE IF NOT EXISTS seatunnel(vid_type = FIXED_STRING(64))");
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> succeeds("USE seatunnel"));
        execute("CREATE TAG IF NOT EXISTS person(name string, age int)");
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> succeeds("DESCRIBE TAG person"));
    }

    @TestTemplate
    public void writesAndUpdatesVertices(TestContainer container)
            throws IOException, InterruptedException {
        execute("DELETE VERTEX \"person-1\", \"person-2\"");

        Container.ExecResult insert =
                container.executeJob("/nebulagraph/fake_to_nebulagraph_insert.conf");
        assertEquals(0, insert.getExitCode(), insert.getStderr());

        ResultSet inserted =
                execute(
                        "FETCH PROP ON person \"person-1\" "
                                + "YIELD properties(vertex).name AS name, properties(vertex).age AS age");
        assertEquals("Alice", inserted.rowValues(0).get("name").asString());
        assertEquals(30L, inserted.rowValues(0).get("age").asLong());

        Container.ExecResult update =
                container.executeJob("/nebulagraph/fake_to_nebulagraph_update.conf");
        assertEquals(0, update.getExitCode(), update.getStderr());

        ResultSet updated =
                execute(
                        "FETCH PROP ON person \"person-1\" "
                                + "YIELD properties(vertex).age AS age");
        assertEquals(31L, updated.rowValues(0).get("age").asLong());
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (adminSession != null) {
            adminSession.release();
        }
        if (adminPool != null) {
            adminPool.close();
        }
        if (graphd != null) {
            graphd.stop();
        }
        if (storaged != null) {
            storaged.stop();
        }
        if (metad != null) {
            metad.stop();
        }
    }

    private ResultSet execute(String statement) {
        try {
            ResultSet result = adminSession.execute(statement);
            if (!result.isSucceeded()) {
                throw new IllegalStateException(
                        "NebulaGraph command failed: " + result.getErrorMessage());
            }
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to execute NebulaGraph command.", e);
        }
    }

    private boolean succeeds(String statement) {
        try {
            return adminSession.execute(statement).isSucceeded();
        } catch (Exception e) {
            return false;
        }
    }
}
