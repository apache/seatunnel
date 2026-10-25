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
package org.apache.seatunnel.e2e.connector.fluss;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypes;
import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

// A Fluss table without a bucket leader makes the SeaTunnel job wait forever. Fail the case instead
// of blocking the whole CI job; the slowest case waits at most about 8 minutes when healthy.
@Timeout(value = 15, unit = TimeUnit.MINUTES)
@Slf4j
public abstract class FlussTestBase extends TestSuiteBase implements TestResource {

    protected static final String DOCKER_IMAGE = "fluss/fluss:0.7.0";
    protected static final String DOCKER_ZK_IMAGE = "zookeeper:3.9.2";

    protected static final String FLUSS_COORDINATOR_HOST = "fluss_coordinator_e2e";
    protected static final String FLUSS_TABLET_HOST = "fluss_tablet_e2e";
    protected static final String ZK_HOST = "zk_e2e";
    protected static final int ZK_PORT = 2181;
    protected static final int FLUSS_COORDINATOR_PORT = 9123;
    protected static final int FLUSS_TABLET_PORT = 9124;
    protected static final int FLUSS_COORDINATOR_LOCAL_PORT = 8123;
    protected static final int FLUSS_TABLET_LOCAL_PORT = 8124;
    private static final int TABLE_CREATE_ATTEMPTS = 3;

    private GenericContainer<?> zookeeperServer;
    private GenericContainer<?> coordinatorServer;
    private GenericContainer<?> tabletServer;

    protected Connection flussConnection;

    @BeforeAll
    @Override
    public void startUp() {
        createZookeeperContainer();
        createFlussContainer();
    }

    private void createFlussContainer() {
        log.info("Starting FlussServer container...");
        String coordinatorEnv = String.format("zookeeper.address: %s:%d", ZK_HOST, ZK_PORT);
        coordinatorServer =
                new DynamicFlussContainer(
                                FLUSS_COORDINATOR_HOST,
                                FLUSS_COORDINATOR_PORT,
                                FLUSS_COORDINATOR_LOCAL_PORT,
                                "coordinator-server.sh")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(FLUSS_COORDINATOR_HOST)
                        .withEnv("FLUSS_PROPERTIES", coordinatorEnv)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("coordinatorServer")));
        Startables.deepStart(Stream.of(coordinatorServer)).join();
        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(
                        () ->
                                checkPort(
                                        coordinatorServer.getHost(),
                                        coordinatorServer.getMappedPort(
                                                FLUSS_COORDINATOR_LOCAL_PORT),
                                        1000));
        log.info("coordinatorServer container start success");

        String tabletEnv =
                String.format(
                        "zookeeper.address: %s:%d\n"
                                + "tablet-server.id: 0\n"
                                + "kv.snapshot.interval: 0s\n"
                                + "data.dir: /tmp/fluss/data\n"
                                + "remote.data.dir: /tmp/fluss/remote-data",
                        ZK_HOST, ZK_PORT);
        tabletServer =
                new DynamicFlussContainer(
                                FLUSS_TABLET_HOST,
                                FLUSS_TABLET_PORT,
                                FLUSS_TABLET_LOCAL_PORT,
                                "tablet-server.sh")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(FLUSS_TABLET_HOST)
                        .withEnv("FLUSS_PROPERTIES", tabletEnv)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("tabletServer")));
        Startables.deepStart(Stream.of(tabletServer)).join();
        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(this::initializeConnection);
        log.info("tabletServer container start success");
        log.info("FlussServer Containers are started");
    }

    private void createZookeeperContainer() {
        log.info("Starting ZookeeperServer container...");
        zookeeperServer =
                new GenericContainer<>(DOCKER_ZK_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ZK_HOST)
                        .withExposedPorts(ZK_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(DOCKER_ZK_IMAGE)));
        Startables.deepStart(Stream.of(zookeeperServer)).join();
        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(
                        () ->
                                checkPort(
                                        zookeeperServer.getHost(),
                                        zookeeperServer.getMappedPort(ZK_PORT),
                                        1000));
        log.info("ZookeeperServer Containers are started");
    }

    private void initializeConnection() throws ExecutionException, InterruptedException {
        flussConnection = ConnectionFactory.createConnection(flussConfig());
        // Perform a real admin RPC so the readiness await only passes once the tablet server
        // actually answers, not merely when the (lazy) client object is constructed.
        flussConnection.getAdmin().listDatabases().get();
    }

    private Configuration flussConfig() {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                "bootstrap.servers",
                coordinatorServer.getHost()
                        + ":"
                        + coordinatorServer.getMappedPort(FLUSS_COORDINATOR_LOCAL_PORT));
        return flussConfig;
    }

    protected void createDb(Connection connection, String dbName)
            throws ExecutionException, InterruptedException {
        Admin admin = connection.getAdmin();
        DatabaseDescriptor descriptor = DatabaseDescriptor.builder().build();
        admin.dropDatabase(dbName, true, true).get();
        admin.createDatabase(dbName, descriptor, true).get();
    }

    protected Schema getFlussSchema() {
        return Schema.newBuilder()
                .column("fbytes", DataTypes.BYTES())
                .column("fboolean", DataTypes.BOOLEAN())
                .column("fint", DataTypes.INT())
                .column("ftinyint", DataTypes.TINYINT())
                .column("fsmallint", DataTypes.SMALLINT())
                .column("fbigint", DataTypes.BIGINT())
                .column("ffloat", DataTypes.FLOAT())
                .column("fdouble", DataTypes.DOUBLE())
                .column("fdecimal", DataTypes.DECIMAL(30, 8))
                .column("fstring", DataTypes.STRING())
                .column("fdate", DataTypes.DATE())
                .column("ftime", DataTypes.TIME())
                .column("ftimestamp", DataTypes.TIMESTAMP())
                .column("ftimestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                .primaryKey("fstring")
                .build();
    }

    protected void createTable(
            Connection connection, String dbName, String tableName, Schema schema)
            throws Exception {
        Admin admin = connection.getAdmin();
        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();
        TablePath tablePath = TablePath.of(dbName, tableName);
        // Fluss 0.7.0 can miss a table creation event (apache/fluss#1018). The table is then
        // registered but its buckets never get a leader, so recreate it.
        ConditionTimeoutException lastFailure = null;
        for (int attempt = 1; attempt <= TABLE_CREATE_ATTEMPTS; attempt++) {
            admin.dropTable(tablePath, true).get();
            admin.createTable(tablePath, tableDescriptor, true).get(); // blocking call
            try {
                waitForBucketLeaders(tablePath);
                return;
            } catch (ConditionTimeoutException e) {
                lastFailure = e;
                log.warn(
                        "Fluss table {} has no bucket leader (attempt {} of {})",
                        tablePath,
                        attempt,
                        TABLE_CREATE_ATTEMPTS,
                        e);
            }
        }
        throw new IllegalStateException(
                String.format(
                        "Fluss table %s has no bucket leader after %d attempts, see apache/fluss#1018",
                        tablePath, TABLE_CREATE_ATTEMPTS),
                lastFailure);
    }

    private void waitForBucketLeaders(TablePath tablePath) throws Exception {
        // Use a new connection so no metadata cached for an earlier table at this path is used.
        try (Connection connection = ConnectionFactory.createConnection(flussConfig())) {
            Admin admin = connection.getAdmin();
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            List<Integer> buckets =
                    IntStream.range(0, tableInfo.getNumBuckets())
                            .boxed()
                            .collect(Collectors.toList());
            // Listing offsets fails while a bucket has no leader.
            given().ignoreExceptions()
                    .await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    admin.listOffsets(
                                                    tablePath, buckets, new OffsetSpec.LatestSpec())
                                            .all()
                                            .get(10, TimeUnit.SECONDS));
        }
    }

    protected static boolean checkPort(String host, int port, int timeoutMs) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
            return true;
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (flussConnection != null) {
            flussConnection.close();
        }
        if (tabletServer != null) {
            tabletServer.close();
        }
        if (coordinatorServer != null) {
            coordinatorServer.close();
        }
        if (zookeeperServer != null) {
            zookeeperServer.close();
        }
    }

    private static final class DynamicFlussContainer
            extends GenericContainer<DynamicFlussContainer> {

        private static final String DYNAMIC_CONFIG_PATH = "/tmp/fluss-dynamic.properties";

        private final String internalHost;
        private final int internalPort;
        private final int localClientPort;

        private DynamicFlussContainer(
                String internalHost, int internalPort, int localClientPort, String serverScript) {
            super(DOCKER_IMAGE);
            this.internalHost = internalHost;
            this.internalPort = internalPort;
            this.localClientPort = localClientPort;
            withExposedPorts(localClientPort);
            withCommand(
                    "bash",
                    "-c",
                    "while [ ! -f "
                            + DYNAMIC_CONFIG_PATH
                            + " ]; do sleep 0.1; done; "
                            + "cat "
                            + DYNAMIC_CONFIG_PATH
                            + " >> \"$FLUSS_HOME/conf/server.yaml\"; "
                            + "exec \"$FLUSS_HOME/bin/"
                            + serverScript
                            + "\" start-foreground");
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo) {
            String dynamicConfig =
                    String.format(
                            "bind.listeners: INTERNAL://0.0.0.0:%d, LOCALCLIENT://0.0.0.0:%d\n"
                                    + "advertised.listeners: INTERNAL://%s:%d, LOCALCLIENT://%s:%d\n"
                                    + "internal.listener.name: INTERNAL\n",
                            internalPort,
                            localClientPort,
                            internalHost,
                            internalPort,
                            getHost(),
                            getMappedPort(localClientPort));
            copyFileToContainer(Transferable.of(dynamicConfig), DYNAMIC_CONFIG_PATH);
        }
    }
}
