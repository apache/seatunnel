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

package org.apache.seatunnel.e2e.connector.couchbase;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.couchbase.sink.CouchbaseSink;
import org.apache.seatunnel.connectors.seatunnel.couchbase.sink.CouchbaseSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.couchbase.sink.CouchbaseWriter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.utility.DockerImageName;

import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.java.Cluster;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Factory-level readiness tests; no engine containers are needed for this client contract. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SAME_THREAD)
@Slf4j
class CouchbaseReadinessIT {

    private final CouchbaseContainer server =
            new CouchbaseContainer(
                            DockerImageName.parse(
                                    System.getProperty(
                                            "couchbase.test.image",
                                            "couchbase/server:community-7.1.1")))
                    .withNetworkMode("bridge")
                    .withCredentials("Administrator", "password")
                    .withEnabledServices(CouchbaseService.KV)
                    .withBucket(
                            new BucketDefinition("readiness")
                                    .withQuota(128)
                                    .withReplicas(0)
                                    .withPrimaryIndex(false))
                    .withStartupTimeout(Duration.ofMinutes(3))
                    .withStartupAttempts(3)
                    .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("couchbase-readiness"));

    private Cluster verification;

    @BeforeAll
    void startServer() {
        server.start();
        verification =
                Cluster.connect(
                        server.getConnectionString(), server.getUsername(), server.getPassword());
        verification.bucket("readiness").waitUntilReady(Duration.ofSeconds(60));
    }

    @AfterAll
    void closeServer() {
        try {
            if (verification != null) {
                verification.disconnect();
            }
        } finally {
            server.stop();
        }
    }

    @Test
    void testReadinessTimeoutAndRecovery() throws Exception {
        CouchbaseSink unavailableSink = createSink(5, server.getPassword());
        server.getDockerClient().pauseContainerCmd(server.getContainerId()).exec();
        try {
            assertThrows(
                    UnambiguousTimeoutException.class, () -> unavailableSink.createWriter(null));
        } finally {
            server.getDockerClient().unpauseContainerCmd(server.getContainerId()).exec();
        }

        CouchbaseWriter writer = createSink(60, server.getPassword()).createWriter(null);
        try {
            writer.write(new SeaTunnelRow(new Object[] {"recovered"}));
            writer.prepareCommit();
            assertEquals(
                    "recovered",
                    verification
                            .bucket("readiness")
                            .defaultCollection()
                            .get("9:recovered")
                            .contentAsObject()
                            .getString("id"));
        } finally {
            writer.close();
        }
    }

    @Test
    void testInvalidCredentialsStillFailReadiness() {
        CouchbaseSink sink = createSink(5, "incorrect-password");
        assertThrows(UnambiguousTimeoutException.class, () -> sink.createWriter(null));
    }

    private CouchbaseSink createSink(int timeout, String password) {
        Map<String, Object> config = new HashMap<>();
        config.put("connection.string", server.getConnectionString());
        config.put("username", server.getUsername());
        config.put("password", password);
        config.put("bucket", "readiness");
        config.put("collection", "_default");
        config.put("primary-key", Collections.singletonList("id"));
        config.put("upsert-enable", true);
        config.put("ready.timeout", timeout);
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.STRING_TYPE, 64L, false, null, ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
        CouchbaseSinkFactory factory = new CouchbaseSinkFactory();
        ReadonlyConfig options = ReadonlyConfig.fromMap(config);
        ConfigValidator.of(options).validate(factory.optionRule());
        return (CouchbaseSink)
                factory.createSink(
                                new TableSinkFactoryContext(
                                        table, options, getClass().getClassLoader()))
                        .createSink();
    }
}
