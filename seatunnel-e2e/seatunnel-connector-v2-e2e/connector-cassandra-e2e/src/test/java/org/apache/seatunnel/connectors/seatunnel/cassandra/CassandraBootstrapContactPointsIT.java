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

package org.apache.seatunnel.connectors.seatunnel.cassandra;

import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.e2e.common.TestResource;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.lifecycle.Startables;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Verifies that one unreachable bootstrap contact point never hides the reachable seeds.
 *
 * <p>This runs as its own integration test on its own container, because the shared Cassandra
 * pipeline job must keep a single reachable seed: putting an unreachable seed into that shared job
 * configuration makes the pipeline run depend on driver-side failover and destabilized the
 * connector shard.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CassandraBootstrapContactPointsIT implements TestResource {
    private static final String CASSANDRA_DOCKER_IMAGE = "cassandra:4.1.1";
    private static final int PORT = 9042;
    private static final String KEYSPACE_NAME = "bootstrap_contact_points";
    private static final String SEED_TABLE_NAME = "bootstrap_seed";

    private CassandraContainer<?> container;

    @BeforeAll
    @Override
    public void startUp() {
        container =
                new CassandraContainer<>(CASSANDRA_DOCKER_IMAGE)
                        .withStartupTimeout(Duration.ofMinutes(3));
        Startables.deepStart(Stream.of(container)).join();
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(180L, TimeUnit.SECONDS)
                .untilAsserted(this::prepareSeedTable);
    }

    @Test
    public void connectsThroughTheReachableSeedInEitherOrder() {
        String reachable = container.getHost() + ":" + container.getMappedPort(PORT);
        // A closed port on the reachable host fails fast, so the unreachable seed stays cheap.
        String unreachable = container.getHost() + ":1";
        for (String seeds :
                Arrays.asList(unreachable + "," + reachable, reachable + "," + unreachable)) {
            try (CqlSession session =
                    CassandraClient.getCqlSessionBuilder(
                                    seeds, KEYSPACE_NAME, "", "", container.getLocalDatacenter())
                            .build()) {
                Assertions.assertEquals(
                        KEYSPACE_NAME, session.getKeyspace().orElseThrow().asInternal());
                Row row =
                        session.execute(
                                        SimpleStatement.builder("SELECT id FROM " + SEED_TABLE_NAME)
                                                .setKeyspace(KEYSPACE_NAME)
                                                .build())
                                .one();
                Assertions.assertNotNull(row, "A reachable seed must still expose the table");
                Assertions.assertEquals(1, row.getInt("id"));
            }
        }
    }

    /** Creates a reachable, non-empty table so a connected session can be asserted. */
    private void prepareSeedTable() {
        try (CqlSession session =
                CqlSession.builder()
                        .addContactPoint(
                                new InetSocketAddress(
                                        container.getHost(), container.getMappedPort(PORT)))
                        .withLocalDatacenter(container.getLocalDatacenter())
                        .build()) {
            session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS "
                            + KEYSPACE_NAME
                            + " WITH replication = {'class':'SimpleStrategy','replication_factor':'1'}");
            session.execute(
                    SimpleStatement.builder(
                                    "CREATE TABLE IF NOT EXISTS "
                                            + SEED_TABLE_NAME
                                            + " (id int PRIMARY KEY)")
                            .setKeyspace(KEYSPACE_NAME)
                            .build());
            session.execute(
                    SimpleStatement.builder("INSERT INTO " + SEED_TABLE_NAME + " (id) VALUES (1)")
                            .setKeyspace(KEYSPACE_NAME)
                            .build());
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (container != null) {
            container.close();
        }
    }
}
