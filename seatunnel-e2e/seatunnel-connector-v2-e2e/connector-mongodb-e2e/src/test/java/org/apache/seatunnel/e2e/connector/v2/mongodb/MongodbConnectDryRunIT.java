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

package org.apache.seatunnel.e2e.connector.v2.mongodb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.MongodbSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/** Tests authenticated metadata validation without submitting a job or starting an engine. */
@Timeout(60)
public class MongodbConnectDryRunIT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> mongo;
    private MongoClient admin;
    private String host;

    @BeforeAll
    @Override
    public void startUp() {
        mongo =
                new GenericContainer<>(DockerImageName.parse("mongo:8.0.11"))
                        .withEnv("MONGO_INITDB_ROOT_USERNAME", "admin")
                        .withEnv("MONGO_INITDB_ROOT_PASSWORD", "admin-password")
                        .withExposedPorts(27017)
                        .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 2))
                        .withStartupTimeout(Duration.ofMinutes(2));
        mongo.start();
        host = mongo.getHost() + ":" + mongo.getMappedPort(27017);
        admin =
                MongoClients.create(
                        "mongodb://admin:admin-password@" + host + "/?authSource=admin");
        MongoDatabase database = admin.getDatabase("dry_run");
        database.createCollection("events");
        database.createCollection("hidden");
        database.getCollection("events").insertOne(new Document("value", "unchanged"));
        database.runCommand(
                new Document("createRole", "events_reader")
                        .append(
                                "privileges",
                                Collections.singletonList(
                                        new Document(
                                                        "resource",
                                                        new Document("db", "dry_run")
                                                                .append("collection", "events"))
                                                .append(
                                                        "actions",
                                                        Collections.singletonList("find"))))
                        .append("roles", Collections.emptyList()));
        database.runCommand(
                new Document("createUser", "reader")
                        .append("pwd", "reader-password")
                        .append("roles", Collections.singletonList("events_reader")));
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (admin != null) {
                admin.close();
            }
        } finally {
            if (mongo != null) {
                mongo.stop();
            }
        }
    }

    @Test
    void shouldValidateWithOnlyCollectionReadPrivilegeWithoutReadingDocuments() throws Exception {
        List<Document> commands = new CopyOnWriteArrayList<>();
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(uri("reader-password")))
                        .addCommandListener(
                                new CommandListener() {
                                    @Override
                                    public void commandStarted(CommandStartedEvent event) {
                                        commands.add(
                                                new Document("name", event.getCommandName())
                                                        .append("database", event.getDatabaseName())
                                                        .append(
                                                                "command",
                                                                Document.parse(
                                                                        event.getCommand()
                                                                                .toJson())));
                                    }
                                })
                        .build();
        // Capture commands on a real authenticated connection. Unit tests separately assert the
        // settings passed by the factory; only client construction is intercepted here.
        MongoClient observed = MongoClients.create(settings);
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(observed);
            validate(uri("reader-password"), "events");
        } finally {
            observed.close();
        }
        assertEquals(
                1,
                commands.stream()
                        .filter(event -> event.getString("name").equals("listCollections"))
                        .count());
        assertTrue(
                commands.stream()
                        .allMatch(
                                event ->
                                        Arrays.asList(
                                                        "listCollections",
                                                        "killCursors",
                                                        "endSessions")
                                                .contains(event.getString("name"))));
        Document metadata =
                commands.stream()
                        .filter(event -> event.getString("name").equals("listCollections"))
                        .findFirst()
                        .get();
        assertEquals("dry_run", metadata.getString("database"));
        assertEquals(
                "events",
                metadata.get("command", Document.class)
                        .get("filter", Document.class)
                        .getString("name"));
        assertTrue(metadata.get("command", Document.class).getBoolean("nameOnly"));
        assertTrue(metadata.get("command", Document.class).getBoolean("authorizedCollections"));
        assertEquals(1, admin.getDatabase("dry_run").getCollection("events").countDocuments());
        assertEquals(
                "unchanged",
                admin.getDatabase("dry_run")
                        .getCollection("events")
                        .find()
                        .first()
                        .getString("value"));
    }

    @Test
    void shouldRejectInvisibleAndMissingCollections() {
        for (String collection : new String[] {"hidden", "absent"}) {
            IllegalStateException error =
                    assertThrows(
                            IllegalStateException.class,
                            () -> validate(uri("reader-password"), collection));
            assertEquals(
                    "Configured MongoDB collection does not exist or is not visible to the configured user",
                    error.getMessage());
            assertNull(error.getCause());
            assertEquals(0, error.getSuppressed().length);
        }
    }

    @Test
    void shouldRejectWrongCredentialsWithoutExposingTheirValue() {
        IllegalStateException error =
                assertThrows(
                        IllegalStateException.class,
                        () -> validate(uri("invalid-secret"), "events"));
        assertFalse(error.toString().contains("invalid-secret"));
        assertNull(error.getCause());
    }

    @Test
    void shouldAcceptAnExistingEmptyCollection() throws Exception {
        validate("mongodb://admin:admin-password@" + host + "/?authSource=admin", "hidden");
    }

    @Test
    @Timeout(5)
    void shouldHonorShortServerSelectionTimeoutForAnUnresponsiveEndpoint() throws Exception {
        // A listening socket with no MongoDB handshake response cannot become a selected server.
        try (ServerSocket endpoint = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            IllegalStateException error =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    validate(
                                            "mongodb://127.0.0.1:"
                                                    + endpoint.getLocalPort()
                                                    + "/?serverSelectionTimeoutMS=200&connectTimeoutMS=100&socketTimeoutMS=100",
                                            "events"));
            assertEquals("MongoDB connect dry-run connection timed out", error.getMessage());
        }
    }

    private String uri(String password) {
        return "mongodb://reader:"
                + password
                + "@"
                + host
                + "/?authSource=dry_run&serverSelectionTimeoutMS=2000&connectTimeoutMS=2000&socketTimeoutMS=2000";
    }

    private void validate(String uri, String collection) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("uri", uri);
        config.put("database", "dry_run");
        config.put("collection", collection);
        config.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("value", "string")));
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(config), getClass().getClassLoader());
        MongodbSourceFactory factory = new MongodbSourceFactory();
        factory.validateConnectionForDryRun(context, factory.inferSchemaForDryRun(context));
    }
}
