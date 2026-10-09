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

package org.apache.seatunnel.connectors.seatunnel.mongodb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.MongodbSourceFactory;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class MongodbFactoryTest {

    private final MongodbSourceFactory sourceFactory = new MongodbSourceFactory();

    @Test
    void testSourceSupportsConnectivityDryRun() {
        Assertions.assertTrue(sourceFactory instanceof SupportSourceDryRunValidation);
    }

    @Test
    void testDryRunSchemaMatchesRuntimeWithoutConnecting() {
        for (boolean withSchema : new boolean[] {true, false}) {
            Map<String, Object> options = validSourceConfig();
            if (!withSchema) {
                options.remove(ConnectorCommonOptions.SCHEMA.key());
            }
            TableSourceFactoryContext context = context(options);
            try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
                CatalogTable runtime =
                        sourceFactory
                                .createSource(context)
                                .createSource()
                                .getProducedCatalogTables()
                                .get(0);
                CatalogTable dryRun = sourceFactory.inferSchemaForDryRun(context).get(0);
                Assertions.assertEquals(runtime.getTableId(), dryRun.getTableId());
                Assertions.assertEquals(
                        runtime.getSeaTunnelRowType(), dryRun.getSeaTunnelRowType());
                Assertions.assertEquals(runtime.getOptions(), dryRun.getOptions());
                clients.verifyNoInteractions();
            }
        }
    }

    @Test
    void testDryRunUsesOnlyFilteredMetadataAndClosesClient() throws Exception {
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        when(client.getDatabase("test_database")).thenReturn(database);
        when(database.runCommand(any(Document.class), any(ReadPreference.class)))
                .thenReturn(
                        new Document(
                                "cursor",
                                new Document("id", 0L)
                                        .append(
                                                "firstBatch",
                                                Collections.singletonList(
                                                        new Document("name", "test_collection")))));
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(client);
            validate(validSourceConfig());
            verify(database)
                    .runCommand(
                            new Document("listCollections", 1)
                                    .append("filter", new Document("name", "test_collection"))
                                    .append("nameOnly", true)
                                    .append("authorizedCollections", true)
                                    .append("cursor", new Document("batchSize", 2))
                                    .append("maxTimeMS", 30000),
                            ReadPreference.primary());
            verify(client).getDatabase("test_database");
            verify(client).close();
            verifyNoMoreInteractions(client, database);
        }
    }

    @Test
    void testDryRunPreservesUriSettingsAndCapsTimeouts() throws Exception {
        Map<String, Object> options = validSourceConfig();
        options.put(
                "uri",
                "mongodb://user:password@localhost:27017/?authSource=admin&tls=true&readPreference=secondaryPreferred&connectTimeoutMS=500&socketTimeoutMS=0&serverSelectionTimeoutMS=120000&waitQueueTimeoutMS=100&minPoolSize=5&maxPoolSize=10");
        MongoClient client = mock(MongoClient.class);
        when(client.getDatabase(any())).thenThrow(new IllegalStateException());
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenAnswer(
                            invocation -> {
                                MongoClientSettings settings = invocation.getArgument(0);
                                Assertions.assertEquals(
                                        "user", settings.getCredential().getUserName());
                                Assertions.assertEquals(
                                        "admin", settings.getCredential().getSource());
                                Assertions.assertTrue(settings.getSslSettings().isEnabled());
                                Assertions.assertEquals(
                                        ReadPreference.secondaryPreferred(),
                                        settings.getReadPreference());
                                Assertions.assertEquals(
                                        500,
                                        settings.getSocketSettings()
                                                .getConnectTimeout(TimeUnit.MILLISECONDS));
                                Assertions.assertEquals(
                                        30000,
                                        settings.getSocketSettings()
                                                .getReadTimeout(TimeUnit.MILLISECONDS));
                                Assertions.assertEquals(
                                        30000,
                                        settings.getClusterSettings()
                                                .getServerSelectionTimeout(TimeUnit.MILLISECONDS));
                                Assertions.assertEquals(
                                        100,
                                        settings.getConnectionPoolSettings()
                                                .getMaxWaitTime(TimeUnit.MILLISECONDS));
                                Assertions.assertEquals(
                                        0, settings.getConnectionPoolSettings().getMinSize());
                                Assertions.assertEquals(
                                        1, settings.getConnectionPoolSettings().getMaxSize());
                                return client;
                            });
            Assertions.assertThrows(IllegalStateException.class, () -> validate(options));
            verify(client).close();
        }
    }

    @Test
    void testDryRunRejectsMissingCollectionAndClosesClient() {
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        when(client.getDatabase(any())).thenReturn(database);
        when(database.runCommand(any(Document.class), any(ReadPreference.class)))
                .thenReturn(
                        new Document(
                                "cursor",
                                new Document("id", 0L)
                                        .append("firstBatch", Collections.emptyList())));
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(client);
            IllegalStateException error =
                    Assertions.assertThrows(
                            IllegalStateException.class, () -> validate(validSourceConfig()));
            Assertions.assertEquals(
                    "Configured MongoDB collection does not exist or is not visible to the configured user",
                    error.getMessage());
            Assertions.assertNull(error.getCause());
            Assertions.assertEquals(0, error.getSuppressed().length);
            verify(client).close();
        }
    }

    @Test
    void testMissingCollectionCleanupFailuresRemainSanitized() {
        for (boolean cursorFailure : new boolean[] {false, true}) {
            String scenario = "cursorFailure=" + cursorFailure;
            MongoClient client = mock(MongoClient.class);
            MongoDatabase database = mock(MongoDatabase.class);
            when(client.getDatabase(any())).thenReturn(database);
            Document listCollections =
                    new Document("listCollections", 1)
                            .append("filter", new Document("name", "test_collection"))
                            .append("nameOnly", true)
                            .append("authorizedCollections", true)
                            .append("cursor", new Document("batchSize", 2))
                            .append("maxTimeMS", 30_000);
            Document killCursors =
                    new Document("killCursors", "$cmd.listCollections")
                            .append("cursors", Collections.singletonList(123L));
            when(database.runCommand(eq(listCollections), eq(ReadPreference.primary())))
                    .thenReturn(
                            new Document(
                                    "cursor",
                                    new Document("id", cursorFailure ? 123L : 0L)
                                            .append("firstBatch", Collections.emptyList())));
            when(database.runCommand(eq(killCursors), eq(ReadPreference.primary())))
                    .thenThrow(new IllegalStateException("cursor secret"));
            doThrow(new IllegalStateException("close secret")).when(client).close();
            try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
                clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                        .thenReturn(client);
                IllegalStateException error =
                        Assertions.assertThrows(
                                IllegalStateException.class,
                                () -> validate(validSourceConfig()),
                                scenario);
                Assertions.assertEquals(
                        "MongoDB connect dry-run could not validate the configured collection. Check the URI, database, collection, metadata permissions and MongoDB 4.0+ support.",
                        error.getMessage(),
                        scenario);
                Assertions.assertNull(error.getCause(), scenario);
                Assertions.assertEquals(0, error.getSuppressed().length, scenario);
                verify(database).runCommand(listCollections, ReadPreference.primary());
                if (cursorFailure) {
                    verify(database).runCommand(killCursors, ReadPreference.primary());
                }
                verifyNoMoreInteractions(database);
                verify(client).close();
            }
        }
    }

    @Test
    void testDryRunDoesNotExposeDriverFailuresOrSuppressedCloseFailures() {
        MongoClient client = mock(MongoClient.class);
        when(client.getDatabase(any()))
                .thenThrow(
                        new MongoSecurityException(
                                MongoCredential.createCredential(
                                        "user", "admin", "secret".toCharArray()),
                                "mongodb://user:secret@host",
                                new RuntimeException("nested secret")));
        doThrow(new RuntimeException("close secret")).when(client).close();
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(client);
            IllegalStateException error =
                    Assertions.assertThrows(
                            IllegalStateException.class, () -> validate(validSourceConfig()));
            Assertions.assertEquals(
                    "MongoDB connect dry-run authentication failed", error.getMessage());
            Assertions.assertNull(error.getCause());
            Assertions.assertEquals(0, error.getSuppressed().length);
        }
        Map<String, Object> options = validSourceConfig();
        options.put("uri", "mongodb://user:secret@host:invalid-port");
        IllegalStateException error =
                Assertions.assertThrows(IllegalStateException.class, () -> validate(options));
        Assertions.assertFalse(error.toString().contains("secret"));
        Assertions.assertNull(error.getCause());
    }

    @Test
    void testDryRunHonorsInterruptionBeforeOpeningClient() {
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            Thread.currentThread().interrupt();
            try {
                Assertions.assertThrows(
                        InterruptedException.class, () -> validate(validSourceConfig()));
                Assertions.assertTrue(Thread.currentThread().isInterrupted());
                clients.verifyNoInteractions();
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void testDryRunClosesClientOnDriverInterruption() {
        MongoClient client = mock(MongoClient.class);
        when(client.getDatabase(any()))
                .thenThrow(
                        new MongoInterruptedException(
                                "unsafe details", new InterruptedException()));
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(client);
            try {
                InterruptedException error =
                        Assertions.assertThrows(
                                InterruptedException.class, () -> validate(validSourceConfig()));
                Assertions.assertTrue(Thread.currentThread().isInterrupted());
                Assertions.assertNull(error.getCause());
                verify(client).close();
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void testDryRunClosesClientOnTimeout() {
        MongoClient client = mock(MongoClient.class);
        when(client.getDatabase(any())).thenThrow(new MongoTimeoutException("unsafe details"));
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(client);
            IllegalStateException error =
                    Assertions.assertThrows(
                            IllegalStateException.class, () -> validate(validSourceConfig()));
            Assertions.assertEquals(
                    "MongoDB connect dry-run connection timed out", error.getMessage());
            Assertions.assertNull(error.getCause());
            verify(client).close();
        }
    }

    @Test
    void testDryRunReleasesUnexpectedServerCursorWithoutFetchingMore() throws Exception {
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        when(client.getDatabase("test_database")).thenReturn(database);
        when(database.runCommand(any(Document.class), any(ReadPreference.class)))
                .thenReturn(
                        new Document(
                                "cursor",
                                new Document("id", 123L)
                                        .append(
                                                "firstBatch",
                                                Collections.singletonList(
                                                        new Document("name", "test_collection")))));
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(client);
            validate(validSourceConfig());
            verify(database)
                    .runCommand(
                            new Document("killCursors", "$cmd.listCollections")
                                    .append("cursors", Collections.singletonList(123L)),
                            ReadPreference.primary());
            verify(client).close();
        }
    }

    private TableSourceFactoryContext context(Map<String, Object> options) {
        return new TableSourceFactoryContext(
                ReadonlyConfig.fromMap(options), getClass().getClassLoader());
    }

    private void validate(Map<String, Object> options) throws Exception {
        TableSourceFactoryContext context = context(options);
        sourceFactory.validateConnectionForDryRun(
                context, sourceFactory.inferSchemaForDryRun(context));
    }

    @Test
    void testSinkSupportsConnectivityDryRun() {
        Assertions.assertTrue(new MongodbSinkFactory() instanceof SupportSinkDryRunValidation);
    }

    @Test
    void testSinkDryRunOnlyPingsAndClosesForEverySaveMode() {
        for (DataSaveMode saveMode :
                Arrays.asList(
                        DataSaveMode.APPEND_DATA,
                        DataSaveMode.DROP_DATA,
                        DataSaveMode.ERROR_WHEN_DATA_EXISTS)) {
            MongoClient client = mock(MongoClient.class);
            MongoDatabase database = mock(MongoDatabase.class);
            when(client.getDatabase("test_database")).thenReturn(database);
            Map<String, Object> config = validSinkConfig("mongodb://localhost:27017");
            config.put(MongodbSinkOptions.DATA_SAVE_MODE.key(), saveMode.name());
            config.put(MongodbSinkOptions.TRANSACTION.key(), true);
            try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
                clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                        .thenReturn(client);
                new MongodbSinkFactory().validateConnectionForDryRun(sinkContext(config));
                clients.verify(() -> MongoClients.create(any(MongoClientSettings.class)));
                clients.verifyNoMoreInteractions();
            }
            verify(client).getDatabase("test_database");
            verify(database).runCommand(new Document("ping", 1), ReadPreference.primary());
            verify(client).close();
            verifyNoMoreInteractions(client, database);
        }
    }

    @Test
    void testSinkDryRunPreservesUriOptionsAndCapsTimeouts() {
        String uri =
                "mongodb://user:secret@localhost:27017/authdb?authSource=admin"
                        + "&authMechanism=SCRAM-SHA-256&tls=true&replicaSet=rs0"
                        + "&readPreference=secondary&w=majority&retryWrites=false"
                        + "&connectTimeoutMS=1000&socketTimeoutMS=60000"
                        + "&serverSelectionTimeoutMS=0&waitQueueTimeoutMS=70000"
                        + "&minPoolSize=5&maxPoolSize=10&appName=dry-run-test";
        MongoClientSettings settings = capturedSinkSettings(uri);
        MongoClientSettings configured =
                MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(uri))
                        .build();
        Assertions.assertEquals(configured.getCredential(), settings.getCredential());
        Assertions.assertEquals(configured.getSslSettings(), settings.getSslSettings());
        Assertions.assertEquals(configured.getReadPreference(), settings.getReadPreference());
        Assertions.assertEquals(configured.getWriteConcern(), settings.getWriteConcern());
        Assertions.assertEquals(configured.getRetryWrites(), settings.getRetryWrites());
        Assertions.assertEquals(configured.getApplicationName(), settings.getApplicationName());
        Assertions.assertEquals("rs0", settings.getClusterSettings().getRequiredReplicaSetName());
        Assertions.assertEquals(
                1000, settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        Assertions.assertEquals(
                30000, settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        Assertions.assertEquals(
                30000,
                settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
        Assertions.assertEquals(
                30000, settings.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
        Assertions.assertEquals(0, settings.getConnectionPoolSettings().getMinSize());
        Assertions.assertEquals(1, settings.getConnectionPoolSettings().getMaxSize());
    }

    @Test
    void testSinkDryRunPreservesShortTimeoutsAndSupportsNoCredentials() {
        MongoClientSettings settings =
                capturedSinkSettings(
                        "mongodb://localhost:27017/?connectTimeoutMS=100&socketTimeoutMS=200"
                                + "&serverSelectionTimeoutMS=300&waitQueueTimeoutMS=400");
        Assertions.assertNull(settings.getCredential());
        Assertions.assertEquals(
                100, settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        Assertions.assertEquals(
                200, settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        Assertions.assertEquals(
                300,
                settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
        Assertions.assertEquals(
                400, settings.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
        MongoClientSettings defaults = capturedSinkSettings("mongodb://localhost:27017");
        Assertions.assertEquals(
                30000, defaults.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    void testSinkDryRunSanitizesAuthenticationAndCleanupFailures() {
        assertSinkFailure(
                new MongoSecurityException(
                        new ConnectionString("mongodb://user:secret@localhost").getCredential(),
                        "secret"),
                true,
                "MongoDB sink dry-run authentication failed.");
    }

    @Test
    void testSinkDryRunSanitizesTimeoutAndCleanupFailures() {
        assertSinkFailure(
                new MongoTimeoutException("secret"),
                true,
                "MongoDB sink dry-run connection timed out.");
    }

    @Test
    void testSinkDryRunSanitizesCommandAndCleanupFailures() {
        assertSinkFailure(
                new IllegalArgumentException("secret"),
                true,
                "MongoDB sink dry-run connection failed. Check the URI, network and TLS settings.");
    }

    @Test
    void testSinkDryRunSanitizesCloseFailureAfterSuccess() {
        assertSinkFailure(
                null,
                true,
                "MongoDB sink dry-run connection failed. Check the URI, network and TLS settings.");
    }

    @Test
    void testSinkDryRunSanitizesClientCreationFailure() {
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenThrow(new IllegalArgumentException("secret"));
            IllegalStateException failure =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    new MongodbSinkFactory()
                                            .validateConnectionForDryRun(
                                                    sinkContext(
                                                            validSinkConfig(
                                                                    "mongodb://user:secret@localhost"))));
            assertSanitized(
                    failure,
                    "MongoDB sink dry-run connection failed. Check the URI, network and TLS settings.");
        }
    }

    @Test
    void testSinkDryRunSanitizesMalformedUri() {
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            IllegalStateException failure =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    new MongodbSinkFactory()
                                            .validateConnectionForDryRun(
                                                    sinkContext(validSinkConfig("secret"))));
            assertSanitized(
                    failure,
                    "MongoDB sink dry-run connection failed. Check the URI, network and TLS settings.");
            clients.verifyNoInteractions();
        }
    }

    @Test
    void testSinkDryRunPreservesInterruptionDuringConnection() {
        try {
            assertSinkFailure(
                    new MongoInterruptedException("secret", new InterruptedException("secret")),
                    true,
                    "MongoDB sink dry-run was interrupted.");
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void testSinkDryRunRejectsAlreadyInterruptedCallWithoutClient() {
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            Thread.currentThread().interrupt();
            IllegalStateException failure =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    new MongodbSinkFactory()
                                            .validateConnectionForDryRun(
                                                    sinkContext(
                                                            validSinkConfig(
                                                                    "mongodb://localhost"))));
            assertSanitized(failure, "MongoDB sink dry-run was interrupted.");
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
            clients.verifyNoInteractions();
        } finally {
            Thread.interrupted();
        }
    }

    private MongoClientSettings capturedSinkSettings(String uri) {
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        when(client.getDatabase("test_database")).thenReturn(database);
        AtomicReference<MongoClientSettings> settings = new AtomicReference<>();
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenAnswer(
                            invocation -> {
                                settings.set(invocation.getArgument(0));
                                return client;
                            });
            new MongodbSinkFactory().validateConnectionForDryRun(sinkContext(validSinkConfig(uri)));
        }
        verify(client).close();
        return settings.get();
    }

    private void assertSinkFailure(
            RuntimeException commandFailure, boolean closeFailure, String expectedMessage) {
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        when(client.getDatabase("test_database")).thenReturn(database);
        if (commandFailure != null) {
            when(database.runCommand(new Document("ping", 1), ReadPreference.primary()))
                    .thenThrow(commandFailure);
        }
        if (closeFailure) {
            doThrow(new IllegalStateException("close secret")).when(client).close();
        }
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(client);
            IllegalStateException failure =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    new MongodbSinkFactory()
                                            .validateConnectionForDryRun(
                                                    sinkContext(
                                                            validSinkConfig(
                                                                    "mongodb://user:secret@localhost"))));
            assertSanitized(failure, expectedMessage);
        }
        verify(client).getDatabase("test_database");
        verify(database).runCommand(new Document("ping", 1), ReadPreference.primary());
        verify(client).close();
        verifyNoMoreInteractions(client, database);
    }

    private void assertSanitized(IllegalStateException failure, String expectedMessage) {
        Assertions.assertEquals(expectedMessage, failure.getMessage());
        Assertions.assertNull(failure.getCause());
        Assertions.assertEquals(0, failure.getSuppressed().length);
        Assertions.assertFalse(failure.toString().contains("secret"));
    }

    private Map<String, Object> validSinkConfig(String uri) {
        Map<String, Object> config = new HashMap<>();
        config.put(MongodbSinkOptions.URI.key(), uri);
        config.put(MongodbSinkOptions.DATABASE.key(), "test_database");
        config.put(MongodbSinkOptions.COLLECTION.key(), "new_collection");
        return config;
    }

    private TableSinkFactoryContext sinkContext(Map<String, Object> config) {
        return new TableSinkFactoryContext(
                null, ReadonlyConfig.fromMap(config), getClass().getClassLoader());
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull(sourceFactory.optionRule());
        Assertions.assertNotNull(new MongodbSinkFactory().optionRule());
    }

    @Test
    void testDefaultFetchSizePassesOptionValidation() {
        Assertions.assertDoesNotThrow(() -> validateSourceOptionRule(validSourceConfig()));
    }

    @Test
    void testPositiveFetchSizePassesOptionValidation() {
        Map<String, Object> config = validSourceConfig();
        config.put(MongodbSourceOptions.FETCH_SIZE.key(), 1);

        Assertions.assertDoesNotThrow(() -> validateSourceOptionRule(config));

        config.put(MongodbSourceOptions.FETCH_SIZE.key(), 2048);
        Assertions.assertDoesNotThrow(() -> validateSourceOptionRule(config));
    }

    @Test
    void testZeroFetchSizeFailsOptionValidation() {
        Map<String, Object> config = validSourceConfig();
        config.put(MongodbSourceOptions.FETCH_SIZE.key(), 0);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSourceOptionRule(config));

        Assertions.assertTrue(
                exception.getMessage().contains(MongodbSourceOptions.FETCH_SIZE.key()));
    }

    @Test
    void testNegativeFetchSizeFailsOptionValidation() {
        Map<String, Object> config = validSourceConfig();
        config.put(MongodbSourceOptions.FETCH_SIZE.key(), -1);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSourceOptionRule(config));

        Assertions.assertTrue(
                exception.getMessage().contains(MongodbSourceOptions.FETCH_SIZE.key()));
    }

    private void validateSourceOptionRule(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sourceFactory.optionRule());
    }

    private Map<String, Object> validSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(MongodbSourceOptions.URI.key(), "mongodb://localhost:27017");
        config.put(MongodbSourceOptions.DATABASE.key(), "test_database");
        config.put(MongodbSourceOptions.COLLECTION.key(), "test_collection");
        config.put(
                ConnectorCommonOptions.SCHEMA.key(),
                Collections.singletonMap("fields", Collections.singletonMap("value", "string")));
        return config;
    }
}
