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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbSingleCollectionProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.TlsTestMaterial;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.SslSettings;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * Verifies that every MongoDB sink component that opens a connection (writer, transaction
 * committer, save-mode catalog and dry-run validator) receives {@code tls_ca_file}, and that the
 * previous behavior is kept when the option is not set.
 */
class MongodbSinkTlsCaFileTest {

    private static final String URI = "mongodb://user:secret@localhost:27017";

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE});

    @TempDir Path tempDir;

    @Test
    void optionIsRegisteredAsOptional() {
        Assertions.assertTrue(
                new MongodbSinkFactory()
                        .optionRule()
                        .getOptionalOptions()
                        .contains(MongodbSinkOptions.TLS_CA_FILE));
    }

    @Test
    void writerAndTransactionCommitterUseCaFile() throws Exception {
        String ca = caFile();
        Map<String, Object> config = config(ca);
        config.put(MongodbSinkOptions.TRANSACTION.key(), true);

        Assertions.assertEquals(Arrays.asList(ca, ca), providerCaFiles(createSink(config)));
    }

    @Test
    void withoutCaFileProvidersKeepConnectionStringOnly() {
        Map<String, Object> config = config(null);
        config.put(MongodbSinkOptions.TRANSACTION.key(), true);

        Assertions.assertEquals(Arrays.asList(null, null), providerCaFiles(createSink(config)));
    }

    @Test
    void saveModeCatalogUsesCaFile() throws Exception {
        MongodbSink sink = createSink(config(caFile()));
        Catalog catalog = sink.getSaveModeHandler().get().getHandleCatalog();

        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                    .thenReturn(mock(MongoClient.class));
            catalog.open();
            catalog.close();

            ArgumentCaptor<MongoClientSettings> settings =
                    ArgumentCaptor.forClass(MongoClientSettings.class);
            clients.verify(() -> MongoClients.create(settings.capture()));
            clients.verify(() -> MongoClients.create(anyString()), never());
            assertTrustsConfiguredCa(settings.getValue().getSslSettings());
        }
    }

    @Test
    void saveModeCatalogWithoutCaFileUsesConnectionStringAsBefore() {
        MongodbSink sink = createSink(config(null));
        Catalog catalog = sink.getSaveModeHandler().get().getHandleCatalog();

        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(anyString()))
                    .thenReturn(mock(MongoClient.class));
            catalog.open();
            catalog.close();

            clients.verify(() -> MongoClients.create(URI));
            clients.verify(() -> MongoClients.create(any(MongoClientSettings.class)), never());
        }
    }

    @Test
    void dryRunUsesCaFile() throws Exception {
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        when(client.getDatabase("test_database")).thenReturn(database);
        when(database.runCommand(new Document("ping", 1), ReadPreference.primary()))
                .thenReturn(new Document("ok", 1));

        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            ArgumentCaptor<MongoClientSettings> settings =
                    ArgumentCaptor.forClass(MongoClientSettings.class);
            clients.when(() -> MongoClients.create(settings.capture())).thenReturn(client);

            new MongodbSinkFactory().validateConnectionForDryRun(context(config(caFile())));

            assertTrustsConfiguredCa(settings.getValue().getSslSettings());
        }
    }

    @Test
    void dryRunReportsInvalidCaFileWithoutLeakingUri() {
        String missing = tempDir.resolve("missing.pem").toString();

        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            IllegalStateException failure =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    new MongodbSinkFactory()
                                            .validateConnectionForDryRun(context(config(missing))));

            Assertions.assertTrue(failure.getMessage().contains("tls_ca_file"));
            Assertions.assertTrue(failure.getMessage().contains(missing));
            Assertions.assertFalse(failure.toString().contains("secret"));
            Assertions.assertNull(failure.getCause());
            clients.verify(() -> MongoClients.create(any(MongoClientSettings.class)), never());
        }
    }

    private String caFile() throws Exception {
        return TlsTestMaterial.writePem(tempDir, "ca.pem", TlsTestMaterial.testCa()).toString();
    }

    private static Map<String, Object> config(String caFile) {
        Map<String, Object> config = new HashMap<>();
        config.put(MongodbSinkOptions.URI.key(), URI);
        config.put(MongodbSinkOptions.DATABASE.key(), "test_database");
        config.put(MongodbSinkOptions.COLLECTION.key(), "test_collection");
        if (caFile != null) {
            config.put(MongodbSinkOptions.TLS_CA_FILE.key(), caFile);
        }
        return config;
    }

    private static TableSinkFactoryContext context(Map<String, Object> config) {
        return new TableSinkFactoryContext(
                CatalogTableUtil.getCatalogTable("test_table", ROW_TYPE),
                ReadonlyConfig.fromMap(config),
                MongodbSinkTlsCaFileTest.class.getClassLoader());
    }

    private static MongodbSink createSink(Map<String, Object> config) {
        return (MongodbSink) new MongodbSinkFactory().createSink(context(config)).createSink();
    }

    /** Creates the writer and the aggregated committer and returns the CA file each received. */
    private static List<String> providerCaFiles(MongodbSink sink) {
        List<String> caFiles = new ArrayList<>();
        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                Mockito.mockConstruction(
                        MongodbSingleCollectionProvider.class,
                        (provider, context) -> caFiles.add((String) context.arguments().get(3)))) {
            sink.createWriter(mock(SinkWriter.Context.class));
            sink.createAggregatedCommitter().get();
        }
        return caFiles;
    }

    private static void assertTrustsConfiguredCa(SslSettings ssl) {
        Assertions.assertTrue(ssl.isEnabled());
        Assertions.assertNotNull(ssl.getContext(), "per-client trust context from tls_ca_file");
        Assertions.assertFalse(ssl.isInvalidHostNameAllowed());
    }
}
