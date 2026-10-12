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
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataDocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataToBsonConverters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongoKeyExtractor;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbSink;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils.PeriodicCheckpointOptions;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.WriteModel;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@Slf4j
public class MongodbIT extends AbstractMongodbIT {

    @Test
    @Timeout(60)
    public void testSinkDryRunLeavesDataAndMissingCollectionsUntouched() {
        String uri =
                "mongodb://"
                        + mongodbContainer.getHost()
                        + ":"
                        + mongodbContainer.getMappedPort(MONGODB_PORT)
                        + "/?serverSelectionTimeoutMS=2000&connectTimeoutMS=2000&socketTimeoutMS=2000";
        String existing = "dry_run_existing";
        String missing = "dry_run_missing";
        Document original = new Document("_id", 1).append("value", "keep");
        MongoCollection<Document> collection =
                client.getDatabase(MONGODB_DATABASE).getCollection(existing);
        collection.insertOne(original);
        try {
            for (DataSaveMode mode :
                    Arrays.asList(
                            DataSaveMode.APPEND_DATA,
                            DataSaveMode.DROP_DATA,
                            DataSaveMode.ERROR_WHEN_DATA_EXISTS)) {
                Map<String, Object> config = sinkDryRunConfig(uri, existing);
                config.put(MongodbSinkOptions.DATA_SAVE_MODE.key(), mode.name());
                config.put(MongodbSinkOptions.TRANSACTION.key(), true);
                validateSinkDryRun(config);
                Assertions.assertEquals(
                        Collections.singletonList(original),
                        collection.find().into(new ArrayList<>()),
                        mode.name());
            }
            validateSinkDryRun(sinkDryRunConfig(uri, missing));
            Assertions.assertFalse(
                    client.getDatabase(MONGODB_DATABASE)
                            .listCollectionNames()
                            .into(new ArrayList<>())
                            .contains(missing));
            Map<String, Object> newDatabase = sinkDryRunConfig(uri, missing);
            newDatabase.put(MongodbSinkOptions.DATABASE.key(), "dry_run_missing_database");
            validateSinkDryRun(newDatabase);
            Assertions.assertFalse(
                    client.listDatabaseNames()
                            .into(new ArrayList<>())
                            .contains("dry_run_missing_database"));
        } finally {
            collection.drop();
        }
    }

    @Test
    @Timeout(180)
    public void testSinkDryRunAuthenticatesWithoutDataPrivileges() {
        try (GenericContainer<?> authenticated =
                new GenericContainer<>(DockerImageName.parse("mongo:8.0.11"))
                        .withEnv("MONGO_INITDB_ROOT_USERNAME", "admin")
                        .withEnv("MONGO_INITDB_ROOT_PASSWORD", "admin-password")
                        .withExposedPorts(MONGODB_PORT)
                        .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 2))
                        .withStartupTimeout(Duration.ofMinutes(2))) {
            authenticated.start();
            String endpoint =
                    authenticated.getHost() + ":" + authenticated.getMappedPort(MONGODB_PORT);
            String options =
                    "/?authSource=admin&serverSelectionTimeoutMS=2000&connectTimeoutMS=2000&socketTimeoutMS=2000";
            try (MongoClient admin =
                    MongoClients.create("mongodb://admin:admin-password@" + endpoint + options)) {
                admin.getDatabase("admin")
                        .runCommand(
                                new Document("createUser", "probe")
                                        .append("pwd", "probe-password")
                                        .append("roles", Collections.emptyList()));
                MongoCollection<Document> collection =
                        admin.getDatabase(MONGODB_DATABASE).getCollection("dry_run_auth");
                Document original = new Document("_id", 1).append("value", "keep");
                collection.insertOne(original);

                String uri = "mongodb://probe:probe-password@" + endpoint + options;
                // The account has no document privileges. Only ping (and driver session cleanup)
                // may be sent, and even destructive save modes must leave the documents intact.
                List<String> commands = new CopyOnWriteArrayList<>();
                MongoClientSettings settings =
                        MongoClientSettings.builder()
                                .applyConnectionString(new ConnectionString(uri))
                                .addCommandListener(
                                        new CommandListener() {
                                            @Override
                                            public void commandStarted(CommandStartedEvent event) {
                                                commands.add(event.getCommandName());
                                            }
                                        })
                                .build();
                try (MongoClient observed = spy(MongoClients.create(settings));
                        MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
                    clients.when(() -> MongoClients.create(any(MongoClientSettings.class)))
                            .thenReturn(observed);
                    Map<String, Object> config = sinkDryRunConfig(uri, "dry_run_auth");
                    config.put(
                            MongodbSinkOptions.DATA_SAVE_MODE.key(), DataSaveMode.DROP_DATA.name());
                    config.put(MongodbSinkOptions.TRANSACTION.key(), true);
                    validateSinkDryRun(config);
                    verify(observed).close();
                }
                Assertions.assertEquals(1, commands.stream().filter("ping"::equals).count());
                Assertions.assertTrue(
                        commands.stream()
                                .allMatch(
                                        command ->
                                                command.equals("ping")
                                                        || command.equals("endSessions")),
                        commands.toString());
                Assertions.assertEquals(
                        Collections.singletonList(original),
                        collection.find().into(new ArrayList<>()));

                // Exercise the unmodified client creation path against an authenticated server too.
                validateSinkDryRun(sinkDryRunConfig(uri, "not_created"));
                Assertions.assertFalse(
                        admin.getDatabase(MONGODB_DATABASE)
                                .listCollectionNames()
                                .into(new ArrayList<>())
                                .contains("not_created"));
                IllegalStateException failure =
                        Assertions.assertThrows(
                                IllegalStateException.class,
                                () ->
                                        validateSinkDryRun(
                                                sinkDryRunConfig(
                                                        "mongodb://probe:wrong-password@"
                                                                + endpoint
                                                                + options,
                                                        "dry_run_auth")));
                Assertions.assertEquals(
                        "MongoDB sink dry-run authentication failed.", failure.getMessage());
                Assertions.assertNull(failure.getCause());
                Assertions.assertEquals(0, failure.getSuppressed().length);
                // Without configured credentials ping only establishes connectivity, not write
                // access.
                validateSinkDryRun(
                        sinkDryRunConfig("mongodb://" + endpoint + options, "dry_run_auth"));
            }
        }
    }

    @Test
    @Timeout(10)
    public void testSinkDryRunFailsWithinConfiguredTimeout() throws IOException {
        // Keep the port occupied but never speak MongoDB: no race with another process binding it.
        try (ServerSocket unresponsive =
                new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            String uri =
                    "mongodb://127.0.0.1:"
                            + unresponsive.getLocalPort()
                            + "/?serverSelectionTimeoutMS=1000&connectTimeoutMS=1000&socketTimeoutMS=1000";
            IllegalStateException failure =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () -> validateSinkDryRun(sinkDryRunConfig(uri, "not_created")));
            Assertions.assertEquals(
                    "MongoDB sink dry-run connection timed out.", failure.getMessage());
            Assertions.assertNull(failure.getCause());
            Assertions.assertEquals(0, failure.getSuppressed().length);
        }
    }

    private Map<String, Object> sinkDryRunConfig(String uri, String collection) {
        Map<String, Object> config = new HashMap<>();
        config.put(MongodbSinkOptions.URI.key(), uri);
        config.put(MongodbSinkOptions.DATABASE.key(), MONGODB_DATABASE);
        config.put(MongodbSinkOptions.COLLECTION.key(), collection);
        return config;
    }

    @Test
    @Timeout(60)
    public void testSinkDryRunDoesNotChangeRuntimeSaveModes() throws Exception {
        String uri =
                "mongodb://"
                        + mongodbContainer.getHost()
                        + ":"
                        + mongodbContainer.getMappedPort(MONGODB_PORT);
        String name = "dry_run_then_write";
        MongoCollection<Document> collection =
                client.getDatabase(MONGODB_DATABASE).getCollection(name);
        for (DataSaveMode mode :
                Arrays.asList(
                        DataSaveMode.APPEND_DATA,
                        DataSaveMode.DROP_DATA,
                        DataSaveMode.ERROR_WHEN_DATA_EXISTS)) {
            Document original = new Document("_id", 0).append("value", "keep");
            collection.insertOne(original);
            try {
                Map<String, Object> config = sinkDryRunConfig(uri, name);
                config.put(MongodbSinkOptions.DATA_SAVE_MODE.key(), mode.name());
                config.put(MongodbSinkOptions.BUFFER_FLUSH_MAX_ROWS.key(), 1);
                TableSinkFactoryContext context =
                        new TableSinkFactoryContext(
                                getCatalogTable(name),
                                ReadonlyConfig.fromMap(config),
                                getClass().getClassLoader());
                MongodbSinkFactory factory = new MongodbSinkFactory();
                factory.validateConnectionForDryRun(context);
                Assertions.assertEquals(
                        Collections.singletonList(original),
                        collection.find().into(new ArrayList<>()));
                MongodbSink sink = (MongodbSink) factory.createSink(context).createSink();
                try (SaveModeHandler handler = sink.getSaveModeHandler().get()) {
                    handler.open();
                    if (mode == DataSaveMode.ERROR_WHEN_DATA_EXISTS) {
                        Assertions.assertThrows(
                                SeaTunnelRuntimeException.class, handler::handleSaveMode);
                        Assertions.assertEquals(1, collection.countDocuments());
                        continue;
                    }
                    handler.handleSaveMode();
                }
                SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> writer =
                        sink.createWriter(new DefaultSinkWriterContext(0, 1));
                try {
                    writer.write(getSeaTunnelRowOne());
                } finally {
                    writer.close();
                }
                Assertions.assertEquals(
                        mode == DataSaveMode.APPEND_DATA ? 2 : 1,
                        collection.countDocuments(),
                        mode.name());
            } finally {
                collection.drop();
            }
        }
    }

    private void validateSinkDryRun(Map<String, Object> config) {
        new MongodbSinkFactory()
                .validateConnectionForDryRun(
                        new TableSinkFactoryContext(
                                null, ReadonlyConfig.fromMap(config), getClass().getClassLoader()));
    }

    @TestTemplate
    public void testMongodbSourceAndSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult insertResult = container.executeJob("/fake_source_to_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult assertResult = container.executeJob("/mongodb_source_to_assert.conf");
        Assertions.assertEquals(0, assertResult.getExitCode(), assertResult.getStderr());
        clearData(MONGODB_SINK_TABLE);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Currently SPARK and FLINK do not support mongodb null value write")
    public void testMongodbNullValue(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult nullResult = container.executeJob("/mongodb_null_value.conf");
        Assertions.assertEquals(0, nullResult.getExitCode(), nullResult.getStderr());
        Assertions.assertIterableEquals(
                TEST_NULL_DATASET.stream().peek(e -> e.remove("_id")).collect(Collectors.toList()),
                readMongodbData(MONGODB_NULL_TABLE_RESULT).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearData(MONGODB_NULL_TABLE);
        clearData(MONGODB_NULL_TABLE_RESULT);
    }

    @TestTemplate
    public void testMongodbSourceMatch(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult queryResult =
                container.executeJob("/matchIT/mongodb_matchQuery_source_to_assert.conf");
        Assertions.assertEquals(0, queryResult.getExitCode(), queryResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_MATCH_DATASET.stream()
                        .filter(x -> x.get("c_int").equals(2))
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_MATCH_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearData(MONGODB_MATCH_RESULT_TABLE);

        Container.ExecResult projectionResult =
                container.executeJob("/matchIT/mongodb_matchProjection_source_to_assert.conf");
        Assertions.assertEquals(0, projectionResult.getExitCode(), projectionResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_MATCH_DATASET.stream()
                        .map(Document::new)
                        .peek(document -> document.remove("c_bigint"))
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_MATCH_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearData(MONGODB_MATCH_RESULT_TABLE);
    }

    @TestTemplate
    public void testFakeSourceToUpdateMongodb(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult insertResult =
                container.executeJob("/updateIT/fake_source_to_updateMode_insert_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult updateResult =
                container.executeJob("/updateIT/fake_source_to_update_mongodb.conf");
        Assertions.assertEquals(0, updateResult.getExitCode(), updateResult.getStderr());

        Container.ExecResult assertResult =
                container.executeJob("/updateIT/update_mongodb_to_assert.conf");
        Assertions.assertEquals(0, assertResult.getExitCode(), assertResult.getStderr());

        clearData(MONGODB_UPDATE_TABLE);
    }

    @TestTemplate
    public void testFlatSyncString(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult insertResult =
                container.executeJob("/flatIT/fake_source_to_flat_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult assertResult =
                container.executeJob("/flatIT/mongodb_flat_source_to_assert.conf");
        Assertions.assertEquals(0, assertResult.getExitCode(), assertResult.getStderr());

        clearData(MONGODB_FLAT_TABLE);
    }

    @TestTemplate
    public void testMongodbSourceSplit(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult queryResult =
                container.executeJob("/splitIT/mongodb_split_key_source_to_assert.conf");
        Assertions.assertEquals(0, queryResult.getExitCode(), queryResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_SPLIT_DATASET.stream()
                        .map(Document::new)
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_SPLIT_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearData(MONGODB_SPLIT_RESULT_TABLE);

        Container.ExecResult projectionResult =
                container.executeJob("/splitIT/mongodb_split_size_source_to_assert.conf");
        Assertions.assertEquals(0, projectionResult.getExitCode(), projectionResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_SPLIT_DATASET.stream()
                        .map(Document::new)
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_SPLIT_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearData(MONGODB_SPLIT_RESULT_TABLE);
    }

    @TestTemplate
    public void testCompatibleParameters(TestContainer container)
            throws IOException, InterruptedException {
        // `upsert-key` compatible test
        Container.ExecResult insertResult =
                container.executeJob("/updateIT/fake_source_to_updateMode_insert_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult updateResult =
                container.executeJob("/compatibleParametersIT/fake_source_to_update_mongodb.conf");
        Assertions.assertEquals(0, updateResult.getExitCode(), updateResult.getStderr());

        Container.ExecResult assertResult =
                container.executeJob("/updateIT/update_mongodb_to_assert.conf");
        Assertions.assertEquals(0, assertResult.getExitCode(), assertResult.getStderr());

        clearData(MONGODB_UPDATE_TABLE);

        // `matchQuery` compatible test
        Container.ExecResult queryResult =
                container.executeJob("/matchIT/mongodb_matchQuery_source_to_assert.conf");
        Assertions.assertEquals(0, queryResult.getExitCode(), queryResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_MATCH_DATASET.stream()
                        .filter(x -> x.get("c_int").equals(2))
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_MATCH_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearData(MONGODB_MATCH_RESULT_TABLE);
    }

    @TestTemplate
    public void testTransactionSinkAndUpsert(TestContainer container)
            throws IOException, InterruptedException {
        runTransactionSinkFlow(MONGODB_TRANSACTION_SINK_TABLE, false);
        runTransactionSinkFlow(MONGODB_TRANSACTION_UPSERT_TABLE, true);
    }

    @TestTemplate
    public void testMongodbDoubleValue(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult assertSinkResult = container.executeJob("/mongodb_double_value.conf");
        Assertions.assertEquals(0, assertSinkResult.getExitCode(), assertSinkResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_DOUBLE_DATASET.stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_DOUBLE_TABLE_RESULT).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearData(MONGODB_DOUBLE_TABLE_RESULT);
    }

    @TestTemplate
    public void testFakeSourceToMongodbMultipleTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult insertResult =
                container.executeJob("/fake_source_to_mongodb_multiple_table.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());
        String collectionOneStr = "testDatabase1_testSchema1_testTable1_check";
        MongoCollection<BsonDocument> collectionOne =
                client.getDatabase(MONGODB_DATABASE)
                        .getCollection(collectionOneStr, BsonDocument.class);
        Assertions.assertEquals(1, collectionOne.countDocuments());
        String collectionTwoStr = "testDatabase2_testSchema2_testTable2_check";
        MongoCollection<BsonDocument> collectionTwo =
                client.getDatabase(MONGODB_DATABASE)
                        .getCollection(collectionTwoStr, BsonDocument.class);
        Assertions.assertEquals(1, collectionTwo.countDocuments());
        clearData(collectionOneStr);
        clearData(collectionTwoStr);
    }

    @SneakyThrows
    @TestTemplate
    public void testDropDataSaveMode(TestContainer container) {
        // test drop data save mode
        String collectionName = "drop_data_save_mode_coll";
        MongoCollection<BsonDocument> collection =
                client.getDatabase(MONGODB_DATABASE)
                        .getCollection(collectionName, BsonDocument.class);
        // insert one row
        beforeInsertData(collectionName, DataSaveMode.DROP_DATA, collection);
        // build sink
        final MongodbSink mongoDbSink = getSinkInstance(collectionName, DataSaveMode.DROP_DATA);
        final SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> writer =
                mongoDbSink.createWriter(new DefaultSinkWriterContext(0, 1));
        final Optional<SaveModeHandler> saveModeHandlerOptional = mongoDbSink.getSaveModeHandler();
        // do save mode
        if (saveModeHandlerOptional.isPresent()) {
            final SaveModeHandler saveModeHandler = saveModeHandlerOptional.get();
            saveModeHandler.open();
            saveModeHandler.handleSaveMode();
            saveModeHandler.close();
        }
        // do write
        writer.write(getSeaTunnelRowOne());
        Assertions.assertEquals(1L, collection.countDocuments());
        // clear
        collection.drop();
    }

    @SneakyThrows
    @TestTemplate
    public void testAppendDataSaveMode(TestContainer container) {
        // test drop data save mode
        String collectionName = "append_data_save_mode_coll";
        MongoCollection<BsonDocument> collection =
                client.getDatabase(MONGODB_DATABASE)
                        .getCollection(collectionName, BsonDocument.class);
        // insert one row
        beforeInsertData(collectionName, DataSaveMode.APPEND_DATA, collection);
        // build sink
        final MongodbSink mongoDbSink = getSinkInstance(collectionName, DataSaveMode.APPEND_DATA);
        final SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> writer =
                mongoDbSink.createWriter(new DefaultSinkWriterContext(0, 1));
        final Optional<SaveModeHandler> saveModeHandlerOptional = mongoDbSink.getSaveModeHandler();
        // do save mode
        if (saveModeHandlerOptional.isPresent()) {
            final SaveModeHandler saveModeHandler = saveModeHandlerOptional.get();
            saveModeHandler.open();
            saveModeHandler.handleSaveMode();
            saveModeHandler.close();
        }
        // do write
        writer.write(getSeaTunnelRowOne());
        Assertions.assertEquals(3L, collection.countDocuments());
        // clear
        collection.drop();
    }

    @SneakyThrows
    @TestTemplate
    public void testErrorWhenDataExistsSaveMode(TestContainer container) {
        // test drop data save mode
        String collectionName = "error_data_save_mode_coll";
        MongoCollection<BsonDocument> collection =
                client.getDatabase(MONGODB_DATABASE)
                        .getCollection(collectionName, BsonDocument.class);
        // insert one row
        beforeInsertData(collectionName, DataSaveMode.ERROR_WHEN_DATA_EXISTS, collection);
        // build sink
        final MongodbSink mongoDbSink =
                getSinkInstance(collectionName, DataSaveMode.ERROR_WHEN_DATA_EXISTS);
        final SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> writer =
                mongoDbSink.createWriter(new DefaultSinkWriterContext(0, 1));
        final Optional<SaveModeHandler> saveModeHandlerOptional = mongoDbSink.getSaveModeHandler();
        // do save mode
        if (saveModeHandlerOptional.isPresent()) {
            final SaveModeHandler saveModeHandler = saveModeHandlerOptional.get();
            saveModeHandler.open();
            Assertions.assertThrows(
                    SeaTunnelRuntimeException.class,
                    saveModeHandler::handleDataSaveMode,
                    "When there exist data, an error will be reported");
            saveModeHandler.close();
        }
        Assertions.assertEquals(2L, collection.countDocuments());
        // clear
        collection.drop();
    }

    private void beforeInsertData(
            String collection,
            DataSaveMode dataSaveMode,
            MongoCollection<BsonDocument> dropDataCollection) {
        final RowDataDocumentSerializer rowDataDocumentSerializer =
                new RowDataDocumentSerializer(
                        RowDataToBsonConverters.createConverter(
                                getCatalogTable(collection).getSeaTunnelRowType()),
                        getMongodbWriterOptions(collection, dataSaveMode),
                        new MongoKeyExtractor(getMongodbWriterOptions(collection, dataSaveMode)));
        WriteModel<BsonDocument> bsonDocumentWriteModelOne =
                rowDataDocumentSerializer.serializeToWriteModel(getSeaTunnelRowOne());
        WriteModel<BsonDocument> bsonDocumentWriteModelTwo =
                rowDataDocumentSerializer.serializeToWriteModel(getSeaTunnelRowTwo());
        List<WriteModel<BsonDocument>> writeModelList = new ArrayList<>();
        writeModelList.add(bsonDocumentWriteModelOne);
        writeModelList.add(bsonDocumentWriteModelTwo);
        dropDataCollection.bulkWrite(writeModelList);
    }

    private SeaTunnelRow getSeaTunnelRowOne() {
        return new SeaTunnelRow(new Object[] {1L, "A", 100});
    }

    private SeaTunnelRow getSeaTunnelRowTwo() {
        return new SeaTunnelRow(new Object[] {2L, "B", 200});
    }

    private MongodbSink getSinkInstance(String collection, DataSaveMode dataSaveMode) {
        return new MongodbSink(
                getMongodbWriterOptions(collection, dataSaveMode), getCatalogTable(collection));
    }

    private MongodbWriterOptions getMongodbWriterOptions(
            String collection, DataSaveMode dataSaveMode) {
        String host = mongodbContainer.getContainerIpAddress();
        int port = mongodbContainer.getFirstMappedPort();
        String url = String.format("mongodb://%s:%d/%s", host, port, MONGODB_DATABASE);
        return MongodbWriterOptions.builder()
                .withConnectString(url)
                .withDatabase(MONGODB_DATABASE)
                .withCollection(collection)
                .withDataSaveMode(dataSaveMode)
                .withFlushSize(1)
                .build();
    }

    private CatalogTable getCatalogTable(String collection) {
        return CatalogTable.of(
                TableIdentifier.of(
                        MongodbBaseOptions.CONNECTOR_IDENTITY, MONGODB_DATABASE, collection),
                getTableSchema(),
                new HashMap<>(),
                new ArrayList<>(),
                "");
    }

    private TableSchema getTableSchema() {
        return TableSchema.builder().columns(getColumns()).build();
    }

    private List<Column> getColumns() {
        List<Column> columns = new ArrayList<>();
        columns.add(new PhysicalColumn("c_int", BasicType.LONG_TYPE, 64L, 0, true, "", ""));
        columns.add(new PhysicalColumn("name", BasicType.STRING_TYPE, 100L, 0, true, "", ""));
        columns.add(new PhysicalColumn("score", BasicType.INT_TYPE, 32L, 0, true, "", ""));
        return columns;
    }

    private void runTransactionSinkFlow(String collection, boolean upsert) throws IOException {
        clearData(collection);
        List<SeaTunnelRow> rows = createTransactionRows(upsert);
        SinkFlowTestUtils.runBatchWithCheckpointEnabled(
                getCatalogTable(collection),
                getTransactionSinkOptions(collection, upsert),
                new MongodbSinkFactory(),
                rows,
                PeriodicCheckpointOptions.builder()
                        .recordsPerCheckpoint(2)
                        .maxCheckpointCount(5)
                        .triggerOnFinish(true)
                        .build());
        assertTransactionSinkResult(collection, upsert);
        clearData(collection);
    }

    private List<SeaTunnelRow> createTransactionRows(boolean upsert) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        rows.add(createRow(RowKind.INSERT, 1L, "alpha", 10));
        rows.add(createRow(RowKind.INSERT, 2L, "beta", 20));
        rows.add(createRow(RowKind.INSERT, 3L, "gamma", 30));
        if (upsert) {
            rows.add(createRow(RowKind.UPDATE_AFTER, 2L, "beta-updated", 200));
        }
        return rows;
    }

    private SeaTunnelRow createRow(RowKind kind, long id, String name, int score) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name, score});
        row.setRowKind(kind);
        return row;
    }

    private ReadonlyConfig getTransactionSinkOptions(String collection, boolean upsert) {
        String host = mongodbContainer.getHost();
        int port = mongodbContainer.getFirstMappedPort();
        String uri = String.format("mongodb://%s:%d", host, port);
        HashMap<String, Object> config = new HashMap<>();
        config.put(MongodbSinkOptions.URI.key(), uri);
        config.put(MongodbSinkOptions.DATABASE.key(), MONGODB_DATABASE);
        config.put(MongodbSinkOptions.COLLECTION.key(), collection);
        config.put(MongodbSinkOptions.TRANSACTION.key(), true);
        config.put(MongodbSinkOptions.DATA_SAVE_MODE.key(), DataSaveMode.APPEND_DATA);
        config.put(MongodbSinkOptions.BUFFER_FLUSH_MAX_ROWS.key(), 2);
        if (upsert) {
            config.put(MongodbSinkOptions.UPSERT_ENABLE.key(), true);
            config.put(MongodbSinkOptions.PRIMARY_KEY.key(), Arrays.asList("c_int"));
        }
        return ReadonlyConfig.fromMap(config);
    }

    private void assertTransactionSinkResult(String collection, boolean upsert) {
        MongoCollection<Document> mongoCollection =
                client.getDatabase(MONGODB_DATABASE).getCollection(collection);
        List<Document> documents =
                mongoCollection.find().sort(Sorts.ascending("c_int")).into(new ArrayList<>());
        Assertions.assertEquals(3, documents.size());
        Assertions.assertEquals("alpha", documents.get(0).getString("name"));
        if (upsert) {
            Assertions.assertEquals("beta-updated", documents.get(1).getString("name"));
            Assertions.assertEquals(200, documents.get(1).getInteger("score"));
        } else {
            Assertions.assertEquals("beta", documents.get(1).getString("name"));
            Assertions.assertEquals(20, documents.get(1).getInteger("score"));
        }
        Assertions.assertEquals("gamma", documents.get(2).getString("name"));
    }
}
