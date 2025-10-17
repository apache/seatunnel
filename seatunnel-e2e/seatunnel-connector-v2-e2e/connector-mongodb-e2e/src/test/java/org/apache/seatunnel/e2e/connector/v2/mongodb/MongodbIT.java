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

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataDocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataToBsonConverters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongoKeyExtractor;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbSink;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@Slf4j
public class MongodbIT extends AbstractMongodbIT {

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
        Container.ExecResult insertResult =
                container.executeJob("/transactionIT/fake_source_to_transaction_sink_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult assertSinkResult =
                container.executeJob(
                        "/transactionIT/mongodb_source_transaction_sink_to_assert.conf");
        Assertions.assertEquals(0, assertSinkResult.getExitCode(), assertSinkResult.getStderr());

        Container.ExecResult upsertResult =
                container.executeJob(
                        "/transactionIT/fake_source_to_transaction_upsert_mongodb.conf");
        Assertions.assertEquals(0, upsertResult.getExitCode(), upsertResult.getStderr());

        Container.ExecResult assertUpsertResult =
                container.executeJob(
                        "/transactionIT/mongodb_source_transaction_upsert_to_assert.conf");
        Assertions.assertEquals(
                0, assertUpsertResult.getExitCode(), assertUpsertResult.getStderr());

        clearData(MONGODB_TRANSACTION_SINK_TABLE);
        clearData(MONGODB_TRANSACTION_UPSERT_TABLE);
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
                mongoDbSink.createWriter(null);
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
                mongoDbSink.createWriter(null);
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
                mongoDbSink.createWriter(null);
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
                TableIdentifier.of(CONNECTOR_IDENTITY, MONGODB_DATABASE, collection),
                getTableSchema(),
                new HashMap<>(),
                new ArrayList<>(),
                "");
    }

    private TableSchema getTableSchema() {
        return new TableSchema(getColumns(), null, null);
    }

    private List<Column> getColumns() {
        List<Column> columns = new ArrayList<>();
        columns.add(new PhysicalColumn("c_int", BasicType.LONG_TYPE, 64L, 0, true, "", ""));
        columns.add(new PhysicalColumn("name", BasicType.STRING_TYPE, 100L, 0, true, "", ""));
        columns.add(new PhysicalColumn("score", BasicType.INT_TYPE, 32L, 0, true, "", ""));
        return columns;
    }
}
