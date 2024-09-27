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

package org.apache.seatunnel.e2e.connector.v2.milvus;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.catalog.MilvusCatalog;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.milvus.MilvusContainer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK not support adapt")
public class MilvusIT extends TestSuiteBase implements TestResource {

    private static final String HOST = "milvus-e2e";
    private static final String MILVUS_IMAGE = "milvusdb/milvus:2.4-20240711-7e2a9d6b";
    private static final String TOKEN = "root:Milvus";
    private MilvusContainer container;
    private MilvusServiceClient milvusClient;
    private static final String COLLECTION_NAME = "simple_example";
    private static final String COLLECTION_NAME_1 = "simple_example_1";
    private static final String COLLECTION_NAME_2 = "simple_example_2";
    private static final String ID_FIELD = "book_id";
    private static final String VECTOR_FIELD = "book_intro";
    private static final String VECTOR_FIELD2 = "book_kind";
    private static final String VECTOR_FIELD3 = "book_binary";
    private static final String VECTOR_FIELD4 = "book_map";

    private static final String TITLE_FIELD = "book_title";
    private static final Integer VECTOR_DIM = 4;

    private Catalog catalog;
    private static final Gson gson = new Gson();

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.container =
                new MilvusContainer(MILVUS_IMAGE).withNetwork(NETWORK).withNetworkAliases(HOST);
        Startables.deepStart(Stream.of(this.container)).join();
        log.info("Milvus host is {}", container.getHost());
        log.info("Milvus container started");
        Awaitility.given().ignoreExceptions().await().atMost(720L, TimeUnit.SECONDS);
        this.initMilvus();
        this.initSourceData();
    }

    private void initMilvus()
            throws SQLException, ClassNotFoundException, InstantiationException,
                    IllegalAccessException {
        Map<String, Object> config = new HashMap<>();
        config.put(MilvusSinkConfig.URL.key(), this.container.getEndpoint());
        config.put(MilvusSinkConfig.TOKEN.key(), TOKEN);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        catalog = new MilvusCatalog(COLLECTION_NAME, readonlyConfig);
        catalog.open();
        milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withUri(this.container.getEndpoint())
                                .withToken(TOKEN)
                                .build());
    }

    private void initSourceData() {
        // Define fields
        List<FieldType> fieldsSchema =
                Arrays.asList(
                        FieldType.newBuilder()
                                .withName(ID_FIELD)
                                .withDataType(DataType.Int64)
                                .withPrimaryKey(true)
                                .withAutoID(false)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD)
                                .withDataType(DataType.FloatVector)
                                .withDimension(VECTOR_DIM)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD2)
                                .withDataType(DataType.Float16Vector)
                                .withDimension(VECTOR_DIM)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD3)
                                .withDataType(DataType.BinaryVector)
                                .withDimension(VECTOR_DIM * 2)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD4)
                                .withDataType(DataType.SparseFloatVector)
                                .build(),
                        FieldType.newBuilder()
                                .withName(TITLE_FIELD)
                                .withDataType(DataType.VarChar)
                                .withMaxLength(64)
                                .build());

        // Create the collection with 3 fields
        R<RpcStatus> ret =
                milvusClient.createCollection(
                        CreateCollectionParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldTypes(fieldsSchema)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create collection! Error: " + ret.getMessage());
        }

        // Specify an index type on the vector field.
        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD)
                                .withIndexType(IndexType.FLAT)
                                .withMetricType(MetricType.L2)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD2)
                                .withIndexType(IndexType.FLAT)
                                .withMetricType(MetricType.L2)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }
        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD3)
                                .withIndexType(IndexType.BIN_FLAT)
                                .withMetricType(MetricType.HAMMING)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD4)
                                .withIndexType(IndexType.SPARSE_INVERTED_INDEX)
                                .withMetricType(MetricType.IP)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        // Call loadCollection() to enable automatically loading data into memory for searching
        milvusClient.loadCollection(
                LoadCollectionParam.newBuilder().withCollectionName(COLLECTION_NAME).build());

        log.info("Collection created");

        // Insert 10 records into the collection
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 1L; i <= 10; ++i) {

            JsonObject row = new JsonObject();
            row.add(ID_FIELD, gson.toJsonTree(i));
            List<Float> vector = Arrays.asList((float) i, (float) i, (float) i, (float) i);
            row.add(VECTOR_FIELD, gson.toJsonTree(vector));
            Short[] shorts = {(short) i, (short) i, (short) i, (short) i};
            ByteBuffer shortByteBuffer = BufferUtils.toByteBuffer(shorts);
            row.add(VECTOR_FIELD2, gson.toJsonTree(shortByteBuffer.array()));
            ByteBuffer binaryByteBuffer = ByteBuffer.wrap(new byte[] {16});
            row.add(VECTOR_FIELD3, gson.toJsonTree(binaryByteBuffer.array()));
            HashMap<Long, Float> sparse = new HashMap<>();
            sparse.put(1L, 1.0f);
            sparse.put(2L, 2.0f);
            sparse.put(3L, 3.0f);
            sparse.put(4L, 4.0f);
            row.add(VECTOR_FIELD4, gson.toJsonTree(sparse));
            row.addProperty(TITLE_FIELD, "Tom and Jerry " + i);
            rows.add(row);
        }

        R<MutationResult> insertRet =
                milvusClient.insert(
                        InsertParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withRows(rows)
                                .build());
        if (insertRet.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to insert! Error: " + insertRet.getMessage());
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        this.milvusClient.close();
        this.container.close();
        if (catalog != null) {
            catalog.close();
        }
    }

    @TestTemplate
    public void testMilvus(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/milvus-to-milvus.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // assert table exist
        R<Boolean> hasCollectionResponse =
                this.milvusClient.hasCollection(
                        HasCollectionParam.newBuilder()
                                .withDatabaseName("test")
                                .withCollectionName(COLLECTION_NAME)
                                .build());
        Assertions.assertTrue(hasCollectionResponse.getData());

        // check table fields
        R<DescribeCollectionResponse> describeCollectionResponseR =
                this.milvusClient.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withDatabaseName("test")
                                .withCollectionName(COLLECTION_NAME)
                                .build());

        DescribeCollectionResponse data = describeCollectionResponseR.getData();
        List<String> fileds =
                data.getSchema().getFieldsList().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        Assertions.assertTrue(fileds.contains(ID_FIELD));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD2));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD3));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD4));
        Assertions.assertTrue(fileds.contains(TITLE_FIELD));
    }

    @TestTemplate
    public void testFakeToMilvus(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake-to-milvus.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // assert table exist
        R<Boolean> hasCollectionResponse =
                this.milvusClient.hasCollection(
                        HasCollectionParam.newBuilder()
                                .withDatabaseName("test1")
                                .withCollectionName(COLLECTION_NAME_1)
                                .build());
        Assertions.assertTrue(hasCollectionResponse.getData());

        // check table fields
        R<DescribeCollectionResponse> describeCollectionResponseR =
                this.milvusClient.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withDatabaseName("test1")
                                .withCollectionName(COLLECTION_NAME_1)
                                .build());

        DescribeCollectionResponse data = describeCollectionResponseR.getData();
        List<String> fileds =
                data.getSchema().getFieldsList().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        Assertions.assertTrue(fileds.contains(ID_FIELD));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD));
        Assertions.assertTrue(fileds.contains(TITLE_FIELD));
    }

    @TestTemplate
    public void testMultiFakeToMilvus(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/multi-fake-to-milvus.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // assert table exist
        R<Boolean> hasCollectionResponse =
                this.milvusClient.hasCollection(
                        HasCollectionParam.newBuilder()
                                .withDatabaseName("test2")
                                .withCollectionName(COLLECTION_NAME_2)
                                .build());
        Assertions.assertTrue(hasCollectionResponse.getData());

        // check table fields
        R<DescribeCollectionResponse> describeCollectionResponseR =
                this.milvusClient.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withDatabaseName("test2")
                                .withCollectionName(COLLECTION_NAME_2)
                                .build());

        DescribeCollectionResponse data = describeCollectionResponseR.getData();
        List<String> fileds =
                data.getSchema().getFieldsList().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());

        // assert table fields
        Assertions.assertTrue(fileds.contains(ID_FIELD));
        Assertions.assertTrue(fileds.contains("book_intro_1"));
        Assertions.assertTrue(fileds.contains("book_intro_2"));
        Assertions.assertTrue(fileds.contains("book_intro_3"));
        Assertions.assertTrue(fileds.contains("book_intro_4"));
    }

    @TestTemplate
    public void testCatalog(TestContainer container) {
        // simple_example always exist
        Assertions.assertThrows(
                TableAlreadyExistException.class,
                () -> catalog.createTable(TablePath.of("default", "simple_example"), null, false));
        Assertions.assertDoesNotThrow(
                () -> catalog.createTable(TablePath.of("default", "simple_example"), null, true));

        // create tmp
        Assertions.assertDoesNotThrow(
                () ->
                        catalog.createTable(
                                TablePath.of("default", "tmp"),
                                CatalogTable.of(
                                        TableIdentifier.of(
                                                COLLECTION_NAME, TablePath.of("default", "tmp")),
                                        TableSchema.builder()
                                                .column(
                                                        new PhysicalColumn(
                                                                "id",
                                                                BasicType.LONG_TYPE,
                                                                null,
                                                                null,
                                                                false,
                                                                null,
                                                                null))
                                                .column(
                                                        new PhysicalColumn(
                                                                "vector",
                                                                VectorType.VECTOR_FLOAT_TYPE,
                                                                128L,
                                                                8,
                                                                false,
                                                                null,
                                                                null))
                                                .primaryKey(
                                                        new PrimaryKey(
                                                                "",
                                                                Collections.singletonList("id")))
                                                .build(),
                                        Collections.emptyMap(),
                                        Collections.emptyList(),
                                        ""),
                                false));
        Assertions.assertDoesNotThrow(
                () -> catalog.dropTable(TablePath.of("default", "tmp"), false));
        Assertions.assertThrows(
                TableNotExistException.class,
                () -> catalog.dropTable(TablePath.of("default", "tmp"), false));

        // create new database
        Assertions.assertDoesNotThrow(
                () -> catalog.createDatabase(TablePath.of("new_db.table"), true));
        Assertions.assertThrows(
                DatabaseAlreadyExistException.class,
                () -> catalog.createDatabase(TablePath.of("new_db.table"), false));
        Assertions.assertDoesNotThrow(
                () -> catalog.dropDatabase(TablePath.of("new_db.table"), false));
    }
}
