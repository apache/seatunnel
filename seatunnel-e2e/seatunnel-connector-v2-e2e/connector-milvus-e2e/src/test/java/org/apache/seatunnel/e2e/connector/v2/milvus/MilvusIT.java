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

import com.alibaba.fastjson.JSONObject;
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private static final String ID_FIELD = "book_id";
    private static final String VECTOR_FIELD = "book_intro";
    private static final String TITLE_FIELD = "book_title";
    private static final Integer VECTOR_DIM = 4;

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

        // Call loadCollection() to enable automatically loading data into memory for searching
        milvusClient.loadCollection(
                LoadCollectionParam.newBuilder().withCollectionName(COLLECTION_NAME).build());

        log.info("Collection created");

        // Insert 10 records into the collection
        List<JSONObject> rows = new ArrayList<>();
        for (long i = 1L; i <= 10; ++i) {
            JSONObject row = new JSONObject();
            row.put(ID_FIELD, i);
            List<Float> vector = Arrays.asList((float) i, (float) i, (float) i, (float) i);
            row.put(VECTOR_FIELD, vector);
            row.put(TITLE_FIELD, "Tom and Jerry " + i);
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
        Assertions.assertTrue(fileds.contains(TITLE_FIELD));
    }
}
