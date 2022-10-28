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

package org.apache.seatunnel.e2e.spark.v2.mongodb;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.data.DefaultSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.data.Serializer;
import org.apache.seatunnel.e2e.spark.SparkContainer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class MongodbIT extends SparkContainer {

    private static final String MONGODB_IMAGE = "mongo:latest";
    private static final String MONGODB_CONTAINER_HOST = "spark_e2e_mongodb";
    private static final int MONGODB_PORT = 27017;
    private static final String MONGODB_DATABASE = "test_db";
    private static final String MONGODB_SOURCE_TABLE = "source_table";
    private static final String MONGODB_SINK_TABLE = "sink_table";

    private static final List<Document> TEST_DATASET = generateTestDataSet();

    private MongoClient client;
    private GenericContainer<?> mongodbContainer;

    @BeforeEach
    public void startMongoContainer() {
        DockerImageName imageName = DockerImageName.parse(MONGODB_IMAGE);
        mongodbContainer = new GenericContainer<>(imageName)
            .withNetwork(NETWORK)
            .withNetworkAliases(MONGODB_CONTAINER_HOST)
            .withExposedPorts(MONGODB_PORT)
            .waitingFor(new HttpWaitStrategy()
                .forPort(MONGODB_PORT)
                .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                .withStartupTimeout(Duration.ofMinutes(2)))
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MONGODB_IMAGE)));
        Startables.deepStart(Stream.of(mongodbContainer)).join();
        log.info("Mongodb container started");

        Awaitility.given().ignoreExceptions()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(this::initConnection);
        this.initSourceData();
    }

    @Test
    public void testMongodb() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/mongodb/mongodb_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertIterableEquals(
            TEST_DATASET.stream()
                .map(e -> {
                    e.remove("_id");
                    return e;
                })
                .collect(Collectors.toList()),
            readSinkData().stream()
                .map(e -> {
                    e.remove("_id");
                    return e;
                })
                .collect(Collectors.toList()));
    }

    public void initConnection() {
        String host = mongodbContainer.getContainerIpAddress();
        int port = mongodbContainer.getFirstMappedPort();
        String url = String.format("mongodb://%s:%d/%s", host, port, MONGODB_DATABASE);

        client = MongoClients.create(url);
    }

    private void initSourceData() {
        MongoCollection<Document> sourceTable = client
            .getDatabase(MONGODB_DATABASE)
            .getCollection(MONGODB_SOURCE_TABLE);

        sourceTable.deleteMany(new Document());
        sourceTable.insertMany(TEST_DATASET);
    }

    private List<Document> readSinkData() {
        MongoCollection<Document> sinkTable = client
            .getDatabase(MONGODB_DATABASE)
            .getCollection(MONGODB_SINK_TABLE);
        MongoCursor<Document> cursor = sinkTable.find()
            .sort(Sorts.ascending("id"))
            .cursor();
        List<Document> documents = new ArrayList<>();
        while (cursor.hasNext()) {
            documents.add(cursor.next());
        }
        return documents;
    }

    private static List<Document> generateTestDataSet() {
        SeaTunnelRowType seatunnelRowType = new SeaTunnelRowType(
            new String[]{
                "id",
                "c_map",
                "c_array",
                "c_string",
                "c_boolean",
                "c_tinyint",
                "c_smallint",
                "c_int",
                "c_bigint",
                "c_float",
                "c_double",
                "c_decimal",
                "c_bytes",
                "c_date",
                "c_timestamp"
            },
            new SeaTunnelDataType[]{
                BasicType.LONG_TYPE,
                new MapType(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
                ArrayType.BYTE_ARRAY_TYPE,
                BasicType.STRING_TYPE,
                BasicType.BOOLEAN_TYPE,
                BasicType.BYTE_TYPE,
                BasicType.SHORT_TYPE,
                BasicType.INT_TYPE,
                BasicType.LONG_TYPE,
                BasicType.FLOAT_TYPE,
                BasicType.DOUBLE_TYPE,
                new DecimalType(2, 1),
                PrimitiveByteArrayType.INSTANCE,
                LocalTimeType.LOCAL_DATE_TYPE,
                LocalTimeType.LOCAL_DATE_TIME_TYPE
            }
        );
        Serializer serializer = new DefaultSerializer(seatunnelRowType);

        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row = new SeaTunnelRow(
                new Object[]{
                    Long.valueOf(i),
                    Collections.singletonMap("key", Short.parseShort("1")),
                    new Byte[]{Byte.parseByte("1")},
                    "string",
                    Boolean.FALSE,
                    Byte.parseByte("1"),
                    Short.parseShort("1"),
                    Integer.parseInt("1"),
                    Long.parseLong("1"),
                    Float.parseFloat("1.1"),
                    Double.parseDouble("1.1"),
                    BigDecimal.valueOf(11, 1),
                    "test".getBytes(),
                    LocalDate.now(),
                    LocalDateTime.now()
                });
            documents.add(serializer.serialize(row));
        }
        return documents;
    }

    @AfterEach
    public void closeMongoContainer() {
        if (client != null) {
            client.close();
        }
        if (mongodbContainer != null) {
            mongodbContainer.close();
        }
    }
}
