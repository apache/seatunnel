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

package org.apache.seatunnel.e2e.connector.v2.qdrant;

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
import org.testcontainers.qdrant.QdrantContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.Points;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorFactory.vector;
import static io.qdrant.client.VectorsFactory.namedVectors;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "SPARK and FLINK do not support vector types yet")
public class QdrantIT extends TestSuiteBase implements TestResource {

    private static final String ALIAS = "qdrante2e";
    private static final String SOURCE_COLLECTION = "source_collection";
    private static final String SINK_COLLECTION = "sink_collection";
    private static final String IMAGE = "qdrant/qdrant:latest";
    private QdrantContainer container;
    private QdrantClient qdrantClient;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.container = new QdrantContainer(IMAGE).withNetwork(NETWORK).withNetworkAliases(ALIAS);
        Startables.deepStart(Stream.of(this.container)).join();
        Awaitility.given().ignoreExceptions().await().atMost(10L, TimeUnit.SECONDS);
        this.initQdrant();
        this.initSourceData();
    }

    private void initQdrant() {
        qdrantClient =
                new QdrantClient(
                        QdrantGrpcClient.newBuilder(
                                        container.getHost(), container.getGrpcPort(), false)
                                .build());
    }

    private void initSourceData() throws Exception {
        qdrantClient
                .createCollectionAsync(
                        SOURCE_COLLECTION,
                        ImmutableMap.of(
                                "my_vector",
                                Collections.VectorParams.newBuilder()
                                        .setSize(4)
                                        .setDistance(Collections.Distance.Cosine)
                                        .build()))
                .get();

        qdrantClient
                .createCollectionAsync(
                        SINK_COLLECTION,
                        ImmutableMap.of(
                                "my_vector",
                                Collections.VectorParams.newBuilder()
                                        .setSize(4)
                                        .setDistance(Collections.Distance.Cosine)
                                        .build()))
                .get();

        List<Points.PointStruct> points = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Points.PointStruct.Builder pointStruct = Points.PointStruct.newBuilder();
            pointStruct.setId(id(i));
            List<Float> floats = Arrays.asList((float) i, (float) i, (float) i, (float) i);
            pointStruct.setVectors(namedVectors(ImmutableMap.of("my_vector", vector(floats))));

            pointStruct.putPayload("file_size", value(i));
            pointStruct.putPayload("file_name", value("file-name-" + i));

            points.add(pointStruct.build());
        }

        qdrantClient
                .upsertAsync(
                        Points.UpsertPoints.newBuilder()
                                .setCollectionName(SOURCE_COLLECTION)
                                .addAllPoints(points)
                                .build())
                .get();
    }

    @AfterAll
    @Override
    public void tearDown() {
        this.qdrantClient.close();
    }

    @TestTemplate
    public void testQdrant(TestContainer container)
            throws IOException, InterruptedException, ExecutionException {
        Container.ExecResult execResult = container.executeJob("/qdrant-to-qdrant.conf");
        Assertions.assertEquals(execResult.getExitCode(), 0);
        Assertions.assertEquals(qdrantClient.countAsync(SINK_COLLECTION).get(), 10);
    }
}
