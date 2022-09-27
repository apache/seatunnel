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

import org.apache.seatunnel.e2e.container.mongdb.MongodbContainer;
import org.apache.seatunnel.e2e.spark.SparkContainer;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MongodbIT extends SparkContainer {

    private MongodbContainer mongodbContainer;

    @BeforeEach
    public void startMongoContainer() {
        this.mongodbContainer = new MongodbContainer();
        this.mongodbContainer.startMongoContainer(NETWORK);
    }

    @Test
    public void testMongodb() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/mongodb/mongodb_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<Document> testDataSet = MongodbContainer.TEST_DATASET;
        List<Document> readSinkData = mongodbContainer.readSinkData();
        Assertions.assertIterableEquals(
            testDataSet.stream()
                .map(e -> {
                    e.remove("_id");
                    return e;
                })
                .collect(Collectors.toList()),
            readSinkData.stream()
                .map(e -> {
                    e.remove("_id");
                    return e;
                })
                .collect(Collectors.toList()));
    }

    @AfterEach
    public void closeMongoContainer() {
        if (mongodbContainer != null) {
            mongodbContainer.closeMongoContainer();
        }
    }
}
