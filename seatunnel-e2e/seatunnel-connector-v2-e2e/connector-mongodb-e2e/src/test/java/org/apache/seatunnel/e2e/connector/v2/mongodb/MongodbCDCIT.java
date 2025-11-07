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

import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Spark engine will lose the row kind of record")
@Slf4j
public class MongodbCDCIT extends AbstractMongodbIT {

    @TestTemplate
    public void testMongodbCDCUpsertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult queryResult =
                container.executeJob("/cdcIT/fake_cdc_upsert_sink_mongodb.conf");
        Assertions.assertEquals(0, queryResult.getExitCode(), queryResult.getStderr());
        Assertions.assertIterableEquals(
                Stream.<List<Object>>of(Arrays.asList(1L, "A_1", 100), Arrays.asList(3L, "C", 100))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_CDC_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .map(Document::entrySet)
                        .map(Set::stream)
                        .map(
                                entryStream ->
                                        entryStream
                                                .map(Map.Entry::getValue)
                                                .collect(Collectors.toCollection(ArrayList::new)))
                        .collect(Collectors.toList()));
        clearDate(MONGODB_CDC_RESULT_TABLE);
    }

    @TestTemplate
    public void testMongodbCDCSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult queryResult =
                container.executeJob("/cdcIT/fake_cdc_sink_mongodb.conf");
        Assertions.assertEquals(0, queryResult.getExitCode(), queryResult.getStderr());
        Assertions.assertIterableEquals(
                Stream.<List<Object>>of(Arrays.asList(1L, "A_1", 100), Arrays.asList(3L, "C", 100))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_CDC_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .map(Document::entrySet)
                        .map(Set::stream)
                        .map(
                                entryStream ->
                                        entryStream
                                                .map(Map.Entry::getValue)
                                                .collect(Collectors.toCollection(ArrayList::new)))
                        .collect(Collectors.toList()));
        clearDate(MONGODB_CDC_RESULT_TABLE);
    }
}
