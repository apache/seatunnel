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

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.stream.Collectors;

@Slf4j
public class MongodbIT extends AbstractMongodbIT {

    @TestTemplate
    public void testMongodbSourceAndSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult insertResult = container.executeJob("/fake_source_to_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult assertResult = container.executeJob("/mongodb_source_to_assert.conf");
        Assertions.assertEquals(0, assertResult.getExitCode(), assertResult.getStderr());
        clearDate(MONGODB_SINK_TABLE);
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
        clearDate(MONGODB_MATCH_RESULT_TABLE);

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
        clearDate(MONGODB_MATCH_RESULT_TABLE);
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

        clearDate(MONGODB_UPDATE_TABLE);
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

        clearDate(MONGODB_FLAT_TABLE);
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
        clearDate(MONGODB_SPLIT_RESULT_TABLE);

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
        clearDate(MONGODB_SPLIT_RESULT_TABLE);
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

        clearDate(MONGODB_UPDATE_TABLE);

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
        clearDate(MONGODB_MATCH_RESULT_TABLE);
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

        clearDate(MONGODB_TRANSACTION_SINK_TABLE);
        clearDate(MONGODB_TRANSACTION_UPSERT_TABLE);
    }
}
