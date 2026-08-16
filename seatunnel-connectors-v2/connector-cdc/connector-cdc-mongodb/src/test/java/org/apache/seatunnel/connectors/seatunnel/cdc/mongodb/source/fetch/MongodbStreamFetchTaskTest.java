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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.fetch;

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;

import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies bounded change-stream stop semantics. */
class MongodbStreamFetchTaskTest {

    @Test
    void testRecordAtStopOffsetIsEmittedBeforeStopping() {
        ChangeStreamOffset stopOffset = new ChangeStreamOffset(new BsonTimestamp(100, 1));
        ChangeStreamOffset currentOffset = new ChangeStreamOffset(new BsonTimestamp(100, 1));

        Assertions.assertTrue(MongodbStreamFetchTask.shouldEmit(currentOffset, stopOffset));
        Assertions.assertTrue(MongodbStreamFetchTask.hasReachedStop(currentOffset, stopOffset));
    }
}
