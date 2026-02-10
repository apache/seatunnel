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

package mongodb.utils;

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.HEARTBEAT_KEY_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.TS_MS_FIELD;

/**
 * Tests for heartbeat record handling in MongoDB CDC.
 *
 * <p>Verifies that heartbeat records (produced when {@code heartbeat.interval.ms > 0}) are
 * correctly identified by {@link MongodbRecordUtils#isHeartbeatEvent} and excluded from data change
 * processing by {@link MongodbRecordUtils#isDataChangeRecord}.
 */
public class MongodbRecordUtilsHeartbeatTest {

    /**
     * Build a SourceRecord that simulates a correctly normalized heartbeat record with {@code
     * HEARTBEAT=true} in offset. This is the expected output of the fixed {@code
     * MongodbStreamFetchTask.normalizeHeartbeatRecord()}.
     */
    private SourceRecord createHeartbeatRecordWithFlag() {
        Map<String, Object> sourcePartition =
                Collections.singletonMap(
                        NS_FIELD, "mongodb://localhost:27017/__mongodb_heartbeats");

        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(ID_FIELD, "{\"_data\": \"test-resume-token\"}");
        sourceOffset.put(HEARTBEAT_KEY_FIELD, "true");

        Schema valueSchema = SchemaBuilder.struct().field(TS_MS_FIELD, Schema.INT64_SCHEMA).build();
        Struct heartbeatValue = new Struct(valueSchema);
        heartbeatValue.put(TS_MS_FIELD, Instant.now().toEpochMilli());

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                "__mongodb_heartbeats",
                null,
                null,
                valueSchema,
                heartbeatValue);
    }

    /**
     * Build a SourceRecord that simulates a heartbeat record WITHOUT the {@code HEARTBEAT=true}
     * flag in offset. This represents the old buggy behavior before the fix.
     */
    private SourceRecord createHeartbeatRecordWithoutFlag() {
        Map<String, Object> sourcePartition =
                Collections.singletonMap(
                        NS_FIELD, "mongodb://localhost:27017/__mongodb_heartbeats");

        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(ID_FIELD, "{\"_data\": \"test-resume-token\"}");

        Schema valueSchema = SchemaBuilder.struct().field(TS_MS_FIELD, Schema.INT64_SCHEMA).build();
        Struct heartbeatValue = new Struct(valueSchema);
        heartbeatValue.put(TS_MS_FIELD, Instant.now().toEpochMilli());

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                "__mongodb_heartbeats",
                null,
                null,
                valueSchema,
                heartbeatValue);
    }

    @Test
    @DisplayName("isHeartbeatEvent should return true when offset contains HEARTBEAT=true")
    void testIsHeartbeatEventReturnsTrueWithFlag() {
        SourceRecord heartbeatRecord = createHeartbeatRecordWithFlag();

        boolean result = MongodbRecordUtils.isHeartbeatEvent(heartbeatRecord);

        Assertions.assertTrue(result);
    }

    @Test
    @DisplayName("isDataChangeRecord should return false for heartbeat record with flag")
    void testIsDataChangeRecordReturnsFalseForHeartbeat() {
        SourceRecord heartbeatRecord = createHeartbeatRecordWithFlag();

        boolean result = MongodbRecordUtils.isDataChangeRecord(heartbeatRecord);

        Assertions.assertFalse(result);
    }

    @Test
    @DisplayName("getDocumentKey should return null for heartbeat record (no documentKey field)")
    void testGetDocumentKeyReturnsNullForHeartbeatRecord() {
        SourceRecord heartbeatRecord = createHeartbeatRecordWithFlag();

        BsonDocument documentKey = MongodbRecordUtils.getDocumentKey(heartbeatRecord);

        Assertions.assertNull(documentKey);
    }

    @Test
    @DisplayName(
            "isHeartbeatEvent should return false when offset lacks HEARTBEAT flag"
                    + " (old buggy heartbeat record)")
    void testIsHeartbeatEventReturnsFalseWithoutFlag() {
        SourceRecord heartbeatRecord = createHeartbeatRecordWithoutFlag();

        boolean result = MongodbRecordUtils.isHeartbeatEvent(heartbeatRecord);

        Assertions.assertFalse(result);
    }

    @Test
    @DisplayName(
            "isDataChangeRecord incorrectly returns true for heartbeat record without flag"
                    + " (old buggy behavior)")
    void testIsDataChangeRecordReturnsTrueForHeartbeatWithoutFlag() {
        SourceRecord heartbeatRecord = createHeartbeatRecordWithoutFlag();

        boolean result = MongodbRecordUtils.isDataChangeRecord(heartbeatRecord);

        // Without the HEARTBEAT flag, the record is misidentified as a data change record.
        // This demonstrates why the fix in normalizeHeartbeatRecord is necessary.
        Assertions.assertTrue(result);
    }

    @Test
    @DisplayName(
            "Accessing documentKey on heartbeat record without flag would cause NPE"
                    + " (old buggy behavior)")
    void testNpeReproductionWithoutFlag() {
        SourceRecord heartbeatRecord = createHeartbeatRecordWithoutFlag();

        BsonDocument documentKey = MongodbRecordUtils.getDocumentKey(heartbeatRecord);

        // documentKey is null because heartbeat value schema has no documentKey field.
        // Before the defensive fix in isRecordBetween, this would cause NPE.
        Assertions.assertNull(documentKey);
        Assertions.assertThrows(NullPointerException.class, () -> documentKey.get("_id"));
    }
}
