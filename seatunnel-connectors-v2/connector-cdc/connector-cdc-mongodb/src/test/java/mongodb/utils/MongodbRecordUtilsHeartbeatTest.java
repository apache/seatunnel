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

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect.MongodbDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.fetch.MongodbFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamDescriptor;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.mongodb.client.MongoClient;

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
@ExtendWith(MockitoExtension.class)
public class MongodbRecordUtilsHeartbeatTest {

    @Mock private MongodbSourceConfig mockConfig;
    @Mock private MongodbDialect mockDialect;
    @Mock private ChangeStreamDescriptor mockDescriptor;
    @Mock private MongoClient mockMongoClient;

    private MockedStatic<MongodbUtils> mockedMongodbUtils;
    private MongodbFetchTaskContext fetchTaskContext;

    @BeforeEach
    void setUp() {
        mockedMongodbUtils = Mockito.mockStatic(MongodbUtils.class);
        mockedMongodbUtils
                .when(() -> MongodbUtils.createMongoClient(mockConfig))
                .thenReturn(mockMongoClient);
        fetchTaskContext = new MongodbFetchTaskContext(mockDialect, mockConfig, mockDescriptor);
    }

    @AfterEach
    void tearDown() {
        if (mockedMongodbUtils != null) {
            mockedMongodbUtils.close();
        }
    }

    private SourceRecord createHeartbeatRecord(boolean withHeartbeatFlag) {
        Map<String, Object> sourcePartition =
                Collections.singletonMap(
                        NS_FIELD, "mongodb://localhost:27017/__mongodb_heartbeats");

        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(ID_FIELD, "{\"_data\": \"test-resume-token\"}");
        if (withHeartbeatFlag) {
            sourceOffset.put(HEARTBEAT_KEY_FIELD, "true");
        }

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
        SourceRecord heartbeatRecord = createHeartbeatRecord(true);

        boolean result = MongodbRecordUtils.isHeartbeatEvent(heartbeatRecord);

        Assertions.assertTrue(result);
    }

    @Test
    @DisplayName("isDataChangeRecord should return false for heartbeat record with flag")
    void testIsDataChangeRecordReturnsFalseForHeartbeat() {
        SourceRecord heartbeatRecord = createHeartbeatRecord(true);

        boolean result = MongodbRecordUtils.isDataChangeRecord(heartbeatRecord);

        Assertions.assertFalse(result);
    }

    @Test
    @DisplayName("getDocumentKey should return null for heartbeat record (no documentKey field)")
    void testGetDocumentKeyReturnsNullForHeartbeatRecord() {
        SourceRecord heartbeatRecord = createHeartbeatRecord(true);

        BsonDocument documentKey = MongodbRecordUtils.getDocumentKey(heartbeatRecord);

        Assertions.assertNull(documentKey);
    }

    @Test
    @DisplayName(
            "isHeartbeatEvent should return false when offset lacks HEARTBEAT flag"
                    + " (old buggy heartbeat record)")
    void testIsHeartbeatEventReturnsFalseWithoutFlag() {
        SourceRecord heartbeatRecord = createHeartbeatRecord(false);

        boolean result = MongodbRecordUtils.isHeartbeatEvent(heartbeatRecord);

        Assertions.assertFalse(result);
    }

    @Test
    @DisplayName(
            "isDataChangeRecord incorrectly returns true for heartbeat record without flag"
                    + " (old buggy behavior)")
    void testIsDataChangeRecordReturnsTrueForHeartbeatWithoutFlag() {
        SourceRecord heartbeatRecord = createHeartbeatRecord(false);

        boolean result = MongodbRecordUtils.isDataChangeRecord(heartbeatRecord);

        // Without the HEARTBEAT flag, the record is misidentified as a data change record.
        // This demonstrates why the fix in normalizeHeartbeatRecord is necessary.
        Assertions.assertTrue(result);
    }

    @Test
    @DisplayName("isRecordBetween should return false for heartbeat record with null documentKey")
    void testIsRecordBetweenReturnsFalseForHeartbeat() {
        // Given
        SourceRecord heartbeatRecord = createHeartbeatRecord(true);

        BsonDocument splitKeyDoc = new BsonDocument("_id", new BsonInt32(1));
        BsonDocument lowerBound = new BsonDocument("_id", new BsonInt32(0));
        BsonDocument upperBound = new BsonDocument("_id", new BsonInt32(100));
        Object[] splitStart = new Object[] {splitKeyDoc, lowerBound};
        Object[] splitEnd = new Object[] {splitKeyDoc, upperBound};

        // When
        boolean result = fetchTaskContext.isRecordBetween(heartbeatRecord, splitStart, splitEnd);

        // Then
        Assertions.assertFalse(
                result,
                "isRecordBetween should return false for heartbeat record"
                        + " with null documentKey");
    }

    @Test
    @DisplayName(
            "Accessing documentKey on heartbeat record without flag would cause NPE"
                    + " (old buggy behavior)")
    void testNpeReproductionWithoutFlag() {
        SourceRecord heartbeatRecord = createHeartbeatRecord(false);

        BsonDocument documentKey = MongodbRecordUtils.getDocumentKey(heartbeatRecord);

        // documentKey is null because heartbeat value schema has no documentKey field.
        // Before the defensive fix in isRecordBetween, this would cause NPE.
        Assertions.assertNull(documentKey);
        Assertions.assertThrows(NullPointerException.class, () -> documentKey.get("_id"));
    }
}
