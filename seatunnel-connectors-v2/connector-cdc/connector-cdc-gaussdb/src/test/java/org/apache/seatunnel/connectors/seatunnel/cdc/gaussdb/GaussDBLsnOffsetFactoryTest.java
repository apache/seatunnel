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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;

import java.util.HashMap;
import java.util.Map;

/** Tests that GaussDB checkpoints retain only transaction-safe LSN positions. */
class GaussDBLsnOffsetFactoryTest {

    /** Verifies an in-transaction row LSN is replaced by the complete transaction boundary. */
    @Test
    void testUseLastCompletelyProcessedLsnForCheckpoint() {
        Map<String, String> recordOffset = new HashMap<>();
        recordOffset.put(SourceInfo.LSN_KEY, "120");
        recordOffset.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, "100");

        Map<String, String> checkpointOffset =
                GaussDBLsnOffsetFactory.toCheckpointOffset(recordOffset);

        Assertions.assertEquals("100", checkpointOffset.get(SourceInfo.LSN_KEY));
        Assertions.assertEquals("120", recordOffset.get(SourceInfo.LSN_KEY));
    }

    /** Verifies legacy offsets without lsn_proc retain their primary LSN. */
    @Test
    void testRetainLegacyPrimaryLsn() {
        Map<String, String> recordOffset = new HashMap<>();
        recordOffset.put(SourceInfo.LSN_KEY, "90");

        Map<String, String> checkpointOffset =
                GaussDBLsnOffsetFactory.toCheckpointOffset(recordOffset);

        Assertions.assertEquals("90", checkpointOffset.get(SourceInfo.LSN_KEY));
    }
}
