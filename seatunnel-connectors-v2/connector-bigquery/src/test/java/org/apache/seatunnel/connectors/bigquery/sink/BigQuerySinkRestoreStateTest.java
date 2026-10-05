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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BigQuerySinkRestoreStateTest {
    @Test
    void testGetLatestStateByCheckpointId() {
        BigQuerySinkState state1 = new BigQuerySinkState("stream-1", 100L, 1L);
        BigQuerySinkState state3 = new BigQuerySinkState("stream-3", 300L, 3L);
        BigQuerySinkState state2 = new BigQuerySinkState("stream-2", 200L, 2L);

        BigQuerySinkState latest =
                BigQuerySink.getLatestState(Arrays.asList(state1, state3, state2));

        assertEquals("stream-3", latest.getStreamName());
        assertEquals(300L, latest.getNextOffset());
        assertEquals(3L, latest.getCheckpointId());
    }
}
