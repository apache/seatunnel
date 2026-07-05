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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.common.utils.SerializationUtils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** {@link BigtableSourceSplit} 续读进度与序列化兼容性测试。 */
class BigtableSourceSplitTest {

    @Test
    void resumeStartRowKeyFallsBackToSplitStartWhenNoProgress() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "a", "z");
        assertEquals("a", split.getResumeStartRowKey());
    }

    @Test
    void resumeStartRowKeyUsesLastReadRowKeyWhenPresent() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "a", "z", "row-490");
        assertEquals("row-490", split.getResumeStartRowKey());
    }

    @Test
    void lastReadRowKeyCanBeUpdatedDuringRead() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "", "");
        split.setLastReadRowKey("key-42");
        assertEquals("key-42", split.getLastReadRowKey());
        assertEquals("key-42", split.getResumeStartRowKey());
    }

    @Test
    void checkpointRoundTripPreservesLastReadRowKey() {
        BigtableSourceSplit original = new BigtableSourceSplit(0, "start", "end", "resume-here");
        byte[] bytes = SerializationUtils.serialize(original);
        BigtableSourceSplit restored = SerializationUtils.deserialize(bytes);
        assertEquals(original.splitId(), restored.splitId());
        assertEquals("start", restored.getStartRowKey());
        assertEquals("end", restored.getEndRowKey());
        assertEquals("resume-here", restored.getLastReadRowKey());
        assertEquals("resume-here", restored.getResumeStartRowKey());
    }

    @Test
    void legacySplitWithoutProgressDeserializesWithNullLastReadRowKey() {
        BigtableSourceSplit legacy = new BigtableSourceSplit(1, "x", "y");
        byte[] bytes = SerializationUtils.serialize(legacy);
        BigtableSourceSplit restored = SerializationUtils.deserialize(bytes);
        assertNull(restored.getLastReadRowKey());
        assertEquals("x", restored.getResumeStartRowKey());
    }
}
