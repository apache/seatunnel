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

import org.apache.commons.codec.binary.Base64;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for {@link BigtableSourceSplit} resume progress and checkpoint serialization. */
class BigtableSourceSplitTest {

    /**
     * Bytes produced by {@code BigtableSourceSplit(1, "x", "y")} on dev before {@code
     * lastReadRowKey} existed (serialVersionUID = 1L, three fields only).
     */
    private static final String LEGACY_SPLIT_V1_BASE64 =
            "rO0ABXNyAE1vcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5iaWd0YWJsZS5zb3VyY2UuQmlndGFibGVTb3VyY2VTcGxpdAAAAAAAAAABAgADTAAJZW5kUm93S2V5dAASTGphdmEvbGFuZy9TdHJpbmc7TAAHc3BsaXRJZHEAfgABTAALc3RhcnRSb3dLZXlxAH4AAXhwdAABeXQAF2JpZ3RhYmxlX3NvdXJjZV9zcGxpdF8xdAABeA==";

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
    void deserializesLegacyCheckpointBytesFromDevRelease() {
        byte[] legacyBytes = Base64.decodeBase64(LEGACY_SPLIT_V1_BASE64);
        BigtableSourceSplit restored = SerializationUtils.deserialize(legacyBytes);
        assertEquals("bigtable_source_split_1", restored.splitId());
        assertEquals("x", restored.getStartRowKey());
        assertEquals("y", restored.getEndRowKey());
        assertNull(restored.getLastReadRowKey());
        assertEquals("x", restored.getResumeStartRowKey());
    }
}
