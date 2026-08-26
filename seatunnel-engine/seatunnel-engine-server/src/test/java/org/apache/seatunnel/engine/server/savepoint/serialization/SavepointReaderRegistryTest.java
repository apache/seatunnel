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

package org.apache.seatunnel.engine.server.savepoint.serialization;

import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointManifestEntry;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageUtils;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Version-compatibility matrix for the savepoint reader registry: version 1 is readable, versions
 * outside the supported window fail with an explicit, actionable error.
 */
public class SavepointReaderRegistryTest {

    @Test
    public void v1BundleIsReadable() {
        byte[] payload =
                SavepointWireCodec.encode(
                        SavepointWireCodec.fromCompletedCheckpoint(
                                SavepointWireFixtures.sampleCompletedCheckpoint()));
        SavepointManifestEntry entry =
                new SavepointManifestEntry(
                        SavepointWireFixtures.PIPELINE_ID,
                        SavepointWireFixtures.CHECKPOINT_ID,
                        SavepointWireFixtures.PIPELINE_ID
                                + "-"
                                + SavepointWireFixtures.CHECKPOINT_ID
                                + ".ser",
                        payload.length,
                        SavepointStorageUtils.sha256Hex(payload),
                        SavepointStorageConstants.PAYLOAD_FORMAT_V1);
        SavepointMeta meta =
                new SavepointMeta(
                        1,
                        "1000",
                        String.valueOf(SavepointWireFixtures.JOB_ID),
                        "test",
                        2000L,
                        Collections.singletonList(entry),
                        SavepointStorageUtils.manifestChecksum(Collections.singletonList(entry)));
        Map<Integer, byte[]> payloads = new HashMap<>();
        payloads.put(0, payload);

        Map<Integer, CompletedCheckpoint> restored =
                SavepointReaderRegistry.forVersion(meta).read(meta, payloads);

        Assertions.assertEquals(1, restored.size());
        CompletedCheckpoint checkpoint = restored.get(0);
        Assertions.assertEquals(SavepointWireFixtures.JOB_ID, checkpoint.getJobId());
        Assertions.assertEquals(CheckpointType.SAVEPOINT_TYPE, checkpoint.getCheckpointType());
        Assertions.assertFalse(checkpoint.isRestored());
    }

    @Test
    public void versionBelowWindowRequiresMigration() {
        SavepointMeta meta = new SavepointMeta(0, "1000", "1", "test", 2000L, null, "checksum");
        SavepointIncompatibleException exception =
                Assertions.assertThrows(
                        SavepointIncompatibleException.class,
                        () -> SavepointReaderRegistry.forVersion(meta));
        Assertions.assertEquals("1000", exception.getSavepointId());
        Assertions.assertEquals(0, exception.getFormatVersion());
        Assertions.assertTrue(exception.getMessage().contains("migration"));
        Assertions.assertTrue(exception.getMessage().contains("supported"));
    }

    @Test
    public void versionAboveWindowRejectedExplicitly() {
        SavepointMeta meta = new SavepointMeta(99, "1000", "1", "test", 2000L, null, "checksum");
        SavepointIncompatibleException exception =
                Assertions.assertThrows(
                        SavepointIncompatibleException.class,
                        () -> SavepointReaderRegistry.forVersion(meta));
        Assertions.assertEquals(99, exception.getFormatVersion());
        Assertions.assertTrue(exception.getMessage().contains("newer engine"));
        Assertions.assertTrue(exception.getMessage().contains("supported"));
    }
}
