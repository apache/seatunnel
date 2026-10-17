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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.HashMap;
import java.util.Map;

/**
 * Checkpoint storage with {@code storage.type: azure} against Azurite: write, list, restore and
 * delete through the inherited tests plus {@link #testRestoreAndDeleteCheckpoint()}.
 */
@EnabledIf(
        value = "isDockerAvailable",
        disabledReason = "The Azure storage tests run Azurite and need Docker")
public class AzureFileCheckpointTest extends AbstractFileCheckPointTest {

    private static AzuriteEmulator azurite;

    static boolean isDockerAvailable() {
        return AzuriteEmulator.isDockerAvailable();
    }

    @BeforeAll
    public static void setup() throws CheckpointStorageException {
        azurite = AzuriteEmulator.start();
        Map<String, String> config = new HashMap<>();
        config.put("storage.type", "azure");
        config.put("azure.container", AzuriteEmulator.CONTAINER_URI);
        config.put("namespace", "/seatunnel-azurite/checkpoint/");
        STORAGE = new HdfsStorage(config);
        initStorageData();
    }

    @Test
    public void testRestoreAndDeleteCheckpoint() throws CheckpointStorageException {
        String jobId = "azurite-restore-delete";
        byte[] states = {1, 2, 3};
        STORAGE.storeCheckPoint(
                PipelineState.builder()
                        .jobId(jobId)
                        .pipelineId(1)
                        .checkpointId(7)
                        .states(states)
                        .build());

        PipelineState restored = STORAGE.getLatestCheckpointByJobIdAndPipelineId(jobId, "1");
        Assertions.assertEquals(7, restored.getCheckpointId());
        Assertions.assertArrayEquals(states, restored.getStates());

        STORAGE.deleteCheckpoint(jobId);
        Assertions.assertTrue(STORAGE.getAllCheckpoints(jobId).isEmpty());
    }

    /** Hides {@link AbstractFileCheckPointTest#teardown()} so the emulator stops after cleanup. */
    @AfterAll
    public static void teardown() {
        try {
            if (STORAGE != null) {
                STORAGE.deleteCheckpoint(JOB_ID);
            }
        } finally {
            if (azurite != null) {
                azurite.close();
            }
        }
    }
}
