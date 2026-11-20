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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;
import org.apache.seatunnel.engine.checkpoint.storage.state.CheckpointFileLayout;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Restore-time selection between v1 blobs and v2 {@code _metadata} checkpoints (STIP-45). */
public class DualFormatCheckpointReaderTest {

    private static final String JOB_ID = "100";
    private static final String PIPELINE_ID = "1";

    @TempDir Path namespace;

    private LocalFileStorage storage;

    @BeforeEach
    public void setUp() {
        Map<String, String> config = new HashMap<>();
        config.put("namespace", namespace.toString());
        storage = new LocalFileStorage(config);
    }

    @Test
    public void testNoCheckpointReturnsNull() throws Exception {
        Assertions.assertNull(
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID));
        Assertions.assertTrue(
                DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID).isEmpty());
    }

    @Test
    public void testV1OnlyIsRestoredAsBefore() throws Exception {
        storeV1(3, "v1-3");

        PipelineState latest =
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);

        Assertions.assertEquals(3, latest.getCheckpointId());
        Assertions.assertEquals("v1-3", text(latest));
    }

    @Test
    public void testNewerV2WinsAndCarriesTheSamePayloadShape() throws Exception {
        storeV1(3, "v1-3");
        storeV2(4, "v2-4");

        PipelineState latest =
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);

        Assertions.assertEquals(4, latest.getCheckpointId());
        Assertions.assertEquals(JOB_ID, latest.getJobId());
        Assertions.assertEquals(1, latest.getPipelineId());
        Assertions.assertEquals("v2-4", text(latest));
    }

    @Test
    public void testNewerV1WinsOverOlderV2() throws Exception {
        storeV2(4, "v2-4");
        storeV1(5, "v1-5");

        Assertions.assertEquals(
                "v1-5",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));
    }

    @Test
    public void testCorruptedLatestMetadataFallsBackToOlderCheckpoint() throws Exception {
        storeV1(2, "v1-2");
        storeV2(6, "v2-6");
        storeV2(7, "v2-7");
        corruptMetadata(7);

        PipelineState latest =
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);
        Assertions.assertEquals("v2-6", text(latest));

        corruptMetadata(6);
        latest = DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);
        Assertions.assertEquals("v1-2", text(latest));
    }

    @Test
    public void testIncompleteV2CheckpointIsIgnored() throws Exception {
        storeV1(2, "v1-2");
        // a chk dir without _metadata: the master crashed before committing it
        Files.createDirectories(
                namespace.resolve(CheckpointFileLayout.checkpointDir(JOB_ID, PIPELINE_ID, 9)));

        Assertions.assertEquals(
                "v1-2",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));
    }

    @Test
    public void testOnlyIncompleteV2CheckpointsMeansNoCheckpoint() throws Exception {
        // never committed: nothing to restore, which is not a failure
        Files.createDirectories(
                namespace.resolve(CheckpointFileLayout.checkpointDir(JOB_ID, PIPELINE_ID, 9)));

        Assertions.assertNull(
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID));
    }

    @Test
    public void testAllCompletedCheckpointsUnreadableFailsClosed() throws Exception {
        storeV2(6, "v2-6");
        storeV2(7, "v2-7");
        corruptMetadata(6);
        corruptMetadata(7);

        // returning null here would restart the job from its initial position
        CheckpointStorageException latestFailure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                DualFormatCheckpointReader.getLatestCheckpoint(
                                        storage, JOB_ID, PIPELINE_ID));
        Assertions.assertTrue(latestFailure.getMessage().contains("Job " + JOB_ID));
        Assertions.assertTrue(latestFailure.getMessage().contains("pipeline " + PIPELINE_ID));
        Assertions.assertTrue(latestFailure.getMessage().contains("v2 checkpoint 7"));
        Assertions.assertTrue(latestFailure.getMessage().contains("v2 checkpoint 6"));

        Assertions.assertThrows(
                CheckpointStorageException.class,
                () -> DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID));
    }

    @Test
    public void testUnreadableV1DoesNotHideReadableV2() throws Exception {
        storeV2(4, "v2-4");
        LocalFileStorage brokenV1 = Mockito.spy(storage);
        Mockito.doThrow(new CheckpointStorageException("corrupt v1 blob"))
                .when(brokenV1)
                .getLatestCheckpointByJobIdAndPipelineId(JOB_ID, PIPELINE_ID);
        Mockito.doThrow(new IllegalStateException("corrupt v1 blob"))
                .when(brokenV1)
                .getCheckpointsByJobIdAndPipelineId(JOB_ID, PIPELINE_ID);

        Assertions.assertEquals(
                "v2-4",
                text(
                        DualFormatCheckpointReader.getLatestCheckpoint(
                                brokenV1, JOB_ID, PIPELINE_ID)));
        Assertions.assertEquals(
                "v2-4",
                text(
                        DualFormatCheckpointReader.getCheckpoints(brokenV1, JOB_ID, PIPELINE_ID)
                                .get(0)));
    }

    @Test
    public void testUnreadableV1AndNoV2FailsClosed() throws Exception {
        LocalFileStorage brokenV1 = Mockito.spy(storage);
        Mockito.doThrow(new CheckpointStorageException("corrupt v1 blob"))
                .when(brokenV1)
                .getLatestCheckpointByJobIdAndPipelineId(JOB_ID, PIPELINE_ID);

        CheckpointStorageException failure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                DualFormatCheckpointReader.getLatestCheckpoint(
                                        brokenV1, JOB_ID, PIPELINE_ID));
        Assertions.assertTrue(failure.getMessage().contains("corrupt v1 blob"));
    }

    @Test
    public void testGetCheckpointsMergesBothFormatsAndSkipsCorrupted() throws Exception {
        storeV1(1, "v1-1");
        storeV2(2, "v2-2");
        storeV2(3, "v2-3");
        corruptMetadata(3);

        List<String> payloads =
                DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID).stream()
                        .map(DualFormatCheckpointReaderTest::text)
                        .sorted()
                        .collect(Collectors.toList());

        Assertions.assertEquals(Arrays.asList("v1-1", "v2-2"), payloads);
    }

    @Test
    public void testStorageWithoutChunkSupportOnlyUsesV1() throws Exception {
        CheckpointStorage legacy = Mockito.mock(CheckpointStorage.class);
        PipelineState v1 =
                PipelineState.builder()
                        .jobId(JOB_ID)
                        .pipelineId(1)
                        .checkpointId(3)
                        .states(new byte[0])
                        .build();
        Mockito.when(legacy.getLatestCheckpointByJobIdAndPipelineId(JOB_ID, PIPELINE_ID))
                .thenReturn(v1);
        Mockito.when(legacy.getCheckpointsByJobIdAndPipelineId(JOB_ID, PIPELINE_ID))
                .thenReturn(Collections.singletonList(v1));

        Assertions.assertSame(
                v1, DualFormatCheckpointReader.getLatestCheckpoint(legacy, JOB_ID, PIPELINE_ID));
        Assertions.assertEquals(
                1, DualFormatCheckpointReader.getCheckpoints(legacy, JOB_ID, PIPELINE_ID).size());
    }

    private void storeV1(long checkpointId, String payload) throws Exception {
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(JOB_ID)
                        .pipelineId(Integer.parseInt(PIPELINE_ID))
                        .checkpointId(checkpointId)
                        .states(payload.getBytes(StandardCharsets.UTF_8))
                        .build());
    }

    private void storeV2(long checkpointId, String payload) throws Exception {
        storage.writeMetadata(
                JOB_ID,
                PIPELINE_ID,
                checkpointId,
                MetadataEnvelope.of(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private void corruptMetadata(long checkpointId) throws Exception {
        Path metadata =
                namespace.resolve(
                        CheckpointFileLayout.metadataPath(JOB_ID, PIPELINE_ID, checkpointId));
        byte[] bytes = Files.readAllBytes(metadata);
        bytes[bytes.length - 1] ^= 0x01;
        Files.write(metadata, bytes);
    }

    private static String text(PipelineState state) {
        return new String(state.getStates(), StandardCharsets.UTF_8);
    }
}
