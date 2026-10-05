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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class CheckpointServiceTest {

    @Test
    void checkpointRestoreShouldSelectLatestCandidateAcrossCheckpointTypes() throws Exception {
        CheckpointService checkpointService = new CheckpointService(new CheckpointConfig());
        CheckpointStorage checkpointStorage =
                stubCheckpointStorage(
                        Arrays.asList(
                                toPipelineState(
                                        completedCheckpoint(
                                                "job-1", 1, 1L, CheckpointType.CHECKPOINT_TYPE)),
                                toPipelineState(
                                        completedCheckpoint(
                                                "job-1",
                                                1,
                                                Long.MAX_VALUE,
                                                CheckpointType.COMPLETED_POINT_TYPE))));
        setField(checkpointService, "checkpointStorage", checkpointStorage);

        List<JobPipelineCheckpointData> pipelineCheckpoints =
                checkpointService.getLatestCheckpointData("job-1", RestoreMode.CHECKPOINT);

        Assertions.assertEquals(1, pipelineCheckpoints.size());
        Assertions.assertEquals(Long.MAX_VALUE, pipelineCheckpoints.get(0).getCheckpointId());
        Assertions.assertEquals(
                CheckpointType.COMPLETED_POINT_TYPE,
                pipelineCheckpoints.get(0).getCheckpointType());
    }

    @Test
    void checkpointRestoreShouldIgnoreSavepointType() throws Exception {
        CheckpointService checkpointService = new CheckpointService(new CheckpointConfig());
        CheckpointStorage checkpointStorage =
                stubCheckpointStorage(
                        Collections.singletonList(
                                toPipelineState(
                                        completedCheckpoint(
                                                "job-2", 1, 11L, CheckpointType.SAVEPOINT_TYPE))));
        setField(checkpointService, "checkpointStorage", checkpointStorage);

        List<JobPipelineCheckpointData> pipelineCheckpoints =
                checkpointService.getLatestCheckpointData("job-2", RestoreMode.CHECKPOINT);

        Assertions.assertTrue(pipelineCheckpoints.isEmpty());
    }

    private static CompletedCheckpoint completedCheckpoint(
            String jobId, int pipelineId, long checkpointId, CheckpointType checkpointType) {
        long now = System.currentTimeMillis();
        return new CompletedCheckpoint(
                Long.parseLong(jobId.substring(jobId.indexOf('-') + 1)),
                pipelineId,
                checkpointId,
                now,
                checkpointType,
                now,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    private static PipelineState toPipelineState(CompletedCheckpoint checkpoint) throws Exception {
        return PipelineState.builder()
                .jobId(String.valueOf(checkpoint.getJobId()))
                .pipelineId(checkpoint.getPipelineId())
                .checkpointId(checkpoint.getCheckpointId())
                .states(new ProtoStuffSerializer().serialize(checkpoint))
                .build();
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = CheckpointService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static CheckpointStorage stubCheckpointStorage(List<PipelineState> pipelineStates) {
        return new CheckpointStorage() {
            @Override
            public String storeCheckPoint(PipelineState state) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void asyncStoreCheckPoint(PipelineState state) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<PipelineState> getAllCheckpoints(String jobId) {
                return pipelineStates;
            }

            @Override
            public List<PipelineState> getLatestCheckpoint(String jobId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public PipelineState getLatestCheckpointByJobIdAndPipelineId(
                    String jobId, String pipelineId) throws CheckpointStorageException {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<PipelineState> getCheckpointsByJobIdAndPipelineId(
                    String jobId, String pipelineId) throws CheckpointStorageException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteCheckpoint(String jobId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId)
                    throws CheckpointStorageException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId)
                    throws CheckpointStorageException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteCheckpoint(
                    String jobId, String pipelineId, List<String> checkpointIdList)
                    throws CheckpointStorageException {
                throw new UnsupportedOperationException();
            }
        };
    }
}
