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

package org.apache.seatunnel.benchmark.storage.checkpoint;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.HdfsStorage;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** Captures real coordinator output while delegating persistence to the production HDFS plugin. */
public final class BenchmarkCheckpointStorageFactory implements CheckpointStorageFactory {

    private static final AtomicReference<PipelineState> LATEST_FIXTURE = new AtomicReference<>();

    @Override
    public String factoryIdentifier() {
        return "benchmark";
    }

    @Override
    public CheckpointStorage create(Map<String, String> configuration)
            throws CheckpointStorageException {
        return new CapturingCheckpointStorage(new HdfsStorage(new HashMap<>(configuration)));
    }

    static void clearFixture() {
        LATEST_FIXTURE.set(null);
    }

    static PipelineState latestFixture() {
        PipelineState fixture = LATEST_FIXTURE.get();
        if (fixture == null) {
            throw new IllegalStateException("A real checkpoint fixture has not been captured");
        }
        return copy(fixture);
    }

    static CompletedCheckpoint latestCompletedCheckpoint() {
        PipelineState fixture = latestFixture();
        return new ProtoStuffSerializer()
                .deserialize(fixture.getStates(), CompletedCheckpoint.class);
    }

    private static void capture(PipelineState state) {
        LATEST_FIXTURE.set(copy(state));
    }

    private static PipelineState copy(PipelineState state) {
        return PipelineState.builder()
                .jobId(state.getJobId())
                .pipelineId(state.getPipelineId())
                .checkpointId(state.getCheckpointId())
                .states(state.getStates().clone())
                .build();
    }

    private static final class CapturingCheckpointStorage implements CheckpointStorage {

        private final CheckpointStorage delegate;

        private CapturingCheckpointStorage(CheckpointStorage delegate) {
            this.delegate = delegate;
        }

        @Override
        public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
            String result = delegate.storeCheckPoint(state);
            capture(state);
            return result;
        }

        @Override
        public void asyncStoreCheckPoint(PipelineState state) throws CheckpointStorageException {
            delegate.asyncStoreCheckPoint(state);
            capture(state);
        }

        @Override
        public List<PipelineState> getAllCheckpoints(String jobId)
                throws CheckpointStorageException {
            return delegate.getAllCheckpoints(jobId);
        }

        @Override
        public List<PipelineState> getLatestCheckpoint(String jobId)
                throws CheckpointStorageException {
            return delegate.getLatestCheckpoint(jobId);
        }

        @Override
        public PipelineState getLatestCheckpointByJobIdAndPipelineId(
                String jobId, String pipelineId) throws CheckpointStorageException {
            return delegate.getLatestCheckpointByJobIdAndPipelineId(jobId, pipelineId);
        }

        @Override
        public List<PipelineState> getCheckpointsByJobIdAndPipelineId(
                String jobId, String pipelineId) throws CheckpointStorageException {
            return delegate.getCheckpointsByJobIdAndPipelineId(jobId, pipelineId);
        }

        @Override
        public void deleteCheckpoint(String jobId) {
            delegate.deleteCheckpoint(jobId);
        }

        @Override
        public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId)
                throws CheckpointStorageException {
            return delegate.getCheckpoint(jobId, pipelineId, checkpointId);
        }

        @Override
        public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId)
                throws CheckpointStorageException {
            delegate.deleteCheckpoint(jobId, pipelineId, checkpointId);
        }

        @Override
        public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIdList)
                throws CheckpointStorageException {
            delegate.deleteCheckpoint(jobId, pipelineId, checkpointIdList);
        }
    }
}
