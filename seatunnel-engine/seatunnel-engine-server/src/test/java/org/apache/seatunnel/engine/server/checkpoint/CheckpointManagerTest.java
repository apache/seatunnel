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
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.map.IMap;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.seatunnel.engine.common.Constant.IMAP_CHECKPOINT_ID;
import static org.apache.seatunnel.engine.common.Constant.IMAP_RUNNING_JOB_STATE;

@DisabledOnOs(OS.WINDOWS)
@Disabled
public class CheckpointManagerTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testHAByIMapCheckpointIDCounter() throws CheckpointStorageException {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage =
                FactoryUtil.discoverFactory(
                                Thread.currentThread().getContextClassLoader(),
                                CheckpointStorageFactory.class,
                                new CheckpointStorageConfig().getStorage())
                        .create(new HashMap<>());
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        jobId,
                        1,
                        1,
                        Instant.now().toEpochMilli(),
                        CheckpointType.COMPLETED_POINT_TYPE,
                        Instant.now().toEpochMilli(),
                        new HashMap<>(),
                        new HashMap<>());
        checkpointStorage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(jobId + "")
                        .pipelineId(1)
                        .checkpointId(1)
                        .states(new ProtoStuffSerializer().serialize(completedCheckpoint))
                        .build());
        IMap<Integer, Long> checkpointIdMap =
                nodeEngine.getHazelcastInstance().getMap(String.format(IMAP_CHECKPOINT_ID, jobId));
        checkpointIdMap.put(1, 2L);
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        planMap.put(1, CheckpointPlan.builder().pipelineId(1).build());
        CheckpointManager checkpointManager =
                new CheckpointManager(
                        jobId,
                        false,
                        nodeEngine,
                        null,
                        planMap,
                        new CheckpointConfig(),
                        instance.getExecutorService("test"),
                        nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE));
        Assertions.assertTrue(checkpointManager.isCompletedPipeline(1));
        checkpointManager.listenPipeline(1, PipelineStatus.FINISHED);
        Assertions.assertNull(checkpointIdMap.get(1));
        CompletableFuture<Void> future = checkpointManager.shutdown(JobStatus.FINISHED);
        future.join();
        Assertions.assertTrue(checkpointStorage.getAllCheckpoints(jobId + "").isEmpty());
    }
}
