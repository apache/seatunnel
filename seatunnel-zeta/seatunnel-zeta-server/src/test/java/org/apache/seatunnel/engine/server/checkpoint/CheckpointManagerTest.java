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

package org.apache.seatunnel.zeta.server.checkpoint;

import static org.apache.seatunnel.zeta.common.Constant.IMAP_CHECKPOINT_ID;

import org.apache.seatunnel.zeta.checkpoint.storage.PipelineState;
import org.apache.seatunnel.zeta.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.zeta.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.zeta.checkpoint.storage.common.ProtoStuffSerializer;
import org.apache.seatunnel.zeta.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.zeta.common.config.server.CheckpointConfig;
import org.apache.seatunnel.zeta.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.zeta.common.utils.FactoryUtil;
import org.apache.seatunnel.zeta.core.checkpoint.CheckpointType;
import org.apache.seatunnel.zeta.core.job.JobStatus;
import org.apache.seatunnel.zeta.core.job.PipelineStatus;
import org.apache.seatunnel.zeta.server.AbstractSeaTunnelServerTest;

import com.hazelcast.map.IMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@DisabledOnOs(OS.WINDOWS)
public class CheckpointManagerTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testHAByIMapCheckpointIDCounter() throws CheckpointStorageException {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = FactoryUtil.discoverFactory(Thread.currentThread().getContextClassLoader(), CheckpointStorageFactory.class,
                new CheckpointStorageConfig().getStorage())
            .create(new HashMap<>());
        CompletedCheckpoint completedCheckpoint = new CompletedCheckpoint(jobId, 1, 1,
            Instant.now().toEpochMilli(),
            CheckpointType.COMPLETED_POINT_TYPE,
            Instant.now().toEpochMilli(),
            new HashMap<>(),
            new HashMap<>());
        checkpointStorage.storeCheckPoint(PipelineState.builder().jobId(jobId + "").pipelineId(1).checkpointId(1)
            .states(new ProtoStuffSerializer().serialize(completedCheckpoint)).build());
        IMap<Integer, Long> checkpointIdMap = nodeEngine.getHazelcastInstance().getMap(String.format(IMAP_CHECKPOINT_ID, jobId));
        checkpointIdMap.put(1, 2L);
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        planMap.put(1, CheckpointPlan.builder().pipelineId(1).build());
        CheckpointManager checkpointManager = new CheckpointManager(
            jobId,
            nodeEngine,
            null,
            planMap,
            new CheckpointConfig());
        Assertions.assertTrue(checkpointManager.isCompletedPipeline(1));
        checkpointManager.listenPipeline(1, PipelineStatus.FINISHED);
        Assertions.assertNull(checkpointIdMap.get(1));
        CompletableFuture<Void> future = checkpointManager.shutdown(JobStatus.FINISHED);
        future.join();
        Assertions.assertTrue(checkpointStorage.getAllCheckpoints(jobId + "").isEmpty());
    }
}
