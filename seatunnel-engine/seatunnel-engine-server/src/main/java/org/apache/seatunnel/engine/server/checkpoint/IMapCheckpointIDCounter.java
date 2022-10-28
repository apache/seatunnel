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

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.job.PipelineStatus;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.map.IMap;

import java.util.concurrent.CompletableFuture;

public class IMapCheckpointIDCounter implements CheckpointIDCounter {
    private final Integer pipelineId;
    private final IMap<Integer, Long> checkpointIdMap;

    public IMapCheckpointIDCounter(Integer pipelineId,
                                   IMap<Integer, Long> checkpointIdMap) {
        this.pipelineId = pipelineId;
        this.checkpointIdMap = checkpointIdMap;
    }

    @Override
    public void start() throws Exception {
        RetryUtils.retryWithException(() -> {
            return checkpointIdMap.putIfAbsent(pipelineId, INITIAL_CHECKPOINT_ID);
        }, new RetryUtils.RetryMaterial(Constant.OPERATION_RETRY_TIME, true,
            exception -> exception instanceof HazelcastInstanceNotActiveException, Constant.OPERATION_RETRY_SLEEP));
    }

    @Override
    public CompletableFuture<Void> shutdown(PipelineStatus pipelineStatus) {
        if (pipelineStatus.isEndState()) {
            checkpointIdMap.remove(pipelineId);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getAndIncrement() throws Exception {
        Long currentId = checkpointIdMap.get(pipelineId);
        checkpointIdMap.put(pipelineId, currentId + 1);
        return currentId;
    }

    @Override
    public long get() {
        return checkpointIdMap.get(pipelineId);
    }

    @Override
    public void setCount(long newId) throws Exception {
        checkpointIdMap.put(pipelineId, newId);
    }
}
