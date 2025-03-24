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
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.job.PipelineStatus;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;

import java.nio.ByteBuffer;
import java.util.Base64;

import static org.apache.seatunnel.engine.common.Constant.IMAP_CHECKPOINT_ID;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class IMapCheckpointIDCounter implements CheckpointIDCounter {

    private final Long jobID;
    private final Integer pipelineId;
    private final String key;
    private final IMap<String, Long> checkpointIdMap;

    public IMapCheckpointIDCounter(Long jobID, Integer pipelineId, NodeEngine nodeEngine) {
        this.jobID = jobID;
        this.pipelineId = pipelineId;
        this.key = convertLongIntToBase64(jobID, pipelineId);
        this.checkpointIdMap = nodeEngine.getHazelcastInstance().getMap(IMAP_CHECKPOINT_ID);
    }

    @Override
    public void start() throws Exception {
        RetryUtils.retryWithException(
                () -> {
                    return checkpointIdMap.putIfAbsent(key, INITIAL_CHECKPOINT_ID);
                },
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                        Constant.OPERATION_RETRY_SLEEP));
    }

    @Override
    public CompletableFuture<Void> shutdown(PipelineStatus pipelineStatus) {
        if (pipelineStatus.isEndState()) {
            checkpointIdMap.remove(key);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getAndIncrement() throws Exception {
        Long nextId = checkpointIdMap.compute(key, (k, v) -> v == null ? null : v + 1);
        checkNotNull(nextId);
        return nextId - 1;
    }

    @Override
    public long get() {
        return checkpointIdMap.get(key);
    }

    @Override
    public void setCount(long newId) throws Exception {
        checkpointIdMap.put(key, newId);
    }

    public static String convertLongIntToBase64(long longValue, int intValue) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        buffer.putLong(longValue);
        buffer.putInt(intValue);
        byte[] bytes = buffer.array();
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static long[] convertBase64ToLongInt(String encodedStr) {
        byte[] decodedBytes = Base64.getDecoder().decode(encodedStr);
        ByteBuffer buffer = ByteBuffer.wrap(decodedBytes);
        long longValue = buffer.getLong();
        int intValue = buffer.getInt();
        return new long[] {longValue, intValue};
    }
}
