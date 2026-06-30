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
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class StateStoreCheckpointIDCounter implements CheckpointIDCounter {

    private final String key;
    private final CounterStateStore<String> checkpointCounterStore;

    public StateStoreCheckpointIDCounter(
            Long jobID, Integer pipelineId, CounterStateStore<String> checkpointCounterStore) {
        this.key = convertLongIntToBase64(jobID, pipelineId);
        this.checkpointCounterStore =
                Objects.requireNonNull(checkpointCounterStore, "checkpointCounterStore");
    }

    @Override
    public void start() throws Exception {
        RetryUtils.retryWithException(
                () -> checkpointCounterStore.initializeIfAbsent(key, INITIAL_CHECKPOINT_ID),
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        ExceptionUtil::isOperationNeedRetryException,
                        Constant.OPERATION_RETRY_SLEEP));
    }

    @Override
    public CompletableFuture<Void> shutdown(PipelineStatus pipelineStatus) {
        if (pipelineStatus.isEndState()) {
            checkpointCounterStore.remove(key);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getAndIncrement() throws Exception {
        Long nextId = checkpointCounterStore.incrementAndGet(key);
        checkNotNull(nextId);
        return nextId - 1L;
    }

    @Override
    public long get() {
        return checkpointCounterStore.get(key);
    }

    @Override
    public void setCount(long newId) throws Exception {
        checkpointCounterStore.set(key, newId);
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
