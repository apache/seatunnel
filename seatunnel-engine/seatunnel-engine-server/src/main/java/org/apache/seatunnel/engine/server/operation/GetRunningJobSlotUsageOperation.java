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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;
import org.apache.seatunnel.engine.server.trace.RunningJobSlotUsageBuilder;

import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.util.concurrent.Executor;

/**
 * Master operation that returns slot ownership summaries for all running jobs.
 *
 * <p>The cluster-wide aggregation is handed to a named executor and the Hazelcast operation thread
 * is released immediately: {@link AsyncOperation} declares {@code returnsResponse() == false} and
 * sends the success or failure response from the completion callback of the returned future, so no
 * generic operation thread waits for the aggregation to finish.
 */
public class GetRunningJobSlotUsageOperation extends AsyncOperation
        implements AllowedDuringPassiveState {

    static final String EXECUTOR_NAME = "get_running_job_slot_usage_operation";

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.GET_RUNNING_JOB_SLOT_USAGE_OPERATION;
    }

    /**
     * Schedules the aggregation on the master member where running job and owned slot maps are
     * current, and returns without waiting for it.
     */
    @Override
    protected PassiveCompletableFuture<String> doRun() {
        SeaTunnelServer service = getService();
        Executor executor = getNodeEngine().getExecutionService().getExecutor(EXECUTOR_NAME);
        // A failure inside buildResponse completes the future exceptionally; AsyncOperation peels
        // the CompletionException and sends the original exception back to the caller.
        return new PassiveCompletableFuture<>(
                CompletableFuture.supplyAsync(() -> buildResponse(service), executor));
    }

    /**
     * Builds the JSON response on the executor thread, never on the operation thread. Kept
     * package-private so the operation lifecycle test can replace the cluster-wide aggregation.
     */
    String buildResponse(SeaTunnelServer service) {
        return JsonUtils.toJsonString(RunningJobSlotUsageBuilder.build(service));
    }
}
