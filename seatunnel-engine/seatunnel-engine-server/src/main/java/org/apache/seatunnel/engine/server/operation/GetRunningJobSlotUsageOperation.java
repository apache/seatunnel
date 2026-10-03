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
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;
import org.apache.seatunnel.engine.server.trace.RunningJobSlotUsageBuilder;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.ExecutionException;

/** Master operation that returns slot ownership summaries for all running jobs. */
public class GetRunningJobSlotUsageOperation extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private String response;

    @Override
    public int getFactoryId() {
        return ClientToServerOperationDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.GET_RUNNING_JOB_SLOT_USAGE_OPERATION;
    }

    /**
     * Builds the response on the master member where running job and owned slot maps are current.
     */
    @Override
    public void run() {
        SeaTunnelServer service = getService();
        CompletableFuture<String> future =
                CompletableFuture.supplyAsync(
                        () -> JsonUtils.toJsonString(RunningJobSlotUsageBuilder.build(service)),
                        getNodeEngine()
                                .getExecutionService()
                                .getExecutor("get_running_job_slot_usage_operation"));

        response = awaitResponse(future);
    }

    static String awaitResponse(CompletableFuture<String> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw ExceptionUtil.rethrow(e.getCause());
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
