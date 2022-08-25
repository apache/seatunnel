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

package org.apache.seatunnel.engine.server.serializable;

import org.apache.seatunnel.engine.common.serializeable.SeaTunnelFactoryIdConstant;
import org.apache.seatunnel.engine.server.operation.CancelJobOperation;
import org.apache.seatunnel.engine.server.operation.CheckpointAckOperation;
import org.apache.seatunnel.engine.server.operation.CheckpointFinishedOperation;
import org.apache.seatunnel.engine.server.operation.CheckpointTriggerOperation;
import org.apache.seatunnel.engine.server.operation.GetJobStatusOperation;
import org.apache.seatunnel.engine.server.operation.PrintMessageOperation;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.operation.TaskCompletedOperation;
import org.apache.seatunnel.engine.server.operation.WaitForJobCompleteOperation;
import org.apache.seatunnel.engine.server.task.operation.DeployTaskOperation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * A Java Service Provider hook for Hazelcast's Identified Data Serializable
 * mechanism. This is private API.
 * All about the Operation's data serializable define in this class.
 */
@PrivateApi
public final class OperationDataSerializerHook implements DataSerializerHook {
    public static final int PRINT_MESSAGE_OPERATOR = 0;
    public static final int SUBMIT_OPERATOR = 1;

    public static final int DEPLOY_TASK_OPERATOR = 2;

    public static final int TASK_COMPLETED_OPERATOR = 3;
    public static final int WAIT_FORM_JOB_COMPLETE_OPERATOR = 4;

    public static final int CHECKPOINT_TRIGGER_OPERATOR = 5;

    public static final int CHECKPOINT_ACK_OPERATOR = 6;

    public static final int CHECKPOINT_FINISHED_OPERATOR = 7;
    public static final int CANCEL_JOB_OPERATOR = 8;
    public static final int GET_JOB_STATUS_OPERATOR = 9;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(
        SeaTunnelFactoryIdConstant.SEATUNNEL_OPERATION_DATA_SERIALIZER_FACTORY,
        SeaTunnelFactoryIdConstant.SEATUNNEL_OPERATION_DATA_SERIALIZER_FACTORY_ID
    );

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @SuppressWarnings("checkstyle:returncount")
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case PRINT_MESSAGE_OPERATOR:
                    return new PrintMessageOperation();
                case SUBMIT_OPERATOR:
                    return new SubmitJobOperation();
                case DEPLOY_TASK_OPERATOR:
                    return new DeployTaskOperation();
                case TASK_COMPLETED_OPERATOR:
                    return new TaskCompletedOperation();
                case WAIT_FORM_JOB_COMPLETE_OPERATOR:
                    return new WaitForJobCompleteOperation();
                case CHECKPOINT_TRIGGER_OPERATOR:
                    return new CheckpointTriggerOperation();
                case CANCEL_JOB_OPERATOR:
                    return new CancelJobOperation();
                case GET_JOB_STATUS_OPERATOR:
                    return new GetJobStatusOperation();
                case CHECKPOINT_ACK_OPERATOR:
                    return new CheckpointAckOperation();
                case CHECKPOINT_FINISHED_OPERATOR:
                    return new CheckpointFinishedOperation();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
