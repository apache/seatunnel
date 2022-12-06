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
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.Progress;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;
import org.apache.seatunnel.engine.server.task.operation.CancelTaskOperation;
import org.apache.seatunnel.engine.server.task.operation.CleanTaskGroupContextOperation;
import org.apache.seatunnel.engine.server.task.operation.DeployTaskOperation;
import org.apache.seatunnel.engine.server.task.operation.GetTaskGroupAddressOperation;
import org.apache.seatunnel.engine.server.task.operation.GetTaskGroupMetricsOperation;
import org.apache.seatunnel.engine.server.task.operation.NotifyTaskStatusOperation;
import org.apache.seatunnel.engine.server.task.operation.checkpoint.BarrierFlowOperation;
import org.apache.seatunnel.engine.server.task.operation.checkpoint.CloseRequestOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkPrepareCommitOperation;
import org.apache.seatunnel.engine.server.task.operation.sink.SinkRegisterOperation;
import org.apache.seatunnel.engine.server.task.operation.source.AssignSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.LastCheckpointNotifyOperation;
import org.apache.seatunnel.engine.server.task.operation.source.RequestSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.RestoredSplitOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceNoMoreElementOperation;
import org.apache.seatunnel.engine.server.task.operation.source.SourceRegisterOperation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class TaskDataSerializerHook implements DataSerializerHook {

    public static final int SOURCE_REGISTER_TYPE = 1;

    public static final int REQUEST_SPLIT_TYPE = 2;

    public static final int ASSIGN_SPLIT_TYPE = 3;

    public static final int TASK_GROUP_INFO_TYPE = 4;

    public static final int SOURCE_UNREGISTER_TYPE = 5;

    public static final int GET_TASKGROUP_ADDRESS_TYPE = 6;

    public static final int SINK_REGISTER_TYPE = 7;

    public static final int SINK_PREPARE_COMMIT_TYPE = 8;

    public static final int TASK_LOCATION_TYPE = 9;

    public static final int PROGRESS_TYPE = 10;

    public static final int CLOSE_REQUEST_TYPE = 11;

    public static final int DEPLOY_TASK_OPERATOR = 12;

    public static final int CANCEL_TASK_OPERATOR = 13;

    public static final int RESTORED_SPLIT_OPERATOR = 14;

    public static final int NOTIFY_TASK_STATUS_OPERATOR = 15;

    public static final int BARRIER_FLOW_OPERATOR = 16;

    public static final int LAST_CHECKPOINT_NOTIFY = 17;

    public static final int GET_TASKGROUP_METRICS_OPERATION = 18;

    public static final int CLEAN_TASKGROUP_CONTEXT_OPERATION = 19;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(
        SeaTunnelFactoryIdConstant.SEATUNNEL_TASK_DATA_SERIALIZER_FACTORY,
        SeaTunnelFactoryIdConstant.SEATUNNEL_TASK_DATA_SERIALIZER_FACTORY_ID
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

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case SOURCE_REGISTER_TYPE:
                    return new SourceRegisterOperation();
                case REQUEST_SPLIT_TYPE:
                    return new RequestSplitOperation();
                case ASSIGN_SPLIT_TYPE:
                    return new AssignSplitOperation<>();
                case TASK_GROUP_INFO_TYPE:
                    return new TaskGroupImmutableInformation();
                case SOURCE_UNREGISTER_TYPE:
                    return new SourceNoMoreElementOperation();
                case SINK_REGISTER_TYPE:
                    return new SinkRegisterOperation();
                case SINK_PREPARE_COMMIT_TYPE:
                    return new SinkPrepareCommitOperation();
                case TASK_LOCATION_TYPE:
                    return new TaskLocation();
                case PROGRESS_TYPE:
                    return new Progress();
                case CLOSE_REQUEST_TYPE:
                    return new CloseRequestOperation();
                case DEPLOY_TASK_OPERATOR:
                    return new DeployTaskOperation();
                case CANCEL_TASK_OPERATOR:
                    return new CancelTaskOperation();
                case GET_TASKGROUP_ADDRESS_TYPE:
                    return new GetTaskGroupAddressOperation();
                case RESTORED_SPLIT_OPERATOR:
                    return new RestoredSplitOperation();
                case NOTIFY_TASK_STATUS_OPERATOR:
                    return new NotifyTaskStatusOperation();
                case BARRIER_FLOW_OPERATOR:
                    return new BarrierFlowOperation();
                case LAST_CHECKPOINT_NOTIFY:
                    return new LastCheckpointNotifyOperation();
                case GET_TASKGROUP_METRICS_OPERATION:
                    return new GetTaskGroupMetricsOperation();
                case CLEAN_TASKGROUP_CONTEXT_OPERATION:
                    return new CleanTaskGroupContextOperation();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
