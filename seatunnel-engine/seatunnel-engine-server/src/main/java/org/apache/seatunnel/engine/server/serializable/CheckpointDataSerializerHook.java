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
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointBarrierTriggerOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointEndOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointErrorReportOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointFinishedOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.NotifyTaskRestoreOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.NotifyTaskStartOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TriggerSchemaChangeAfterCheckpointOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TriggerSchemaChangeBeforeCheckpointOperation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class CheckpointDataSerializerHook implements DataSerializerHook {

    public static final int CHECKPOINT_BARRIER_TRIGGER_OPERATOR = 1;
    public static final int CHECKPOINT_FINISHED_OPERATOR = 2;
    public static final int TASK_ACK_OPERATOR = 3;

    public static final int TASK_REPORT_STATUS_OPERATOR = 4;

    public static final int NOTIFY_TASK_RESTORE_OPERATOR = 5;
    public static final int NOTIFY_TASK_START_OPERATOR = 6;
    public static final int CHECKPOINT_ERROR_REPORT_OPERATOR = 7;
    public static final int TRIGGER_SCHEMA_CHANGE_BEFORE_CHECKPOINT_OPERATOR = 8;
    public static final int TRIGGER_SCHEMA_CHANGE_AFTER_CHECKPOINT_OPERATOR = 9;

    public static final int CHECKPOINT_END_OPERATOR = 10;

    public static final int FACTORY_ID =
            FactoryIdHelper.getFactoryId(
                    SeaTunnelFactoryIdConstant.SEATUNNEL_CHECKPOINT_DATA_SERIALIZER_FACTORY,
                    SeaTunnelFactoryIdConstant.SEATUNNEL_CHECKPOINT_DATA_SERIALIZER_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new CheckpointDataSerializerHook.Factory();
    }

    private static class Factory implements DataSerializableFactory {

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case CHECKPOINT_BARRIER_TRIGGER_OPERATOR:
                    return new CheckpointBarrierTriggerOperation();
                case CHECKPOINT_FINISHED_OPERATOR:
                    return new CheckpointFinishedOperation();
                case TASK_ACK_OPERATOR:
                    return new TaskAcknowledgeOperation();
                case TASK_REPORT_STATUS_OPERATOR:
                    return new TaskReportStatusOperation();
                case NOTIFY_TASK_RESTORE_OPERATOR:
                    return new NotifyTaskRestoreOperation();
                case NOTIFY_TASK_START_OPERATOR:
                    return new NotifyTaskStartOperation();
                case CHECKPOINT_ERROR_REPORT_OPERATOR:
                    return new CheckpointErrorReportOperation();
                case TRIGGER_SCHEMA_CHANGE_BEFORE_CHECKPOINT_OPERATOR:
                    return new TriggerSchemaChangeBeforeCheckpointOperation();
                case TRIGGER_SCHEMA_CHANGE_AFTER_CHECKPOINT_OPERATOR:
                    return new TriggerSchemaChangeAfterCheckpointOperation();
                case CHECKPOINT_END_OPERATOR:
                    return new CheckpointEndOperation();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
