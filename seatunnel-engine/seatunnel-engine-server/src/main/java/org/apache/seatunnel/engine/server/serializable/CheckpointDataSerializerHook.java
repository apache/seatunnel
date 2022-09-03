package org.apache.seatunnel.engine.server.serializable;

import org.apache.seatunnel.engine.common.serializeable.SeaTunnelFactoryIdConstant;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointBarrierTriggerOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointFinishedOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class CheckpointDataSerializerHook implements DataSerializerHook {

    public static final int CHECKPOINT_BARRIER_TRIGGER_OPERATOR = 1;
    public static final int CHECKPOINT_FINISHED_OPERATOR = 2;

    public static final int TASK_ACK_OPERATOR = 3;

    public static final int TASK_REPORT_STATUS_OPERATOR = 4;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(
        SeaTunnelFactoryIdConstant.SEATUNNEL_CHECKPOINT_DATA_SERIALIZER_FACTORY,
        SeaTunnelFactoryIdConstant.SEATUNNEL_CHECKPOINT_DATA_SERIALIZER_FACTORY_ID
    );

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new CheckpointDataSerializerHook.Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @SuppressWarnings("checkstyle:returncount")
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
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
