package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;

public class StopJobOperation extends AbstractJobAsyncOperation {
    public StopJobOperation() {
        super();
    }

    public StopJobOperation(long jobId) {
        super(jobId);
    }

    @Override
    protected PassiveCompletableFuture<?> doRun() throws Exception {
        SeaTunnelServer service = getService();
        return service.getCoordinatorService().stopJob(jobId);
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.STOP_JOB_OPERATOR;
    }
}
