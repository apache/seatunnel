package org.apache.seatunnel.core.starter.execution;

import org.apache.seatunnel.core.starter.exception.TaskExecuteException;

public interface TaskExecution {

    void execute() throws TaskExecuteException;
}
