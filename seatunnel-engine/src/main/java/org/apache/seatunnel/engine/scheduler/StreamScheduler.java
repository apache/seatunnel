package org.apache.seatunnel.engine.scheduler;

import org.apache.seatunnel.engine.execution.TaskExecution;
import org.apache.seatunnel.engine.executionplan.ExecutionPlan;
import org.apache.seatunnel.engine.executionplan.ExecutionSubTask;
import org.apache.seatunnel.engine.executionplan.ExecutionTask;
import org.apache.seatunnel.engine.task.TaskExecutionState;

import java.util.List;

public class StreamScheduler implements SchedulerStrategy {
    private ExecutionPlan executionPlan;

    public StreamScheduler(ExecutionPlan baseExecutionPlan) {
        this.executionPlan = baseExecutionPlan;
    }

    @Override
    public void startScheduling(TaskExecution taskExecution) {
        List<ExecutionTask> tasks = executionPlan.getExecutionTasks();
        requestAllSlotAndScheduler(tasks, taskExecution);
    }

    private void requestAllSlotAndScheduler(List<ExecutionTask> tasks, TaskExecution taskExecution) {
        // TODO The committed only after all the task resources have been requested
        for (ExecutionTask executionTask : tasks) {
            for (ExecutionSubTask subTask : executionTask.getSubTasks()) {
                subTask.deploy(taskExecution);
            }
        }
    }

    @Override
    public boolean updateExecutionState(TaskExecutionState state) {
        return executionPlan.updateExecutionState(state);
    }
}
