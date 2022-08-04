package org.apache.seatunnel.engine.server.execution;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskTracker {
    public AtomicInteger expiredTimes = new AtomicInteger(0);
    public final NonCompletableFuture taskFuture = new NonCompletableFuture();
    public final Task task;
    public volatile Future<?> taskRuntimeFutures;

    public TaskTracker(Task task, CompletableFuture<Void> cancellationFuture, ILogger logger) {
        this.task = task;

        cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
            if (e == null) {
                e = new IllegalStateException("cancellationFuture should be completed exceptionally");
            }
            taskFuture.internalCompleteExceptionally(e);
            taskRuntimeFutures.cancel(true);
        }));
    }

    @Override
    public String toString() {
        return "Tracking " + task;
    }
}
