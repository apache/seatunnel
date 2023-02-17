package org.apache.seatunnel.engine.server.execution;

import lombok.NonNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Liuli
 * @Date: 2023/2/20 20:37
 */
public class BlockTask implements Task{

    @Override
    public boolean isThreadsShare() {
        return true;
    }

    @NonNull
    @Override
    public ProgressState call() throws Exception {
        BlockingQueue<String> bq = new LinkedBlockingQueue<>();
        bq.poll(1000, TimeUnit.MINUTES);

        return ProgressState.MADE_PROGRESS;

    }

    @NonNull
    @Override
    public Long getTaskID() {
        return (long) this.hashCode();
    }
}
