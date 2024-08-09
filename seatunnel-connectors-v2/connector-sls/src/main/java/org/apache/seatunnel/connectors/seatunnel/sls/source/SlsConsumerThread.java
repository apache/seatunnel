package org.apache.seatunnel.connectors.seatunnel.sls.source;

import com.aliyun.openservices.log.Client;
import lombok.Getter;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SlsConsumerThread implements Runnable {

    private final Client client;

    @Getter private final LinkedBlockingQueue<Consumer<Client>> tasks;

    public SlsConsumerThread(SlsSourceConfig slsSourceConfig) {
        this.client = this.initClient(slsSourceConfig);
        this.tasks = new LinkedBlockingQueue<>();
    }

    public LinkedBlockingQueue<Consumer<Client>> getTasks() {
        return tasks;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Consumer<Client> task = tasks.poll(1, TimeUnit.SECONDS);
                    if (task != null) {
                        task.accept(client);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            try {
                if (client != null) {
                    /** now do nothine, do not need close */
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
    }

    private Client initClient(SlsSourceConfig slsSourceConfig) {
        return new Client(
                slsSourceConfig.getEndpoint(),
                slsSourceConfig.getAccessKeyId(),
                slsSourceConfig.getAccessKeySecret());
    }
}
