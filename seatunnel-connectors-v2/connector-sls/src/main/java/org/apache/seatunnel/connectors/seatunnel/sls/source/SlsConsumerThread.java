/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
