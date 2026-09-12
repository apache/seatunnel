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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns a Kudu client and the executor used by its Netty event loops.
 *
 * <p>Kudu 1.15 initiates a graceful Netty shutdown from {@link KuduClient#close()} but returns
 * before the event-loop threads terminate. Waiting for the supplied executor prevents those threads
 * from accessing connector classes after an engine unloads the user-code classloader.
 */
@Slf4j
public class KuduClientResource implements AutoCloseable {

    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 20L;
    private static final long FORCED_SHUTDOWN_TIMEOUT_SECONDS = 5L;

    private final KuduClient kuduClient;
    private final ExecutorService executorService;

    public KuduClientResource(KuduClient kuduClient, ExecutorService executorService) {
        this.kuduClient = kuduClient;
        this.executorService = executorService;
    }

    public KuduClient getClient() {
        return kuduClient;
    }

    @Override
    public void close() throws KuduException {
        try {
            kuduClient.close();
        } finally {
            awaitExecutorTermination();
        }
    }

    private void awaitExecutorTermination() {
        executorService.shutdown();
        try {
            if (executorService.awaitTermination(
                    GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                return;
            }

            log.warn("Kudu client executor did not terminate gracefully, forcing shutdown.");
            executorService.shutdownNow();
            if (!executorService.awaitTermination(
                    FORCED_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Kudu client executor did not terminate after forced shutdown.");
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for Kudu client executor to terminate.", e);
        }
    }
}
