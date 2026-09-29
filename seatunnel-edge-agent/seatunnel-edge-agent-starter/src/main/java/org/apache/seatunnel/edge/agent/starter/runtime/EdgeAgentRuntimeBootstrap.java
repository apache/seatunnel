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

package org.apache.seatunnel.edge.agent.starter.runtime;

import org.apache.seatunnel.edge.agent.starter.config.AgentRuntimeConfig;

public class EdgeAgentRuntimeBootstrap implements AutoCloseable {

    private final AgentRuntimeConfig config;
    private final EdgeAgentRuntimeContext ctx;

    private EdgeAgentRuntimeScheduler scheduler;

    static EdgeAgentRuntimeBootstrap create(
            AgentRuntimeConfig config, EdgeAgentRuntimeContext ctx) {
        return new EdgeAgentRuntimeBootstrap(config, ctx);
    }

    EdgeAgentRuntimeBootstrap(AgentRuntimeConfig config, EdgeAgentRuntimeContext ctx) {
        this.config = config;
        this.ctx = ctx;
    }

    /**
     * Opens transport and reader, then runs the scheduler until {@code running} is false.
     *
     * <p>Transport opens before the reader so ingress is ready before data is polled. On failure,
     * {@code close()} is invoked to release partial resources.
     *
     * @throws Exception if open, poll/send, or persistence fails
     */
    public void start() throws Exception {
        try {
            ctx.getTransport().open();
            ctx.getReader().open();
            scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);
            scheduler.runUntilStopped(ctx.getRunning());
        } catch (Exception ex) {
            try {
                close();
            } catch (Exception closeEx) {
                ex.addSuppressed(closeEx);
            }
            throw ex;
        }
    }

    /**
     * Stops the scheduler if started; otherwise closes reader, transport, and runtime store.
     *
     * @throws Exception first close failure among reader, transport, or stores
     */
    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            scheduler.close();
        } else {
            ctx.getReader().close();
            ctx.getTransport().close();
            closePersistence();
        }
    }

    private void closePersistence() throws Exception {
        ctx.getWalStore().close();
    }
}
