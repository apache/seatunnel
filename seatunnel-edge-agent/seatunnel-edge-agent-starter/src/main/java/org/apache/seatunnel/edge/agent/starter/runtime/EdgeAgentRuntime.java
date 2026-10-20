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

import org.apache.seatunnel.edge.agent.starter.parse.EdgeAgentConfigLoader;
import org.apache.seatunnel.edge.agent.starter.parse.EdgeAgentResolvedConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class EdgeAgentRuntime {
    private static final Logger LOG = LoggerFactory.getLogger(EdgeAgentRuntime.class);

    /**
     * Loads configuration, assembles components, and runs the agent until shutdown.
     *
     * <p>Registers a JVM shutdown hook that clears the running flag. Opens transport before the
     * reader, then blocks in the scheduler loop until interrupted or shutdown.
     *
     * @param agentYamlPath path to {@code agent.yaml}
     * @throws Exception on config, assembly, or runtime failures
     */
    public static void start(Path agentYamlPath) throws Exception {
        try (EdgeAgentRuntimeSession session =
                bootstrapSession(agentYamlPath, Paths.get("").toAbsolutePath())) {
            EdgeAgentResolvedConfig resolved = session.getResolvedConfig();
            LOG.info(
                    "BOOTSTRAP_READY edge-agent started agentId={}, inputId={}, outputId={},"
                            + " inputType={}, outputType={}, deliveryGuarantee={}",
                    resolved.getAgentId(),
                    resolved.getInputId(),
                    resolved.getOutputId(),
                    resolved.getInputType(),
                    resolved.getOutputType(),
                    resolved.getRuntimeConfig().getDeliveryGuarantee());

            session.getBootstrap().start();
        }
    }

    private static EdgeAgentRuntimeSession bootstrapSession(Path agentYamlPath, Path workDir)
            throws Exception {
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(agentYamlPath, workDir);
        AtomicBoolean running = new AtomicBoolean(true);
        registerShutdownHook(running);
        EdgeAgentRuntimeContext ctx =
                EdgeAgentComponentAssembler.assemble(resolved, workDir, running);
        EdgeAgentRuntimeBootstrap bootstrap =
                EdgeAgentRuntimeBootstrap.create(resolved.getRuntimeConfig(), ctx);
        return new EdgeAgentRuntimeSession(resolved, bootstrap);
    }

    private static void registerShutdownHook(AtomicBoolean running) {
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    running.set(false);
                                    LOG.info("Shutdown signal received; stopping edge agent.");
                                },
                                "edge-agent-shutdown"));
    }
}
