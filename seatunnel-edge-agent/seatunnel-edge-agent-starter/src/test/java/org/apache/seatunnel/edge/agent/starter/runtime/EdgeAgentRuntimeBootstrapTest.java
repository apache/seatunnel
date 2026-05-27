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

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeInputReader;
import org.apache.seatunnel.edge.agent.starter.config.AgentRuntimeConfig;
import org.apache.seatunnel.edge.agent.starter.parse.EdgeAgentConfigLoader;
import org.apache.seatunnel.edge.agent.starter.parse.EdgeAgentResolvedConfig;
import org.apache.seatunnel.edge.agent.starter.wal.mem.MemWalStore;
import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransport;
import org.apache.seatunnel.edge.agent.transport.serialize.RawPayloadSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class EdgeAgentRuntimeBootstrapTest {

    @TempDir Path tempDir;

    @Test
    void bootstrapOpensTransportBeforeReader() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        List<String> openOrder = new ArrayList<>();
        OrderTrackingReader reader = new OrderTrackingReader(running, openOrder);
        FakeTransport transport = new FakeTransport(openOrder);

        AgentRuntimeConfig config = loadRuntimeConfig(tempDir.resolve("wal.db"));
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader, new MemWalStore(), transport, new RawPayloadSerializer(), running);
        EdgeAgentRuntimeBootstrap bootstrap = EdgeAgentRuntimeBootstrap.create(config, ctx);

        bootstrap.start();

        Assertions.assertEquals(Arrays.asList("transport", "reader"), openOrder);
    }

    private AgentRuntimeConfig loadRuntimeConfig(Path sqlitePath) throws Exception {
        Path yamlPath = tempDir.resolve("agent.yaml");
        String yaml =
                "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "queue:\n"
                        + "  sqlite-path: "
                        + sqlitePath
                        + "\n"
                        + "output:\n"
                        + "  type: console\n";
        Files.write(yamlPath, yaml.getBytes(StandardCharsets.UTF_8));
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(yamlPath);
        return resolved.getRuntimeConfig();
    }

    private static final class OrderTrackingReader implements EdgeInputReader {
        private final AtomicBoolean running;
        private final List<String> openOrder;

        OrderTrackingReader(AtomicBoolean running, List<String> openOrder) {
            this.running = running;
            this.openOrder = openOrder;
        }

        @Override
        public void open() {
            openOrder.add("reader");
        }

        @Override
        public List<EdgeEvent> poll(int maxRecords) {
            running.set(false);
            return Collections.emptyList();
        }
    }

    private static final class FakeTransport implements EdgeCollectorTransport {

        private final List<String> openOrder;

        FakeTransport(List<String> openOrder) {
            this.openOrder = openOrder;
        }

        @Override
        public void open() {
            openOrder.add("transport");
        }

        @Override
        public void sendUntilReceived(long batchId, String payload) {}

        @Override
        public boolean probeReachable() {
            return true;
        }

        @Override
        public void close() {}
    }
}
