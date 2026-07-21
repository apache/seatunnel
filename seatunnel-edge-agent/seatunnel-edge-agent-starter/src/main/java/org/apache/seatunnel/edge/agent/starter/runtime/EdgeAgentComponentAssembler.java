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

import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.edge.agent.connector.EdgeInputReader;
import org.apache.seatunnel.edge.agent.connector.EdgeInputReaderFactory;
import org.apache.seatunnel.edge.agent.starter.parse.EdgeAgentResolvedConfig;
import org.apache.seatunnel.edge.agent.starter.wal.WalStore;
import org.apache.seatunnel.edge.agent.starter.wal.WalStoreFactory;
import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransport;
import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransportFactory;
import org.apache.seatunnel.edge.agent.transport.serialize.PayloadSerializer;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class EdgeAgentComponentAssembler {

    /**
     * Discovers input/output/store SPI plugins and wires reader, transport, and serializer.
     *
     * <p>Called once per process start before {@code EdgeAgentRuntimeBootstrap.start()}. The {@link
     * WalStoreFactory} is discovered by {@link
     * org.apache.seatunnel.edge.agent.starter.config.EdgeDeliveryGuarantee#storeFactoryId()} (e.g.
     * {@code sqlite}, {@code mem}), following the same SPI pattern as input and output factories.
     *
     * @param resolved output of {@code EdgeAgentConfigLoader.load}
     * @param workDir base directory for relative {@code queue.sqlite-path}
     * @param running shared flag cleared by shutdown hook to stop the scheduler loop
     * @return context handed to bootstrap and scheduler
     * @throws Exception if factory discovery or plugin {@code create} fails
     */
    public static EdgeAgentRuntimeContext assemble(
            EdgeAgentResolvedConfig resolved, Path workDir, AtomicBoolean running)
            throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        WalStoreFactory storeFactory =
                FactoryUtil.discoverFactory(
                        classLoader, WalStoreFactory.class, resolved.getStoreType());
        WalStore walStore = storeFactory.create(resolved.getRuntimeConfig(), workDir);

        EdgeInputReaderFactory inputFactory =
                FactoryUtil.discoverFactory(
                        classLoader, EdgeInputReaderFactory.class, resolved.getInputType());
        EdgeInputReader reader =
                inputFactory.create(resolved.getInputConfig(), walStore.sourcePositionStore());

        EdgeCollectorTransportFactory outputFactory =
                FactoryUtil.discoverFactory(
                        classLoader, EdgeCollectorTransportFactory.class, resolved.getOutputType());
        EdgeCollectorTransport transport = outputFactory.create(resolved.getOutputConfig());
        PayloadSerializer payloadSerializer =
                outputFactory.payloadSerializer(resolved.getOutputConfig());

        return new EdgeAgentRuntimeContext(reader, walStore, transport, payloadSerializer, running);
    }
}
