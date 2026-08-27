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

package org.apache.seatunnel.e2e.common;

import org.apache.seatunnel.e2e.common.container.ContainerTcpProxy;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainersFactory;
import org.apache.seatunnel.e2e.common.junit.ContainerTestingExtension;
import org.apache.seatunnel.e2e.common.junit.TestCaseInvocationContextProvider;
import org.apache.seatunnel.e2e.common.junit.TestContainers;
import org.apache.seatunnel.e2e.common.junit.TestLoggerExtension;
import org.apache.seatunnel.e2e.common.junit.TimingExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

import com.github.dockerjava.api.DockerClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ExtendWith({
    ContainerTestingExtension.class,
    TestLoggerExtension.class,
    TestCaseInvocationContextProvider.class,
    TimingExtension.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestSuiteBase {

    protected static final Network NETWORK = TestContainer.NETWORK;

    @TestContainers
    private TestContainersFactory containersFactory = ContainerUtil::discoverTestContainers;

    protected DockerClient dockerClient = DockerClientFactory.lazyClient();

    private final List<ContainerTcpProxy> containerTcpProxies = new ArrayList<>();

    /**
     * Starts a container TCP proxy that is automatically closed after the test class.
     *
     * @param portMappings ports advertised by the service and their dynamic container targets
     * @return the managed container TCP proxy
     */
    protected final ContainerTcpProxy startContainerTcpProxy(
            ContainerTcpProxy.PortMapping... portMappings) throws IOException {
        return startContainerTcpProxy(Arrays.asList(portMappings));
    }

    /**
     * Starts a container TCP proxy that is automatically closed after the test class.
     *
     * @param portMappings ports advertised by the service and their dynamic container targets
     * @return the managed container TCP proxy
     */
    protected final ContainerTcpProxy startContainerTcpProxy(
            List<ContainerTcpProxy.PortMapping> portMappings) throws IOException {
        ContainerTcpProxy proxy = ContainerTcpProxy.start(portMappings);
        containerTcpProxies.add(proxy);
        return proxy;
    }

    @AfterAll
    protected final void closeContainerTcpProxies() {
        for (int i = containerTcpProxies.size() - 1; i >= 0; i--) {
            containerTcpProxies.get(i).close();
        }
        containerTcpProxies.clear();
    }
}
