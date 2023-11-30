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

import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainersFactory;
import org.apache.seatunnel.e2e.common.junit.ContainerTestingExtension;
import org.apache.seatunnel.e2e.common.junit.TestCaseInvocationContextProvider;
import org.apache.seatunnel.e2e.common.junit.TestContainers;
import org.apache.seatunnel.e2e.common.junit.TestLoggerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

import com.github.dockerjava.api.DockerClient;

@ExtendWith({
    ContainerTestingExtension.class,
    TestLoggerExtension.class,
    TestCaseInvocationContextProvider.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestSuiteBase {

    protected static final Network NETWORK = TestContainer.NETWORK;

    @TestContainers
    private TestContainersFactory containersFactory = ContainerUtil::discoverTestContainers;

    protected DockerClient dockerClient = DockerClientFactory.lazyClient();
}
