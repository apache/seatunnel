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

package org.apache.seatunnel.e2e.common.container;

import org.apache.seatunnel.e2e.common.TestResource;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.io.IOException;

public interface TestContainer extends TestResource {

    Network NETWORK = Network.newNetwork();

    TestContainerId identifier();

    void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException;

    default Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, -1);
    }

    Container.ExecResult executeJob(String confFile, float timeoutSeconds)
            throws IOException, InterruptedException;

    default void stopContainer() throws IOException, InterruptedException {
        //        new DockerClient().execCreateCmd().exec().getId()
        //        throw new NotImplementedException();
    }
}
