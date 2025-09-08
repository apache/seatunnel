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
import java.util.List;
import java.util.UUID;

public interface TestContainer extends TestResource {

    Network NETWORK =
            Network.builder()
                    .createNetworkCmdModifier(cmd -> cmd.withName("SEATUNNEL-" + UUID.randomUUID()))
                    .enableIpv6(false)
                    .build();

    TestContainerId identifier();

    void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException;

    Container.ExecResult executeJob(String confFile) throws IOException, InterruptedException;

    Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException;

    default Container.ExecResult executeJob(String confFile, String jobId, String... variables)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    default Container.ExecResult executeConnectorCheck(String[] args)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    default Container.ExecResult executeBaseCommand(String[] args)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    default Container.ExecResult savepointJob(String jobId)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    default Container.ExecResult restoreJob(String confFile, String jobId, String... variables)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    default Container.ExecResult cancelJob(String jobId) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    default String getJobStatus(String jobId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    String getServerLogs();

    void copyFileToContainer(String path, String targetPath);

    void copyAbsolutePathToContainer(String path, String targetPath);
}
