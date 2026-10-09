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

package org.apache.seatunnel.resource.yarn.client;

import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;

/** Closing a client leaves a detached application running; cancel explicitly stops it. */
public final class YarnApplicationClient implements AutoCloseable {
    private final YarnClient client;
    private final Configuration configuration;
    private final ApplicationId yarnId;
    private final Path staging;

    public YarnApplicationClient(
            YarnClient client, Configuration configuration, ApplicationId yarnId, Path staging) {
        this.client = client;
        this.configuration = configuration;
        this.yarnId = yarnId;
        this.staging = staging;
    }

    public ApplicationId getClusterId() {
        return yarnId;
    }

    /** Reads native application state and retries artifact cleanup after any terminal state. */
    public ApplicationStatus getStatus() throws Exception {
        ApplicationReport report = client.getApplicationReport(yarnId);
        ApplicationStatus status = YarnApplicationStatus.fromApplicationReport(report);
        if (status.isTerminal()) {
            YarnStagingDirectory.cleanup(configuration, staging);
        }
        return status;
    }

    /** Kills the remote application and removes only its staged submission artifacts. */
    public void cancel() throws Exception {
        client.killApplication(yarnId);
        YarnStagingDirectory.cleanup(configuration, staging);
    }

    /** Releases the submitting process's RPC client without affecting the remote application. */
    @Override
    public void close() {
        client.stop();
    }
}
