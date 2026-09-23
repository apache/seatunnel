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

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.yarn.cluster.YarnStagingDirectory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;

import static org.apache.hadoop.yarn.api.records.ApplicationId.fromString;

/** Closing a client leaves a detached application running; cancel explicitly stops it. */
final class YarnApplicationClient implements ApplicationClient {
    private final YarnClient client;
    private final Configuration configuration;
    private final String yarnId;
    private final Path staging;

    YarnApplicationClient(
            YarnClient client, Configuration configuration, String yarnId, Path staging) {
        this.client = client;
        this.configuration = configuration;
        this.yarnId = yarnId;
        this.staging = staging;
    }

    @Override
    public ApplicationId getApplicationId() {
        return new ApplicationId(DeployType.YARN, yarnId);
    }

    @Override
    public ApplicationStatus getStatus() throws Exception {
        return getResult().getStatus();
    }

    /** Reads the native result and retries artifact cleanup after any terminal state. */
    @Override
    public ApplicationResult getResult() throws Exception {
        ApplicationReport report = client.getApplicationReport(fromString(yarnId));
        ApplicationStatus status = status(report);
        if (status.isTerminal()) {
            YarnStagingDirectory.cleanup(configuration, staging);
        }
        return new ApplicationResult(getApplicationId(), status, report.getDiagnostics());
    }

    /** Kills the remote application and removes only its staged submission artifacts. */
    @Override
    public void cancel() throws Exception {
        client.killApplication(fromString(yarnId));
        YarnStagingDirectory.cleanup(configuration, staging);
    }

    /** Releases the submitting process's RPC client without affecting the remote application. */
    @Override
    public void close() {
        client.stop();
    }

    static ApplicationStatus status(ApplicationReport report) {
        switch (report.getYarnApplicationState()) {
            case NEW:
            case NEW_SAVING:
            case SUBMITTED:
                return ApplicationStatus.CREATED;
            case ACCEPTED:
                return ApplicationStatus.DEPLOYING;
            case RUNNING:
                return ApplicationStatus.RUNNING;
            case KILLED:
                return ApplicationStatus.CANCELED;
            case FAILED:
                return ApplicationStatus.FAILED;
            case FINISHED:
                switch (report.getFinalApplicationStatus()) {
                    case SUCCEEDED:
                        return ApplicationStatus.SUCCEEDED;
                    case KILLED:
                        return ApplicationStatus.CANCELED;
                    default:
                        return ApplicationStatus.FAILED;
                }
            default:
                return ApplicationStatus.UNKNOWN;
        }
    }
}
