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
import org.apache.seatunnel.resource.yarn.exception.YarnClientException;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnApplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;

import lombok.extern.slf4j.Slf4j;

/** Closing a client leaves a detached application running; cancel explicitly stops it. */
@Slf4j
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

    /**
     * Reads the RM report. Each terminal query retries cleanup of this application's staging
     * directory, even when a previous cleanup failed.
     *
     * @return engine application status, or {@link ApplicationStatus#UNKNOWN} if the RM has no
     *     record
     * @throws YarnClientException if reading the report fails
     */
    public ApplicationStatus getStatus() {
        ApplicationStatus status;
        try {
            status = new YarnApplication(yarnId, client.getApplicationReport(yarnId)).getStatus();
        } catch (ApplicationNotFoundException missing) {
            return ApplicationStatus.UNKNOWN;
        } catch (Exception failure) {
            throw new YarnClientException("Could not query YARN application " + yarnId, failure);
        }
        log.debug("YARN application {} status: {}", yarnId, status);
        if (status.isTerminal()) {
            try {
                YarnStagingDirectory.cleanup(configuration, staging);
            } catch (Exception failure) {
                log.warn(
                        "Could not remove staging directory {} for terminal YARN application {} ({}). A later status query can retry cleanup.",
                        staging,
                        yarnId,
                        status,
                        failure);
            }
        }
        return status;
    }

    /**
     * Requests termination and removes the application's staging directory. An application already
     * absent from the RM can still have files to clean up.
     *
     * @throws YarnClientException if the kill request or filesystem cleanup fails
     */
    public void cancel() {
        log.info("Canceling YARN application {}", yarnId);
        try {
            try {
                client.killApplication(yarnId);
            } catch (ApplicationNotFoundException missing) {
                log.debug("YARN application {} is already absent", yarnId);
            }
            YarnStagingDirectory.cleanup(configuration, staging);
        } catch (Exception failure) {
            throw new YarnClientException("Could not cancel YARN application " + yarnId, failure);
        }
    }

    /** Releases the submitting process's RPC client without affecting the remote application. */
    @Override
    public void close() {
        try {
            client.stop();
        } catch (RuntimeException failure) {
            throw new YarnClientException("Could not close YARN client for " + yarnId, failure);
        }
    }
}
