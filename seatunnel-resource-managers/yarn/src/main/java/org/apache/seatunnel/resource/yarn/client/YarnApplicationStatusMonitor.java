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

import org.apache.seatunnel.resource.yarn.exception.YarnClientException;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnApplication;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Observes YARN application startup without mixing polling policy into submission code. */
@Slf4j
public final class YarnApplicationStatusMonitor {

    private static final long STATUS_POLL_INTERVAL_MILLIS = 500;

    private final YarnClient client;

    public YarnApplicationStatusMonitor(YarnClient client) {
        this.client = client;
    }

    /**
     * Polls RM reports during startup. A short job can finish successfully between two polls.
     *
     * @param applicationId native YARN application identifier
     * @param timeoutMillis maximum startup wait
     * @return the first observed running or successful report
     * @throws YarnClientException if the RM query fails or the application is killed or fails
     * @throws TimeoutException if the startup deadline expires
     * @throws InterruptedException if the caller interrupts the wait
     */
    public ApplicationReport awaitRunning(ApplicationId applicationId, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        YarnApplicationState previous = null;
        while (true) {
            ApplicationReport report =
                    YarnClientRequest.call(
                            "Could not read YARN application report for " + applicationId,
                            () -> client.getApplicationReport(applicationId));
            YarnApplication application = new YarnApplication(applicationId, report);
            YarnApplicationState state = application.getState();
            if (state != previous) {
                log.debug("YARN application {} state: {}", applicationId, state);
                previous = state;
            }
            if (application.isRunning() || application.isSucceeded()) {
                return application.getInternalResource();
            }
            if (application.isTerminated()) {
                throw new YarnClientException(
                        "YARN application "
                                + applicationId
                                + " has no live master ("
                                + state
                                + "): "
                                + application.getDiagnostics());
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new TimeoutException(
                        "YARN application "
                                + applicationId
                                + " did not start its ApplicationMaster within "
                                + timeoutMillis
                                + " ms; check queue capacity and NodeManager resources");
            }
            TimeUnit.NANOSECONDS.sleep(
                    Math.min(
                            remaining, TimeUnit.MILLISECONDS.toNanos(STATUS_POLL_INTERVAL_MILLIS)));
        }
    }
}
