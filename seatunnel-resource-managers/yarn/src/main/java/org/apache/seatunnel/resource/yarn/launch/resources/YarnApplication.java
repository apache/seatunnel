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

package org.apache.seatunnel.resource.yarn.launch.resources;

import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/** The ResourceManager's view of an application, including its final result and diagnostics. */
public final class YarnApplication extends YarnResource<ApplicationReport> {

    public YarnApplication(ApplicationId id, ApplicationReport report) {
        super(id.toString(), report);
    }

    public YarnApplicationState getState() {
        return getInternalResource().getYarnApplicationState();
    }

    public ApplicationStatus getStatus() {
        return statusOf(getInternalResource());
    }

    public boolean isRunning() {
        return getState() == YarnApplicationState.RUNNING;
    }

    public boolean isSucceeded() {
        return getStatus() == ApplicationStatus.SUCCEEDED;
    }

    public boolean isTerminated() {
        return getStatus().isTerminal();
    }

    public String getDiagnostics() {
        return getInternalResource().getDiagnostics();
    }

    /**
     * Maps the RM lifecycle to an engine status. A {@code FINISHED} report uses the AM's final
     * result to distinguish success, cancellation and failure.
     *
     * @param report a single RM snapshot of the application
     * @return the status exposed to SeaTunnel application clients
     */
    public static ApplicationStatus statusOf(ApplicationReport report) {
        YarnApplicationState state = report.getYarnApplicationState();
        switch (state) {
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
                return fromFinalApplicationStatus(report.getFinalApplicationStatus());
            default:
                return ApplicationStatus.UNKNOWN;
        }
    }

    private static ApplicationStatus fromFinalApplicationStatus(
            FinalApplicationStatus finalStatus) {
        if (finalStatus == FinalApplicationStatus.SUCCEEDED) {
            return ApplicationStatus.SUCCEEDED;
        }
        if (finalStatus == FinalApplicationStatus.KILLED) {
            return ApplicationStatus.CANCELED;
        }
        return ApplicationStatus.FAILED;
    }
}
