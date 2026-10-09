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

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/** Centralizes YARN application state conversion used by clients and the ApplicationMaster. */
public final class YarnApplicationStatus {

    private YarnApplicationStatus() {}

    /** Maps a YARN application report to the platform-independent application status. */
    public static ApplicationStatus fromApplicationReport(ApplicationReport report) {
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

    /** Maps the final platform status published by the ApplicationMaster. */
    public static FinalApplicationStatus toFinalApplicationStatus(ApplicationStatus status) {
        if (status == ApplicationStatus.SUCCEEDED) {
            return FinalApplicationStatus.SUCCEEDED;
        }
        if (status == ApplicationStatus.CANCELED) {
            return FinalApplicationStatus.KILLED;
        }
        return FinalApplicationStatus.FAILED;
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
