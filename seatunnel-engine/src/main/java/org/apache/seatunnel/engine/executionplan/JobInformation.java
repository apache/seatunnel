/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.executionplan;

import org.apache.seatunnel.engine.api.common.JobID;
import org.apache.seatunnel.engine.api.source.Boundedness;
import org.apache.seatunnel.engine.config.Configuration;

import static com.google.common.base.Preconditions.checkNotNull;

public class JobInformation {

    /**
     * Id of the job.
     */
    private final JobID jobId;

    /**
     * Job name.
     */
    private final String jobName;

    /**
     * Configuration of the job.
     */
    private final Configuration jobConfiguration;

    private final Boundedness boundedness;

    public JobInformation(
            JobID jobId,
            String jobName,
            Configuration jobConfiguration,
            Boundedness boundedness) {
        this.jobId = checkNotNull(jobId);
        this.jobName = checkNotNull(jobName);
        this.jobConfiguration = checkNotNull(jobConfiguration);
        this.boundedness = boundedness;
    }

    public JobID getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    public Boundedness getBoundedness() {
        return boundedness;
    }
}
