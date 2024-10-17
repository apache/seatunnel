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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.core.job.JobStatus;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobStatusRunner implements Runnable {

    private final JobClient jobClient;
    private final Long jobId;
    private boolean isEnterPending = false;

    public JobStatusRunner(JobClient jobClient, Long jobId) {
        this.jobClient = jobClient;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("job-status-runner-" + jobId);
        try {
            while (isPrint(jobClient.getJobStatus(jobId))) {
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            log.error("Failed to get job runner status. {}", ExceptionUtils.getMessage(e));
        }
    }

    private boolean isPrint(String jobStatus) {
        boolean isPrint = true;
        switch (JobStatus.fromString(jobStatus)) {
            case PENDING:
                isEnterPending = true;
                log.info(
                        "Job Id : {} enter pending queue, current status:{} ,please wait task schedule",
                        jobId,
                        jobStatus);
                break;
            case RUNNING:
            case SCHEDULED:
            case FAILING:
            case FAILED:
            case DOING_SAVEPOINT:
            case SAVEPOINT_DONE:
            case CANCELING:
            case CANCELED:
            case FINISHED:
            case UNKNOWABLE:
                if (isEnterPending) {
                    // Log only if it transitioned from the PENDING state
                    log.info(
                            "Job ID: {} has been scheduled and entered the next state. Current status: {}",
                            jobId,
                            jobStatus);
                }
                isPrint = false;
            default:
                break;
        }
        return isPrint;
    }
}
