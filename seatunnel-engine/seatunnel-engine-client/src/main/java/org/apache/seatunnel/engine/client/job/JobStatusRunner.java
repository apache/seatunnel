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

import org.apache.seatunnel.engine.core.job.JobStatus;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class JobStatusRunner implements Runnable {
    private final JobClient jobClient;
    private final Long jobId;
    private final AtomicReference<String> atomicReference;

    public JobStatusRunner(
            JobClient jobClient, Long jobId, AtomicReference<String> atomicReference) {
        this.jobClient = jobClient;
        this.jobId = jobId;
        this.atomicReference = atomicReference;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("job-status-runner-" + jobId);
        try {
            String jobStatus = jobClient.getJobStatus(jobId);
            String lastJobStatus = atomicReference.get();
            if (lastJobStatus == null
                    || lastJobStatus.equals(JobStatus.PENDING.toString())
                    || !lastJobStatus.equals(jobStatus)) {
                atomicReference.set(jobStatus);
                log.info("Job status: {}", jobStatus);
            }

        } catch (Exception e) {
            log.warn("Failed to get job runner status.");
        }
    }
}
