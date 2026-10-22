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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.master.JobHistoryService;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoordinatorServiceStatusTest {

    @Test
    void missingHistoryRecordIsUnknowable() {
        assertEquals(JobStatus.UNKNOWABLE, CoordinatorService.resolveHistoryStatus(null));
    }

    @Test
    void missingStatusInsideHistoryRecordIsUnknowable() {
        JobHistoryService.JobState jobState =
                new JobHistoryService.JobState(1L, "job", null, 1L, null, null, null, null);

        assertEquals(JobStatus.UNKNOWABLE, CoordinatorService.resolveHistoryStatus(jobState));
    }

    @Test
    void knownHistoryStatusIsPreserved() {
        JobHistoryService.JobState jobState =
                new JobHistoryService.JobState(
                        1L, "job", JobStatus.FINISHED, 1L, null, null, null, null);

        assertEquals(JobStatus.FINISHED, CoordinatorService.resolveHistoryStatus(jobState));
    }
}
