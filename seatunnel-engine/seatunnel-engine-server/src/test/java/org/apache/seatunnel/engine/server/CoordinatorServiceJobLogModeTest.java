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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers job-mode extraction used before opening job-scoped log appenders.
 *
 * <p>The regression target is safe routing when the persisted job payload is incomplete.
 */
public class CoordinatorServiceJobLogModeTest {

    /**
     * A complete immutable job payload should expose the exact configured job mode.
     *
     * <p>This protects streaming jobs from being routed as unclassified at startup.
     */
    @Test
    void testExtractJobModeFromImmutableInformation() {
        JobImmutableInformation jobImmutableInformation = mock(JobImmutableInformation.class);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext(123L).setJobMode(JobMode.STREAMING));
        when(jobImmutableInformation.getJobConfig()).thenReturn(jobConfig);

        assertEquals(JobMode.STREAMING, CoordinatorService.extractJobMode(jobImmutableInformation));
    }

    /**
     * Missing job config or job context should fail closed to unclassified log routing.
     *
     * <p>The engine must not guess a streaming mode when old or broken metadata is restored.
     */
    @Test
    void testExtractJobModeReturnsNullWhenInformationIsIncomplete() {
        JobImmutableInformation jobImmutableInformation = mock(JobImmutableInformation.class);

        assertNull(CoordinatorService.extractJobMode(null));
        assertNull(CoordinatorService.extractJobMode(jobImmutableInformation));

        JobConfig jobConfig = new JobConfig();
        when(jobImmutableInformation.getJobConfig()).thenReturn(jobConfig);
        assertNull(CoordinatorService.extractJobMode(jobImmutableInformation));
    }
}
