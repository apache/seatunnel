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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.config.JobConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the single job-level checkpoint predicate shared by coordinator job cleanup and physical
 * plan generation.
 *
 * <p>Plan generation stamps this answer into the managed Source lane selection, where it drives how
 * long the assignment ledger retains split ownership. Regressing it would either leak ownership for
 * non-checkpointed batch jobs or release it too early for streaming jobs.
 */
class JobCheckpointUtilsTest {

    @Test
    void shouldTreatMissingJobConfigAsCheckpointed() {
        Assertions.assertTrue(JobCheckpointUtils.isCheckpointEnabled(null));
    }

    @Test
    void shouldAlwaysCheckpointStreamingJobs() {
        Assertions.assertTrue(
                JobCheckpointUtils.isCheckpointEnabled(jobConfig(JobMode.STREAMING, false)));
    }

    @Test
    void shouldCheckpointBatchJobsOnlyWithAnExplicitInterval() {
        Assertions.assertFalse(
                JobCheckpointUtils.isCheckpointEnabled(jobConfig(JobMode.BATCH, false)));
        Assertions.assertTrue(
                JobCheckpointUtils.isCheckpointEnabled(jobConfig(JobMode.BATCH, true)));
    }

    private static JobConfig jobConfig(JobMode jobMode, boolean withCheckpointInterval) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext().setJobMode(jobMode));
        if (withCheckpointInterval) {
            jobConfig.getEnvOptions().put(EnvCommonOptions.CHECKPOINT_INTERVAL.key(), 10000);
        }
        return jobConfig;
    }
}
