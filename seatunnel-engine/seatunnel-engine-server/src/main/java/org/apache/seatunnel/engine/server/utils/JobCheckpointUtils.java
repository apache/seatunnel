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

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.config.JobConfig;

/**
 * Shared job-level checkpoint predicates.
 *
 * <p>Coordinator job cleanup and physical plan generation both need the same answer to "does this
 * job run checkpoints?". Keeping a single implementation prevents the two paths from drifting: plan
 * generation stamps the answer into the managed Source lane selection, where it decides how long
 * the assignment ledger retains split ownership. A divergent copy would silently retain or release
 * ownership under different rules than the engine uses to decide whether checkpoints exist at all.
 */
public final class JobCheckpointUtils {

    private JobCheckpointUtils() {}

    /**
     * Returns whether the supplied job runs checkpoints.
     *
     * <p>Streaming jobs always checkpoint. A batch job only checkpoints when it explicitly
     * configures a checkpoint interval.
     *
     * @param jobConfig job configuration; {@code null} for engine-internal jobs that carry no user
     *     configuration, which are treated as checkpointed
     * @return {@code true} when checkpoints are enabled for this job
     */
    public static boolean isCheckpointEnabled(JobConfig jobConfig) {
        // A JobConfig without a JobContext never occurs for a real submitted job — the engine
        // always attaches one before physical plan generation runs. Minimal test fixtures built
        // directly around a LogicalDag/JobConfig throughout the engine test suite do leave it
        // unset, though, and this predicate now runs unconditionally during plan generation for
        // every job. Treat an absent JobContext the same as an absent JobConfig: the job mode is
        // unknown, so assume checkpoints are enabled rather than throwing.
        if (jobConfig == null || jobConfig.getJobContext() == null) {
            return true;
        }
        return jobConfig.getJobContext().getJobMode() != JobMode.BATCH
                || jobConfig
                        .getEnvOptions()
                        .containsKey(EnvCommonOptions.CHECKPOINT_INTERVAL.key());
    }
}
