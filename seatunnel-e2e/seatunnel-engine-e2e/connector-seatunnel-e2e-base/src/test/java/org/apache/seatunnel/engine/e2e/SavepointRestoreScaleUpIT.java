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

package org.apache.seatunnel.engine.e2e;

import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Verifies that a savepoint taken at one parallelism can be restored at a HIGHER parallelism
 * (scale-up), exercising the modulo-based per-task state remap in {@code
 * CheckpointCoordinator#restoreTaskState} with a genuinely different old/new parallelism pair.
 *
 * <p>Every other checkpoint/savepoint restore IT in this package (e.g. {@link
 * CheckpointRestoreWithStopIT}, {@link SavepointRestoreIT}) restores at the SAME parallelism the
 * checkpoint was taken at, so the remap's {@code currentParallelism !=
 * actionState.getParallelism()} branch is never exercised end-to-end anywhere else.
 *
 * <p>Rigor mirrors {@link SavepointRestoreIT}: exact offset reconciliation (no loss, no
 * duplication) across the savepoint boundary. In addition, this test verifies the RESTORED job's
 * actual physical task count via the {@code /trace/task-mapping} REST endpoint (backed by the live
 * {@code JobMaster#getPhysicalPlan()}, the same white-box source of truth used elsewhere in this
 * test family), rather than trusting that the configured parallelism was applied.
 */
public class SavepointRestoreScaleUpIT extends AbstractSavepointRescaleIT {

    private static final String ORIGINAL_CONF_FILE =
            "/savepoint-restore-rescale/stream_p2_to_localfile_scaleup.conf";
    private static final String RESTORE_CONF_FILE =
            "/savepoint-restore-rescale/stream_p4_to_localfile_scaleup.conf";
    private static final String SINK_OUTPUT_DIR =
            HOST_VOLUME_MOUNT_PATH + "/savepoint-restore-rescale/scale-up/sinkfile";

    // Must match env.parallelism in stream_p2_to_localfile_scaleup.conf.
    private static final int ORIGINAL_PARALLELISM = 2;
    // Must match env.parallelism in stream_p4_to_localfile_scaleup.conf.
    private static final int RESTORED_PARALLELISM = 4;

    @Override
    protected String originalConfFile() {
        return ORIGINAL_CONF_FILE;
    }

    @Override
    protected String restoreConfFile() {
        return RESTORE_CONF_FILE;
    }

    @Override
    protected String sinkOutputDir() {
        return SINK_OUTPUT_DIR;
    }

    @Override
    protected int originalParallelism() {
        return ORIGINAL_PARALLELISM;
    }

    @Override
    protected int restoredParallelism() {
        return RESTORED_PARALLELISM;
    }

    @Test
    public void testRestoreFromSavepointWithHigherParallelism()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        verifySavepointRestoreWithRescale();
    }
}
