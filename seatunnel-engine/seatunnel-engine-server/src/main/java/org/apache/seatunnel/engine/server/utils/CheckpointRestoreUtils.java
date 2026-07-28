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

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.RestoreMode;

public final class CheckpointRestoreUtils {

    private CheckpointRestoreUtils() {}

    /**
     * Returns whether a checkpoint type is eligible for the requested restore mode.
     *
     * <p>{@link RestoreMode#SAVEPOINT} accepts only savepoint. {@link RestoreMode#CHECKPOINT}
     * accepts regular completed checkpoints and completed-point snapshots used by the runtime.
     * {@link RestoreMode#NONE} never matches because it is not a restore request.
     */
    public static boolean matchesRestoreCheckpointType(
            CheckpointType checkpointType, RestoreMode restoreMode) {
        if (restoreMode == RestoreMode.CHECKPOINT) {
            return checkpointType == CheckpointType.CHECKPOINT_TYPE
                    || checkpointType == CheckpointType.COMPLETED_POINT_TYPE;
        }
        if (restoreMode == RestoreMode.SAVEPOINT) {
            return checkpointType == CheckpointType.SAVEPOINT_TYPE;
        }
        return false;
    }
}
