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

package org.apache.seatunnel.engine.server.checkpoint;

public enum CheckpointCloseReason {
    PIPELINE_END("Pipeline turn to end state."),
    CHECKPOINT_EXPIRED(
            "Checkpoint expired before completing. Please increase checkpoint timeout in the seatunnel.yaml or jobConfig env."),
    CHECKPOINT_COORDINATOR_COMPLETED("CheckpointCoordinator completed."),
    CHECKPOINT_COORDINATOR_SHUTDOWN("CheckpointCoordinator shutdown."),
    CHECKPOINT_COORDINATOR_RESET("CheckpointCoordinator reset."),
    CHECKPOINT_INSIDE_ERROR("CheckpointCoordinator inside have error."),
    AGGREGATE_COMMIT_ERROR("Aggregate commit error."),
    TASK_NOT_ALL_READY_WHEN_SAVEPOINT("Task not all ready, savepoint error"),
    CHECKPOINT_NOTIFY_COMPLETE_FAILED("Checkpoint notify complete failed");

    private final String message;

    CheckpointCloseReason(String message) {
        this.message = message;
    }

    public String message() {
        return message;
    }
}
