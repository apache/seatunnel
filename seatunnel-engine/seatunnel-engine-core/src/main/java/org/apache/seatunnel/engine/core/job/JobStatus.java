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

package org.apache.seatunnel.engine.core.job;

/** Possible states of a job once it has been accepted by the dispatcher. */
public enum JobStatus {
    /**
     * The job has been received by the Dispatcher, and is waiting for the job manager to receive
     * leadership and to be created.
     */
    INITIALIZING(EndState.NOT_END),

    /** Job is newly created, no task has started to run. */
    CREATED(EndState.NOT_END),

    /**
     * Job will scheduler every pipeline, each PhysicalVertex in the pipeline will be scheduler and
     * deploying
     */
    SCHEDULED(EndState.NOT_END),

    /** The job is already running, and each pipeline is already running. */
    RUNNING(EndState.NOT_END),

    /** The job has failed and is currently waiting for the cleanup to complete. */
    FAILING(EndState.NOT_END),

    /** The job has failed with a non-recoverable task failure. */
    FAILED(EndState.GLOBALLY),

    /** Job is being cancelled. */
    CANCELING(EndState.NOT_END),

    /** Job has been cancelled. */
    CANCELED(EndState.GLOBALLY),

    /** All of the job's tasks have successfully finished. */
    FINISHED(EndState.GLOBALLY),

    /** Cannot find the JobID or the job status has already been cleared. */
    UNKNOWABLE(EndState.GLOBALLY);

    // --------------------------------------------------------------------------------------------

    private enum EndState {
        NOT_END,
        LOCALLY,
        GLOBALLY
    }

    private final EndState endState;

    JobStatus(EndState endState) {
        this.endState = endState;
    }

    public boolean isEndState() {
        return endState != EndState.NOT_END;
    }
}
