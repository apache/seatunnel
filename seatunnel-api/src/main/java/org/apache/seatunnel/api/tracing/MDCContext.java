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

package org.apache.seatunnel.api.tracing;

import org.apache.seatunnel.common.constants.JobMode;

import org.slf4j.MDC;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.Serializable;

/**
 * MDC context for tracing.
 *
 * <p>reference: https://www.slf4j.org/manual.html#mdc
 *
 * <p>Example:
 *
 * <pre>
 *     try (MDCContext ctx = MDCContext.of(jobId, pipelineId, taskId).activate()) {
 *          // do something
 *          new Thread(new MDCRunnable(MDCContext.current(), new Runnable() {
 *             @Override
 *             public void run() {
 *                  // do something
 *             }
 *          }))
 *          .start();
 *     }
 *     // MDC context will be restored after the try block
 * </pre>
 */
@Slf4j
@EqualsAndHashCode
public class MDCContext implements Serializable, Closeable {
    private static final MDCContext EMPTY = new MDCContext(null, null, null, null);
    private static final String EMPTY_TO_STRING = "NA";

    public static final String JOB_ID = "ST-JID";
    public static final String PIPELINE_ID = "ST-PID";
    public static final String TASK_ID = "ST-TID";

    /**
     * Job mode key used by Log4j routing and cleanup policy classification.
     *
     * <p>This value is optional so mixed-version task contexts can fall back safely.
     */
    public static final String JOB_MODE = "ST-JOB-MODE";

    /**
     * Job id used by Log4j routing to isolate one job's log files.
     *
     * <p>A null value means the current execution is outside a job-scoped context.
     */
    private final Long jobId;

    /**
     * Optional pipeline id propagated to task-level logs.
     *
     * <p>The value stays null for job-level operations that do not enter a pipeline.
     */
    private final Long pipelineId;

    /**
     * Optional task id propagated to task-level logs.
     *
     * <p>The value stays null before the execution enters a concrete task.
     */
    private final Long taskId;

    /**
     * Optional job mode used by Log4j routing to distinguish streaming logs from batch logs.
     *
     * <p>A null mode routes logs to the unclassified fallback instead of guessing.
     */
    private final JobMode jobMode;

    private transient volatile MDCContext toRestore;

    /**
     * Create a backward-compatible MDC context without job mode classification.
     *
     * @param jobId job id used by job log routing
     * @param pipelineId pipeline id used by task log tracing, or null
     * @param taskId task id used by task log tracing, or null
     */
    public MDCContext(Long jobId, Long pipelineId, Long taskId) {
        this(jobId, pipelineId, taskId, null);
    }

    /**
     * Create an MDC context with optional job, pipeline, task, and mode dimensions.
     *
     * @param jobId job id used by job log routing
     * @param pipelineId pipeline id used by task log tracing, or null
     * @param taskId task id used by task log tracing, or null
     * @param jobMode job mode used by mode-aware job log routing, or null
     */
    public MDCContext(Long jobId, Long pipelineId, Long taskId, JobMode jobMode) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.taskId = taskId;
        this.jobMode = jobMode;
    }

    public synchronized MDCContext activate() {
        if (this == EMPTY) {
            return this;
        }

        if (this.toRestore != null) {
            throw new IllegalStateException("MDCContext is already activated");
        }
        this.toRestore = current();

        try {
            if (jobId != null) {
                MDC.put(JOB_ID, String.valueOf(jobId));
            }
            if (pipelineId != null) {
                MDC.put(PIPELINE_ID, String.valueOf(pipelineId));
            }
            if (taskId != null) {
                MDC.put(TASK_ID, String.valueOf(taskId));
            }
            if (jobMode != null) {
                MDC.put(JOB_MODE, jobMode.name());
            } else {
                // Job mode controls log cleanup policy, so a nested context must not inherit it.
                MDC.remove(JOB_MODE);
            }
        } catch (Throwable e) {
            log.error("Failed to put MDC context", e);
            throw e;
        }
        return this;
    }

    public synchronized MDCContext deactivate() {
        if (this == EMPTY) {
            return this;
        }

        if (this.toRestore == null) {
            throw new IllegalStateException("MDCContext is not activated");
        }

        try {
            MDC.remove(JOB_ID);
            MDC.remove(PIPELINE_ID);
            MDC.remove(TASK_ID);
            MDC.remove(JOB_MODE);
        } catch (Throwable e) {
            log.error("Failed to clear MDC context", e);
            throw e;
        }

        if (this.toRestore != null) {
            this.toRestore.activate();
        }

        return this;
    }

    @Override
    public void close() {
        deactivate();
    }

    @Override
    public String toString() {
        if (this == EMPTY) {
            return EMPTY_TO_STRING;
        }
        String legacyContext =
                String.format(
                        "%d/%d/%d",
                        jobId, pipelineId == null ? 0 : pipelineId, taskId == null ? 0 : taskId);
        if (jobMode == null) {
            return legacyContext;
        }
        return String.format("%s/%s", legacyContext, jobMode.name());
    }

    public static MDCContext of(long jobId) {
        return new MDCContext(jobId, null, null, null);
    }

    /**
     * Create a job-scoped context with an optional job mode for mode-aware log routing.
     *
     * @param jobId job id written to {@link #JOB_ID}
     * @param jobMode job mode written to {@link #JOB_MODE}, or null for fail-closed routing
     * @return a context that can be activated around job-scoped work
     */
    public static MDCContext of(long jobId, JobMode jobMode) {
        return new MDCContext(jobId, null, null, jobMode);
    }

    public static MDCContext of(long jobId, long pipelineId) {
        return new MDCContext(jobId, pipelineId, null, null);
    }

    public static MDCContext of(long jobId, long pipelineId, long taskId) {
        return new MDCContext(jobId, pipelineId, taskId, null);
    }

    /**
     * Create a task-scoped context with an optional job mode for mode-aware log routing.
     *
     * @param jobId job id written to {@link #JOB_ID}
     * @param pipelineId pipeline id written to {@link #PIPELINE_ID}
     * @param taskId task id written to {@link #TASK_ID}
     * @param jobMode job mode written to {@link #JOB_MODE}, or null for fail-closed routing
     * @return a context that can be activated around task-scoped work
     */
    public static MDCContext of(long jobId, long pipelineId, long taskId, JobMode jobMode) {
        return new MDCContext(jobId, pipelineId, taskId, jobMode);
    }

    public static MDCContext of(MDCContext context) {
        return new MDCContext(context.jobId, context.pipelineId, context.taskId, context.jobMode);
    }

    /**
     * Return the job mode captured in this context.
     *
     * @return job mode, or null when the caller intentionally falls back to unclassified routing
     */
    public JobMode getJobMode() {
        return jobMode;
    }

    public static MDCContext current() {
        String jobId = MDC.get(JOB_ID);
        if (jobId == null) {
            return EMPTY;
        }

        String pipelineId = MDC.get(PIPELINE_ID);
        String taskId = MDC.get(TASK_ID);
        String jobMode = MDC.get(JOB_MODE);
        return new MDCContext(
                Long.parseLong(jobId),
                pipelineId != null ? Long.parseLong(pipelineId) : null,
                taskId != null ? Long.parseLong(taskId) : null,
                parseJobMode(jobMode));
    }

    public static MDCContext valueOf(String s) {
        if (EMPTY_TO_STRING.equals(s)) {
            return EMPTY;
        }

        String[] arr = s.split("/");
        Long jobId = Long.parseLong(arr[0]);
        Long pipelineId = parseNullableId(arr[1]);
        Long taskId = parseNullableId(arr[2]);
        JobMode jobMode = arr.length >= 4 ? parseJobMode(arr[3]) : null;
        return new MDCContext(jobId, pipelineId, taskId, jobMode);
    }

    /**
     * Parse the zero sentinel used by the compact MDC wire string back into a nullable id.
     *
     * @param value numeric id text from the serialized MDC context
     * @return null when the serialized value is zero, otherwise the parsed id
     */
    private static Long parseNullableId(String value) {
        Long parsed = Long.parseLong(value);
        return parsed == 0 ? null : parsed;
    }

    /**
     * Parse an optional job mode and fail closed when an old or unknown value is seen.
     *
     * @param value job mode text from MDC or serialized context
     * @return a known job mode, or null so Log4j routes to the unclassified branch
     */
    private static JobMode parseJobMode(String value) {
        if (value == null || EMPTY_TO_STRING.equals(value)) {
            return null;
        }
        try {
            return JobMode.valueOf(value);
        } catch (IllegalArgumentException e) {
            log.warn("Unknown job mode in MDC context: {}", value, e);
            return null;
        }
    }
}
