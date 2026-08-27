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
    private static final MDCContext EMPTY = new MDCContext(null, null, null);
    private static final String EMPTY_TO_STRING = "NA";

    public static final String JOB_ID = "ST-JID";
    public static final String PIPELINE_ID = "ST-PID";
    public static final String TASK_ID = "ST-TID";

    private final Long jobId;
    private final Long pipelineId;
    private final Long taskId;
    private transient volatile MDCContext toRestore;

    public MDCContext(Long jobId, Long pipelineId, Long taskId) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.taskId = taskId;
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
        return String.format(
                "%d/%d/%d",
                jobId, pipelineId == null ? 0 : pipelineId, taskId == null ? 0 : taskId);
    }

    public static MDCContext of(long jobId) {
        return new MDCContext(jobId, null, null);
    }

    public static MDCContext of(long jobId, long pipelineId) {
        return new MDCContext(jobId, pipelineId, null);
    }

    public static MDCContext of(long jobId, long pipelineId, long taskId) {
        return new MDCContext(jobId, pipelineId, taskId);
    }

    public static MDCContext of(MDCContext context) {
        return new MDCContext(context.jobId, context.pipelineId, context.taskId);
    }

    public static MDCContext current() {
        String jobId = MDC.get(JOB_ID);
        if (jobId == null) {
            return EMPTY;
        }

        String pipelineId = MDC.get(PIPELINE_ID);
        String taskId = MDC.get(TASK_ID);
        return new MDCContext(
                Long.parseLong(jobId),
                pipelineId != null ? Long.parseLong(pipelineId) : null,
                taskId != null ? Long.parseLong(taskId) : null);
    }

    public static MDCContext valueOf(String s) {
        if (EMPTY_TO_STRING.equals(s)) {
            return EMPTY;
        }

        String[] arr = s.split("/");
        Long jobId = Long.parseLong(arr[0]);
        Long pipelineId = Long.parseLong(arr[1]);
        Long taskId = Long.parseLong(arr[2]);
        if (pipelineId == 0 || taskId == 0) {
            return MDCContext.of(jobId);
        }
        return MDCContext.of(jobId, pipelineId, taskId);
    }
}
