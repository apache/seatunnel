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

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

/**
 * MDC context for tracing.
 *
 * <p>reference: https://www.slf4j.org/manual.html#mdc
 */
@Slf4j
@Builder
@EqualsAndHashCode
public class MDCContext implements Serializable {
    private static final MDCContext EMPTY = MDCContext.builder().build();
    private static final String EMPTY_TO_STRING = "NA";

    public static final String JOB_ID = "ST-JID";
    public static final String PIPELINE_ID = "ST-PID";
    public static final String TASK_ID = "ST-TID";

    private final Long jobId;
    private final Long pipelineId;
    private final Long taskId;

    public static MDCContext of(long jobId) {
        return MDCContext.builder().jobId(jobId).build();
    }

    public static MDCContext of(long jobId, long pipelineId) {
        return MDCContext.builder().jobId(jobId).pipelineId(pipelineId).build();
    }

    public static MDCContext of(long jobId, long pipelineId, long taskId) {
        return MDCContext.builder().jobId(jobId).pipelineId(pipelineId).taskId(taskId).build();
    }

    public static MDCContext current() {
        return MDCContext.builder()
                .jobId(MDC.get(JOB_ID) != null ? Long.parseLong(MDC.get(JOB_ID)) : null)
                .pipelineId(
                        MDC.get(PIPELINE_ID) != null ? Long.parseLong(MDC.get(PIPELINE_ID)) : null)
                .taskId(MDC.get(TASK_ID) != null ? Long.parseLong(MDC.get(TASK_ID)) : null)
                .build();
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

    @Override
    public String toString() {
        if (jobId != null) {
            return String.format(
                    "%d/%d/%d",
                    jobId, pipelineId == null ? 0 : pipelineId, taskId == null ? 0 : taskId);
        } else {
            return EMPTY_TO_STRING;
        }
    }

    public void put() {
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
    }

    public void clear() {
        try {
            MDC.remove(JOB_ID);
            MDC.remove(PIPELINE_ID);
            MDC.remove(TASK_ID);
        } catch (Throwable e) {
            log.error("Failed to clear MDC context", e);
            throw e;
        }
    }
}
