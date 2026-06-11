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

package org.apache.seatunnel.api.common.multitable;

import lombok.Getter;

import java.io.Serializable;

/** A normalized failed-table record used by parser and runtime multi-table components. */
@Getter
public class MultiTableFailedTable implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String tablePath;
    private final MultiTableFailurePhase phase;
    private final String pluginName;
    private final String exceptionClass;
    private final String messageSummary;
    private final long firstFailureTime;
    // Throwable chains may capture connector internals that are not safe to ship with job graph
    // serialization. Failed-table metadata is the stable contract across stages.
    private final transient Throwable cause;

    public MultiTableFailedTable(
            String tablePath,
            MultiTableFailurePhase phase,
            String pluginName,
            String exceptionClass,
            String messageSummary,
            long firstFailureTime,
            Throwable cause) {
        this.tablePath = tablePath;
        this.phase = phase;
        this.pluginName = pluginName;
        this.exceptionClass = exceptionClass;
        this.messageSummary = messageSummary;
        this.firstFailureTime = firstFailureTime;
        this.cause = cause;
    }
}
