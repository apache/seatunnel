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

package org.apache.seatunnel.engine.server.trace;

/** Shared keys and metric names used by the stain trace sampling and reporting pipeline. */
public final class StainTraceConstants {
    private StainTraceConstants() {}

    public static final long NO_TRACE_ID = -1L;

    public static final String TRACE_PAYLOAD_OPTION_KEY = "__st_trace_payload";

    public static final String METRIC_SAMPLES_GENERATED_TOTAL =
            "stain_trace_samples_generated_total";
    public static final String METRIC_EVENTS_REPORTED_TOTAL = "stain_trace_events_reported_total";
    public static final String METRIC_BUDGET_THROTTLED_TOTAL = "stain_trace_budget_throttled_total";
    public static final String METRIC_ENTRIES_TRUNCATED_TOTAL =
            "stain_trace_entries_truncated_total";
    public static final String METRIC_INVALID_PAYLOAD_TOTAL = "stain_trace_invalid_payload_total";
}
