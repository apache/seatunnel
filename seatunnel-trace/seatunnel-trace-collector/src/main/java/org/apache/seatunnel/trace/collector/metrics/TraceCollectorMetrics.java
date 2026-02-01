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

package org.apache.seatunnel.trace.collector.metrics;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

public class TraceCollectorMetrics {

    public final Counter httpRequestsTotal =
            Counter.build()
                    .name("st_trace_collector_http_requests_total")
                    .help("HTTP requests total")
                    .labelNames("path", "method", "code")
                    .register();

    public final Histogram httpRequestSeconds =
            Histogram.build()
                    .name("st_trace_collector_http_request_seconds")
                    .help("HTTP request latency in seconds")
                    .labelNames("path", "method")
                    .register();

    public final Counter eventsReceivedTotal =
            Counter.build()
                    .name("st_trace_collector_events_received_total")
                    .help("Events received total")
                    .labelNames("event_type")
                    .register();

    public final Counter stainTraceReceivedTotal =
            Counter.build()
                    .name("st_trace_collector_stain_trace_received_total")
                    .help("StainTrace events received total")
                    .register();

    public final Counter invalidEventsTotal =
            Counter.build()
                    .name("st_trace_collector_invalid_events_total")
                    .help("Invalid events total")
                    .labelNames("reason")
                    .register();

    public final Counter dbWriteFailuresTotal =
            Counter.build()
                    .name("st_trace_collector_db_write_failures_total")
                    .help("DB write failures total")
                    .labelNames("table")
                    .register();

    public final Counter dbReadFailuresTotal =
            Counter.build()
                    .name("st_trace_collector_db_read_failures_total")
                    .help("DB read failures total")
                    .labelNames("op")
                    .register();
}
