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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class HugeGraphOptions {

    public static final String PLUGIN_NAME = "HugeGraph";

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HugeGraph server host");

    public static final Option<Integer> PORT =
            Options.key("port").intType().noDefaultValue().withDescription("HugeGraph server port");

    public static final Option<String> PROTOCOL =
            Options.key("protocol")
                    .stringType()
                    .defaultValue("http")
                    .withDescription("HugeGraph server protocol. Supported values: http, https");

    public static final Option<String> GRAPH_NAME =
            Options.key("graph_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the graph to be operated on");

    public static final Option<String> GRAPH_SPACE =
            Options.key("graph_space")
                    .stringType()
                    .defaultValue("DEFAULT")
                    .withDescription(
                            "The graph space the graph belongs to. Defaults to 'DEFAULT'.");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HugeGraph username");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HugeGraph password");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size").intType().defaultValue(500).withDescription("The batch size");

    public static final Option<Integer> BATCH_INTERVAL_MS =
            Options.key("batch_interval_ms")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            "Retained for compatibility. Configure sink.flush.interval in the "
                                    + "job env block for Zeta timer flush.");

    public static final Option<Boolean> CHECK_VERTEX =
            Options.key("check_vertex")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the server verifies that an edge's source/target vertices "
                                    + "exist when writing edges. When false (default), edges whose "
                                    + "endpoints were never loaded are written as orphan edges (or "
                                    + "trigger server-side phantom vertex auto-creation). Enable to "
                                    + "reject such edges.");

    public static final Option<Boolean> BATCH_FAILURE_FALLBACK =
            Options.key("batch_failure_fallback")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, a failed batch insert falls back to inserting records one "
                                    + "by one so a single bad ('poison') record no longer fails the "
                                    + "whole batch. Failed records are logged and skipped; the rest "
                                    + "succeed. Default false (fail-fast): any batch failure fails "
                                    + "the task immediately. Opt in explicitly when record skipping "
                                    + "is acceptable.");

    public static final Option<Integer> MAX_INSERT_ERRORS =
            Options.key("max_insert_errors")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Maximum number of records that may be skipped by the single-record "
                                    + "fallback (batch_failure_fallback=true) before the task is "
                                    + "failed. Default 0: any skipped record fails the task. Set to "
                                    + "-1 for unlimited (never fail on skipped records). Only applies "
                                    + "when batch_failure_fallback is enabled.");

    public static final Option<String> FAILURE_DATA_PATH =
            Options.key("failure_data_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional local directory. When set, every record skipped by the "
                                    + "single-record fallback is appended (as the mapped vertex/edge "
                                    + "id, label, properties and the server error) to a per-subtask "
                                    + "file under this directory for offline investigation. Note: in "
                                    + "cluster mode the file is created on the worker node running "
                                    + "the sink subtask, not the submitting client.");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries").intType().defaultValue(3).withDescription("The retry times");

    public static final Option<Integer> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            "The base retry backoff time in milliseconds. Backoff grows "
                                    + "exponentially per attempt (retry_backoff_ms * 2^(attempt-1)), "
                                    + "capped at retry_backoff_max_ms.");

    public static final Option<Integer> RETRY_BACKOFF_MAX_MS =
            Options.key("retry_backoff_max_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Upper bound in milliseconds for the exponential retry backoff, so a "
                                    + "high max_retries cannot produce pathologically long sleeps.");
}
