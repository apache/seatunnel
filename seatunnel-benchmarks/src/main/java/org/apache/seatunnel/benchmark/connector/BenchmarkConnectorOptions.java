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

package org.apache.seatunnel.benchmark.connector;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import java.util.Collections;

/** Shared schema and options for the benchmark source and sink. */
public final class BenchmarkConnectorOptions {

    public static final String PLUGIN_OUTPUT = "benchmark_rows";

    public static final Option<Long> TOTAL_ROWS =
            Options.key("total_rows")
                    .longType()
                    .defaultValue(10_000_000L)
                    .withDescription("Total number of rows emitted across all source subtasks.");

    public static final Option<Long> RATE_PER_SECOND =
            Options.key("rate_per_second")
                    .longType()
                    .defaultValue(250_000L)
                    .withDescription(
                            "Open-loop offered rate across all source subtasks. Zero means unlimited.");

    public static final Option<Integer> PAYLOAD_SIZE =
            Options.key("payload_size")
                    .intType()
                    .defaultValue(256)
                    .withDescription("Generated payload size in characters.");

    public static final Option<Integer> TRANSFORM_OPERATIONS =
            Options.key("transform_operations")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Configured transform hash operations per row.");

    public static final Option<Long> START_DELAY_MILLIS =
            Options.key("start_delay_millis")
                    .longType()
                    .defaultValue(2_000L)
                    .withDescription(
                            "Delay before the common open-loop schedule starts, in milliseconds.");

    public static final Option<Integer> EMIT_BATCH_SIZE =
            Options.key("emit_batch_size")
                    .intType()
                    .defaultValue(1_024)
                    .withDescription("Maximum number of due rows emitted by one source poll.");

    public static final Option<String> RESULT_PATH =
            Options.key("result_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Directory where the benchmark sink writes its JSON result.");

    public static final Option<String> RUN_ID =
            Options.key("run_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Unique identifier used as the benchmark result file name.");

    public static final Option<Long> EXPECTED_ROWS =
            Options.key("expected_rows")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Expected global row count used for correctness checks.");

    public static final Option<Integer> MAX_TRACKED_LATENCY_MILLIS =
            Options.key("max_tracked_latency_millis")
                    .intType()
                    .defaultValue(60_000)
                    .withDescription(
                            "Largest latency represented exactly in the in-memory histogram.");

    public static final Option<Long> MAX_P99_LATENCY_MILLIS =
            Options.key("max_p99_latency_millis")
                    .longType()
                    .defaultValue(1_000L)
                    .withDescription("P99 limit used by the sustainable-throughput verdict.");

    public static final Option<Double> MAX_LATENCY_GROWTH_RATIO =
            Options.key("max_latency_growth_ratio")
                    .doubleType()
                    .defaultValue(1.20D)
                    .withDescription("Maximum allowed second-half P99 divided by first-half P99.");

    private BenchmarkConnectorOptions() {}

    public static CatalogTable catalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "sequence",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "Global sequence number"))
                        .column(
                                PhysicalColumn.of(
                                        "scheduled_at_millis",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "Open-loop scheduled generation time"))
                        .column(
                                PhysicalColumn.of(
                                        "payload",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "Deterministic benchmark payload"))
                        .column(
                                PhysicalColumn.of(
                                        "checksum",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        false,
                                        0L,
                                        "Transform checksum consumed by the sink"))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("benchmark", "benchmark", "events"),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "SeaTunnel system benchmark events");
    }
}
