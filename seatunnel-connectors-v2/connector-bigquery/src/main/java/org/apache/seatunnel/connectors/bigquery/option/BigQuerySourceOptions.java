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

package org.apache.seatunnel.connectors.bigquery.option;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class BigQuerySourceOptions extends BigQueryBaseOptions {

    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "SQL query to execute against BigQuery. Either 'query' or the 'dataset_id' + 'table_id' pair is required.");

    public static final Option<Long> MAX_RESULTS =
            Options.key("max_results")
                    .longType()
                    .defaultValue(100L)
                    .withDescription(
                            "BigQuery API page size. The SDK's TableResult handles pagination internally via getNextPage() / getNextPageToken().");

    public static final Option<String> LOCATION =
            Options.key("location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "BigQuery job location, set on JobId.setLocation(). Required for non-US/EU jobs.");

    public static final Option<Boolean> USE_QUERY_CACHE =
            Options.key("use_query_cache")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to use BigQuery query result caching (default true), mapped to QueryJobConfiguration.useQueryCache().");

    public static final Option<Boolean> DRY_RUN =
            Options.key("dry_run")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, executes a dry run that returns estimated bytes scanned and cost without actually running the query, mapped to QueryJobConfiguration.dryRun().");

    public static final Option<Long> MAXIMUM_BYTES_BILLED =
            Options.key("maximum_bytes_billed")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Cost cap; the query fails if estimated bytes exceed this limit, preventing accidental full-table scans, mapped to QueryJobConfiguration.getMaximumBytesBilled().");

    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch_size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "SeaTunnel internal batch size — the maximum number of rows emitted to the downstream per poll cycle. "
                                    + "Zero means no limit (all rows are emitted in one cycle). "
                                    + "This is independent of max_results (the BigQuery API page size): a smaller fetch_size allows more frequent checkpointing between batches.");

    public static final Option<String> PARTITION_FILTER =
            Options.key("partition_filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Partition filter predicate for direct table reads (dataset_id + table_id mode only), "
                                    + "e.g. \"_PARTITIONDATE >= '2026-01-01'\". "
                                    + "Rejected with a clear error if combined with 'query'. "
                                    + "Required when the target table has requirePartitionFilter enabled in BigQuery metadata.");
}
