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

package org.apache.seatunnel.connectors.seatunnel.bigtable.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class BigtableSourceOptions extends BigtableBaseOptions {

    public static final Option<String> START_ROW_KEY =
            Options.key("start_rowkey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bigtable scan start row key (inclusive).");

    public static final Option<String> END_ROW_KEY =
            Options.key("end_rowkey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bigtable scan end row key (exclusive).");

    public static final Option<Long> START_TIMESTAMP =
            Options.key("start_timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Start timestamp (inclusive) for scan time range in microseconds since epoch.");

    public static final Option<Long> END_TIMESTAMP =
            Options.key("end_timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "End timestamp (exclusive) for scan time range in microseconds since epoch.");

    public static final Option<Integer> MAX_VERSIONS =
            Options.key("max_versions")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Maximum number of cell versions to return per column. Default is 1 (latest only).");

    public static final Option<Integer> SCAN_ROW_LIMIT =
            Options.key("scan_row_limit")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Maximum number of rows to scan. -1 (default) means no limit.");
}
