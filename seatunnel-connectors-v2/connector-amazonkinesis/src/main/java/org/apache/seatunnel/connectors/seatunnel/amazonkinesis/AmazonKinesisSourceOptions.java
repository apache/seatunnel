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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Shared-throughput Kinesis source settings. Read validation and serialized reader settings use
 * these stable user-facing option keys.
 */
public final class AmazonKinesisSourceOptions {
    public static final Option<String> START_POSITION =
            Options.key("start_position")
                    .stringType()
                    .defaultValue("TRIM_HORIZON")
                    .withDescription("TRIM_HORIZON, AT_TIMESTAMP or LATEST");
    public static final Option<String> START_TIMESTAMP =
            Options.key("start_timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ISO-8601 instant for AT_TIMESTAMP");
    public static final Option<Integer> RECORD_LIMIT =
            Options.key("record_limit")
                    .intType()
                    .defaultValue(500)
                    .withDescription("Requested GetRecords record limit, 1 to 10000");
    public static final Option<Integer> SHARD_POLL_MS =
            Options.key("shard_poll_ms")
                    .intType()
                    .defaultValue(250)
                    .withDescription("Minimum time between reads of one shard, 200 to 60000 ms");
    public static final Option<Integer> DISCOVERY_MS =
            Options.key("discovery_interval_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Streaming shard refresh interval, 1000 to 600000 ms");
    public static final Option<Integer> BATCH_TIMEOUT_MS =
            Options.key("batch_timeout_ms")
                    .intType()
                    .defaultValue(600000)
                    .withDescription("Batch catch-up deadline, 1000 to 86400000 ms");

    private AmazonKinesisSourceOptions() {}
}
