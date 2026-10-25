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
package org.apache.seatunnel.connectors.seatunnel.fluss.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.time.Duration;

public class FlussSourceOptions extends FlussBaseOptions {

    public static final Option<Long> POLL_TIMEOUT_MS =
            Options.key("poll.timeout.ms")
                    .longType()
                    .defaultValue(Duration.ofSeconds(10).toMillis())
                    .withDescription(
                            "The maximum time to block in the Fluss log scanner poll when "
                                    + "fetching records");

    public static final Option<StartMode> START_MODE =
            Options.key("start_mode")
                    .enumType(StartMode.class)
                    .defaultValue(StartMode.EARLIEST)
                    .withDescription(
                            "The offset each bucket starts reading from: [earliest] reads the "
                                    + "whole log from its earliest available offset; [latest] reads "
                                    + "only records appended after the job starts. [latest] is only "
                                    + "meaningful for streaming jobs and is rejected in BATCH mode.");
}
