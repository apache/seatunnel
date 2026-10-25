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

package org.apache.seatunnel.edge.agent.connector.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class FileCollectOptions {

    public static final Option<String> ID =
            Options.key("id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Logical identifier for the input source.");

    public static final Option<List<String>> PATHS =
            Options.key("paths")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("Glob patterns for files to collect.");

    public static final Option<String> ENCODING =
            Options.key("encoding")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("Character encoding for file reading.");

    public static final Option<Boolean> READ_FROM_BEGINNING =
            Options.key("read-from-beginning")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When true, read files from the beginning; otherwise tail-follow from EOF.");

    public static final Option<Long> GLOB_SCAN_INTERVAL_MS =
            Options.key("glob-scan-interval-ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("Interval in milliseconds between glob scans for new files.");

    public static final Option<Long> CLOSE_INACTIVE_MS =
            Options.key("close-inactive-ms")
                    .longType()
                    .defaultValue(300000L)
                    .withDescription(
                            "Close file handles after this many milliseconds of inactivity.");

    public static final Option<String> ON_ERROR =
            Options.key("on-error")
                    .stringType()
                    .defaultValue("skip")
                    .withDescription(
                            "Error handling strategy: \"skip\" to skip bad records, \"fail\" to abort.");

    public static final Option<String> MULTILINE_PATTERN =
            Options.key("multiline.pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Multiline boundary regex; omit to disable multiline assembly.");

    public static final Option<String> MULTILINE_MATCH =
            Options.key("multiline.match")
                    .stringType()
                    .defaultValue("after")
                    .withDescription("Multiline matching direction: \"after\" or \"before\".");

    public static final Option<Boolean> MULTILINE_NEGATE =
            Options.key("multiline.negate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Negate the multiline pattern match.");

    public static final Option<Integer> MULTILINE_MAX_LINES =
            Options.key("multiline.max-lines")
                    .intType()
                    .defaultValue(500)
                    .withDescription("Maximum number of lines to combine in a multiline event.");

    public static final Option<Long> MULTILINE_FLUSH_IDLE_TIMEOUT_MS =
            Options.key("multiline.flush-idle-timeout-ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription(
                            "Flush the multiline buffer when the first buffered line is older than"
                                    + " this threshold (ms). Prevents indefinite buffering when no new"
                                    + " boundary line arrives. Must be > 0 when multiline is enabled.");

    public static final Option<String> OUTPUT_FORMAT_TYPE =
            Options.key("output-format.type")
                    .stringType()
                    .defaultValue("line")
                    .withDescription("Record format: \"line\" for raw lines, \"json\" for JSON.");
}
