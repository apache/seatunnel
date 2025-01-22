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

package org.apache.seatunnel.connectors.tailfile.source;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

@Getter
@ToString
@Builder
public class TailFileSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final Option<String> DIR =
            Options.key("dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The parent directory containing the log files.");

    public static final Option<String> PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Absolute path of the files. Regular expression (and not file system patterns)"
                                    + " can be used for filename only.");

    public static final Option<Integer> SCAN_INTERVAL =
            Options.key("scan_interval")
                    .intType()
                    .defaultValue(2000)
                    .withDescription(
                            "The interval in milliseconds to scan the log paths."
                                    + " The default is 2000ms.");

    public static final Option<Boolean> CACHE_PATTERN_MATCHING =
            Options.key("cache_pattern_matching")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Listing directories and applying the filename regex pattern may be time consuming"
                                    + " for directories containing thousands of files. Caching the list of matching "
                                    + "files can improve performance. The order in which files are consumed will also be cached."
                                    + " Requires that the file system keeps track of modification times with at least a 1-second granularity.");

    public static final Option<Boolean> SKIP_TO_END =
            Options.key("skip_to_end")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to skip the position to EOF in the case of files not written on the position file.");

    public static final Option<Integer> BUFFER_SIZE =
            Options.key("buffer_size")
                    .intType()
                    .defaultValue(1024 * 16)
                    .withDescription(
                            "The size in bytes of the buffer that each harvester uses when fetching a file."
                                    + " The default is 16384.");

    public static final Option<Integer> MAX_BATCH_COUNT =
            Options.key("max_batch_count")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "The max number of batch reads from a file in one loop. The default is Integer.MAX_VALUE.");

    public static final Option<Integer> MAX_MESSAGE_BYTES =
            Options.key("max_message_bytes")
                    .intType()
                    .defaultValue(1024 * 1024 * 10)
                    .withDescription(
                            "The maximum number of bytes that a single log message can have."
                                    + " All bytes after max_message_bytes are discarded and not sent."
                                    + " This setting is especially useful for multiline log messages,"
                                    + " which can get large. The default is 10MB.");

    public static final Option<String> MULTILINE_PATTERN =
            Options.key("multiline_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The regular expression to match the start of a new log message."
                                    + " This is used to support multiline log messages. If a log line does not match"
                                    + " the pattern, it is considered part of the previous log message."
                                    + " The default is null, which means that every line is a new log message."
                                    + " For example: ^\\d{4}-\\d{2}-\\d{2}\\s{1}\\d{2}:\\d{2}:\\d{2},\\d{3} indicates that"
                                    + " the log starts with the timestamp format: yyyy-MM-dd hh:mm:ss,SSS");

    public static final Option<String> HOSTNAME =
            Options.key("hostname")
                    .stringType()
                    .defaultValue(Utils.getHostname())
                    .withDescription("The hostname of the machine.");

    public static final Option<String> IP_ADDRESS =
            Options.key("ip")
                    .stringType()
                    .defaultValue(Utils.getIpAddress())
                    .withDescription("The IP address of the machine.");

    public static final Option<Map<String, String>> TAGS =
            Options.key("tags")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "A map of tags to add to each event. The default is an empty map.");

    public static final Option<String> CHARSET =
            Options.key("charset")
                    .stringType()
                    .defaultValue(StandardCharsets.UTF_8.name())
                    .withDescription("The charset of the log files. The default is UTF-8.");

    public static final Option<Integer> LINE_TIMEOUT =
            Options.key("line_timeout")
                    .intType()
                    .defaultValue((int) Duration.ofSeconds(10).toMillis())
                    .withDescription(
                            "After the specified timeout, sends the multiline event even if no new pattern"
                                    + " is found to start a new event. The default is 10000ms.");

    public static final Option<Integer> IDLE_TIMEOUT =
            Options.key("idle_timeout")
                    .intType()
                    .defaultValue((int) Duration.ofMinutes(2).toMillis())
                    .withDescription(
                            "Time (ms) to close inactive files. If the closed file is appended new lines to,"
                                    + " this source will automatically re-open it. The default is 120000ms.");

    public static final Option<Long> IGNORE_OLDER =
            Options.key("ignore_older")
                    .longType()
                    .defaultValue(Duration.ofDays(90).toMillis())
                    .withDescription(
                            "Ignore files that haven't been modified for the given time span."
                                    + " The default is 7776000000ms(90days).");

    private final String dir;
    private final String path;
    private final int scanInterval;
    private final boolean skipToEnd;
    private final boolean cachePatternMatching;
    private final int bufferSize;
    private final int maxBatchCount;
    private final int maxMessageBytes;
    private final String multilinePattern;
    private final int lineTimeout;
    private final String hostname;
    private final String ipAddress;
    private final Map<String, String> tags;
    private final String charset;
    private final int idleTimeout;
    private final long ignoreOlder;

    // todo 各种配置优化，参数优化，多表配置优化

    public static TailFileSourceConfig valueOf(ReadonlyConfig config) {
        TailFileSourceConfig sourceConfig =
                TailFileSourceConfig.builder()
                        .dir(config.get(DIR))
                        .path(config.get(PATH))
                        .scanInterval(config.get(SCAN_INTERVAL))
                        .skipToEnd(config.get(SKIP_TO_END))
                        .cachePatternMatching(config.get(CACHE_PATTERN_MATCHING))
                        .maxBatchCount(config.get(MAX_BATCH_COUNT))
                        .bufferSize(config.get(BUFFER_SIZE))
                        .maxMessageBytes(config.get(MAX_MESSAGE_BYTES))
                        .multilinePattern(config.get(MULTILINE_PATTERN))
                        .lineTimeout(config.get(LINE_TIMEOUT))
                        .idleTimeout(config.get(IDLE_TIMEOUT))
                        .ignoreOlder(config.get(IGNORE_OLDER))
                        .hostname(config.get(HOSTNAME))
                        .ipAddress(config.get(IP_ADDRESS))
                        .tags(config.get(TAGS))
                        .charset(config.get(CHARSET))
                        .build();

        Preconditions.checkArgument(
                sourceConfig.getScanInterval() > 1000,
                "scan_interval must be greater than 1000ms, current: "
                        + sourceConfig.getScanInterval());
        Preconditions.checkArgument(
                sourceConfig.getLineTimeout() > 2000,
                "line_timeout must be greater than 2000ms, current: "
                        + sourceConfig.getLineTimeout());
        Preconditions.checkArgument(
                sourceConfig.getIdleTimeout() > sourceConfig.getLineTimeout(),
                "idle_timeout must be greater than line_timeout, current: "
                        + sourceConfig.getIdleTimeout()
                        + ", line_timeout: "
                        + sourceConfig.getLineTimeout());
        Preconditions.checkArgument(
                sourceConfig.getIgnoreOlder() > sourceConfig.getIdleTimeout(),
                "ignore_older must be greater than idle_timeout, current: "
                        + sourceConfig.getIgnoreOlder()
                        + ", idle_timeout: "
                        + sourceConfig.getIdleTimeout());
        return sourceConfig;
    }
}
