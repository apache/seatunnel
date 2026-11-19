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

package org.apache.seatunnel.engine.imap.storage.file.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.util.Collections;
import java.util.Map;

/** Options for opt-in WAL rotation and snapshot compaction. */
@Getter
public final class CompactionConfig {
    public static final Option<Boolean> ENABLED =
            Options.key("compaction.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Compact sealed IMap WAL files. Currently supports HDFS and local files only.");

    public static final Option<Long> INTERVAL_SECONDS =
            Options.key("compaction.interval-seconds")
                    .longType()
                    .defaultValue(300L)
                    .withDescription(
                            "Delay in seconds between compaction attempts for each IMap writer.");

    public static final Option<Long> SEGMENT_SIZE =
            Options.key("wal.segment-size")
                    .longType()
                    .defaultValue(64L * 1024 * 1024)
                    .withDescription(
                            "Seal a WAL segment after at least this many bytes when compaction is enabled. A record is never split.");

    private final boolean enabled;
    private final long intervalSeconds;
    private final long segmentSize;

    public CompactionConfig(Map<String, Object> properties) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
        enabled = config.get(ENABLED);
        intervalSeconds = config.get(INTERVAL_SECONDS);
        segmentSize = config.get(SEGMENT_SIZE);
        if (enabled && (intervalSeconds <= 0 || segmentSize <= 0)) {
            throw new IllegalArgumentException(
                    "compaction.interval-seconds and wal.segment-size must be positive when compaction.enabled=true");
        }
    }

    public static CompactionConfig disabled() {
        return new CompactionConfig(Collections.emptyMap());
    }
}
