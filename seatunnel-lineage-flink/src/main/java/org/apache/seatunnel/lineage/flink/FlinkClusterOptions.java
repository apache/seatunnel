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

package org.apache.seatunnel.lineage.flink;

import org.apache.flink.configuration.GlobalConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Reads the Flink cluster configuration that lineage options and the receiver token come from. */
public final class FlinkClusterOptions {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkClusterOptions.class);

    private FlinkClusterOptions() {}

    /**
     * Loads the Flink cluster configuration once per JVM.
     *
     * <p>{@link GlobalConfiguration#loadConfiguration()} reads {@code config.yaml} (Flink 1.20+) or
     * {@code flink-conf.yaml} (legacy) from disk. The result cannot change within a process, while
     * the status hook consults it on every terminal callback, so the read is memoized. The client
     * and the JobManager memoize their own copies, which is correct because they read their own
     * configuration directory.
     */
    public static Map<String, String> load() {
        return Holder.OPTIONS;
    }

    /** Defers the disk read to first use; class initialisation makes it happen exactly once. */
    private static final class Holder {
        static final Map<String, String> OPTIONS = read();

        private static Map<String, String> read() {
            try {
                return Collections.unmodifiableMap(
                        new LinkedHashMap<>(GlobalConfiguration.loadConfiguration().toMap()));
            } catch (Throwable error) {
                LOGGER.debug("Unable to load Flink cluster configuration for lineage", error);
                return Collections.emptyMap();
            }
        }
    }
}
