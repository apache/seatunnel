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

import org.apache.seatunnel.api.configuration.util.OptionRule;

public class FileCollectOptionRules {

    public static OptionRule rule() {
        return OptionRule.builder()
                .required(FileCollectOptions.PATHS)
                .optional(
                        FileCollectOptions.ID,
                        EdgeInputOptions.TYPE,
                        FileCollectOptions.ENCODING,
                        FileCollectOptions.READ_FROM_BEGINNING,
                        FileCollectOptions.GLOB_SCAN_INTERVAL_MS,
                        FileCollectOptions.CLOSE_INACTIVE_MS,
                        FileCollectOptions.ON_ERROR,
                        FileCollectOptions.MULTILINE_PATTERN,
                        FileCollectOptions.MULTILINE_MATCH,
                        FileCollectOptions.MULTILINE_NEGATE,
                        FileCollectOptions.MULTILINE_MAX_LINES,
                        FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS,
                        FileCollectOptions.OUTPUT_FORMAT_TYPE)
                .build();
    }
}
