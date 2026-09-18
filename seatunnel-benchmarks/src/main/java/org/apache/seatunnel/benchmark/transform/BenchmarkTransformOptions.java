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

package org.apache.seatunnel.benchmark.transform;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Options for the deterministic benchmark transform. */
public final class BenchmarkTransformOptions {

    public static final Option<Integer> OPERATIONS_PER_ROW =
            Options.key("operations_per_row")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Number of deterministic hash-mixing operations per row.");

    public static final Option<Boolean> COPY_ROW =
            Options.key("copy_row")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to copy the input row before writing the checksum.");

    private BenchmarkTransformOptions() {}
}
