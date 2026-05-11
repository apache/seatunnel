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

package org.apache.seatunnel.edge.agent.connector;

import java.util.List;

/**
 * Collects data from a local edge-side source for forwarding upstream.
 *
 * <p><strong>Lifecycle:</strong> {@link #open()} must be invoked before {@link #poll(int)}, and
 * {@link #close()} should be invoked exactly once after use (typically in a {@code finally} block).
 * Implementations may reject {@link #poll(int)} before {@link #open()} or after {@link #close()}.
 *
 * <p><strong>Record format:</strong> each {@link String} returned by {@link #poll(int)} is one JSON
 * text value (typically one JSON object per line, NDJSON). Blank lines are skipped.
 */
public interface AgentInput {

    /** Stable identifier for logging and metrics. */
    String id();

    /** Starts input lifecycle and allocates required resources. */
    void open() throws Exception;

    /**
     * Polls up to {@code maxRecords} JSON records from this input.
     *
     * @param maxRecords maximum number of records to return; non-positive values yield an empty
     *     list
     * @return zero or more records; never {@code null}
     */
    List<String> poll(int maxRecords) throws Exception;

    /** Stops input lifecycle and releases resources. */
    void close() throws Exception;
}
