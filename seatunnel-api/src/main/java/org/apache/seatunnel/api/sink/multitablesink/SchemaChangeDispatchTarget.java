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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * Records one concrete sub-writer that must observe the current schema change, together with the
 * reason why the coordinator selected it.
 */
final class SchemaChangeDispatchTarget {

    /** Exact sub-writer instance that should receive the schema change. */
    private final SinkIdentifier sinkIdentifier;
    /** Target writer that will execute the schema-change mutation. */
    private final SinkWriter<SeaTunnelRow, ?, ?> writer;
    /** Explains whether this target came from source match or shared physical sink match. */
    private final String reason;

    SchemaChangeDispatchTarget(
            SinkIdentifier sinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?> writer, String reason) {
        this.sinkIdentifier = sinkIdentifier;
        this.writer = writer;
        this.reason = reason;
    }

    SinkIdentifier getSinkIdentifier() {
        return sinkIdentifier;
    }

    SinkWriter<SeaTunnelRow, ?, ?> getWriter() {
        return writer;
    }

    String getReason() {
        return reason;
    }
}
