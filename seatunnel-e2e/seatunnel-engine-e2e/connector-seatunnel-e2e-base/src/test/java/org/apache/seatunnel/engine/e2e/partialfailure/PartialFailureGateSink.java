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

package org.apache.seatunnel.engine.e2e.partialfailure;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

/**
 * Test-only sink without committers or writer state. Depending on {@code fail_after_release} it
 * either injects the runtime failure of one pipeline of a multi-pipeline job (see {@link
 * PartialFailureGateSinkWriter}, which parks on {@link PartialFailureGate}) or persists every row
 * it receives to a local text file as the healthy sibling's progress evidence.
 *
 * <p>Having no committer keeps the pipeline down to a split enumerator plus one chained
 * source-and-sink task per parallelism, and it makes the failure happen inside the sink task itself
 * instead of in a commit phase.
 */
public class PartialFailureGateSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    /** Key that binds the failing writer to the gate armed by the test. */
    private final String holdKey;

    /** Directory that receives one text file per writer instance. */
    private final String outputPath;

    /** Whether this sink injects the failure instead of writing the rows. */
    private final boolean failAfterRelease;

    public PartialFailureGateSink(String holdKey, String outputPath, boolean failAfterRelease) {
        this.holdKey = holdKey;
        this.outputPath = outputPath;
        this.failAfterRelease = failAfterRelease;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new PartialFailureGateSinkWriter(context, holdKey, outputPath, failAfterRelease);
    }

    @Override
    public String getPluginName() {
        return PartialFailureGateSinkFactory.IDENTIFIER;
    }
}
