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

package org.apache.seatunnel.engine.e2e.savepointfail;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

/**
 * Test-only sink whose writer rejects every checkpoint snapshot.
 *
 * <p>The test job sets a checkpoint interval far longer than the test itself, so the only barrier
 * that can reach {@link SavepointFailingSinkWriter#snapshotState(long)} is the final barrier of a
 * stop-with-savepoint request. That isolates the savepoint failure path from periodic checkpoint
 * timeouts, task failures and connector behaviour.
 */
public class SavepointFailingSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new SavepointFailingSinkWriter(context);
    }

    @Override
    public String getPluginName() {
        return SavepointFailingSinkFactory.IDENTIFIER;
    }
}
