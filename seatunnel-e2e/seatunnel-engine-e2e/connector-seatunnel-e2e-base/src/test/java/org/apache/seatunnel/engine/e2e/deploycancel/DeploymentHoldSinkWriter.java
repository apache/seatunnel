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

package org.apache.seatunnel.engine.e2e.deploycancel;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;

/**
 * Test-only writer whose construction parks on {@link DeploymentHoldGate}.
 *
 * <p>The engine creates the writer from the action's restore function, i.e. after the task group
 * owns its slot and before the task reports {@code READY_START}. Parking the constructor therefore
 * pins a real deployment at its exact "slot applied, task not started" step, and any row reaching
 * {@link #write(SeaTunnelRow)} proves the hold was no longer in effect.
 */
public class DeploymentHoldSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    public DeploymentHoldSinkWriter() {
        DeploymentHoldGate.awaitDeploymentHold();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        DeploymentHoldGate.recordRow();
    }

    @Override
    public void close() throws IOException {
        // Nothing buffered: rows are only counted to prove the task never started.
    }
}
