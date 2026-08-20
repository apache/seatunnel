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

package org.apache.seatunnel.api.source;

import java.io.Serializable;

/**
 * Optional reader-side gate contract required by dynamic lookup fact inputs.
 *
 * <p>A gate-capable reader must keep ownership of prepared split bytes while closed, snapshot that
 * ownership into a versioned state, restore it exactly once, and only activate the restored reader
 * after the lookup coordinator opens the fact gate.
 *
 * <p>Threading contract: command application runs on the source task thread, while checkpoint
 * snapshot and restore can be invoked from engine checkpoint/recovery threads. Implementations must
 * snapshot all gate flags and prepared split ownership as one atomic reader state.
 */
public interface FactSourceGateCapability extends Serializable {

    /** Prepares a closed gate before the reader polls records. */
    void prepareClosedGate() throws Exception;

    /** Snapshots the closed/open gate state together with prepared split ownership metadata. */
    SourceGateState snapshotGate(long checkpointId) throws Exception;

    /** Restores gate state before normal split delivery or polling is allowed. */
    void restoreGateState(SourceGateState gateState) throws Exception;

    /** Applies a coordinator command to transition the restored gate. */
    void applyGateCommand(SourceGateCommand command) throws Exception;
}
