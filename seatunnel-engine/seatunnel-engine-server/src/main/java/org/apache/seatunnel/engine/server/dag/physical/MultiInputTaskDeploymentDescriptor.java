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

package org.apache.seatunnel.engine.server.dag.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Deployment payload for one subtask of a port-aware multi-input action.
 */
public final class MultiInputTaskDeploymentDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Stable lookup operator identity.
     */
    private final String operatorUid;

    /**
     * Physical-plan action ID for this descriptor.
     */
    private final long actionId;

    /**
     * Target lookup subtask index.
     */
    private final int subtaskIndex;

    /**
     * Immutable explicit input-port declarations.
     */
    private final List<InputPortDescriptor> inputPorts;

    /**
     * Creates the deployment descriptor for one multi-input task.
     *
     * @param operatorUid stable lookup operator identity
     * @param actionId physical-plan action ID
     * @param subtaskIndex target lookup subtask index
     * @param inputPorts explicit input-port declarations
     */
    public MultiInputTaskDeploymentDescriptor(
            String operatorUid,
            long actionId,
            int subtaskIndex,
            List<InputPortDescriptor> inputPorts) {
        if (operatorUid == null || operatorUid.trim().isEmpty()) {
            throw new IllegalArgumentException("operatorUid must not be blank");
        }
        if (subtaskIndex < 0) {
            throw new IllegalArgumentException(
                    "subtaskIndex must be non-negative: " + subtaskIndex);
        }
        if (inputPorts == null || inputPorts.size() < 2) {
            throw new IllegalArgumentException(
                    "A multi-input task requires at least two input ports");
        }
        Set<Integer> portIds = new HashSet<>();
        for (InputPortDescriptor inputPort : inputPorts) {
            if (!portIds.add(inputPort.getInputPortId())) {
                throw new IllegalArgumentException(
                        "Duplicate input port: " + inputPort.getInputPortId());
            }
        }
        this.operatorUid = operatorUid;
        this.actionId = actionId;
        this.subtaskIndex = subtaskIndex;
        this.inputPorts = Collections.unmodifiableList(new ArrayList<>(inputPorts));
    }

    public String getOperatorUid() {
        return operatorUid;
    }

    public long getActionId() {
        return actionId;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public List<InputPortDescriptor> getInputPorts() {
        return inputPorts;
    }
}
