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
import java.util.List;

/** Immutable set of physical channels assigned to one target input port. */
public final class InputPortDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Explicit target port ID. */
    private final int inputPortId;

    /** Immutable physical channels routed to this port. */
    private final List<PhysicalInputChannel> channels;

    /**
     * Creates one input-port declaration and validates channel ownership.
     *
     * @param inputPortId explicit target port ID
     * @param channels physical channels assigned to the port
     */
    public InputPortDescriptor(int inputPortId, List<PhysicalInputChannel> channels) {
        if (inputPortId < 0) {
            throw new IllegalArgumentException("inputPortId must be non-negative: " + inputPortId);
        }
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException(
                    "Input port " + inputPortId + " must declare at least one channel");
        }
        for (PhysicalInputChannel channel : channels) {
            if (channel.getLogicalChannelKey().getTargetInputPort() != inputPortId) {
                throw new IllegalArgumentException(
                        "Channel port "
                                + channel.getLogicalChannelKey().getTargetInputPort()
                                + " does not match descriptor port "
                                + inputPortId);
            }
        }
        this.inputPortId = inputPortId;
        this.channels = Collections.unmodifiableList(new ArrayList<>(channels));
    }

    public int getInputPortId() {
        return inputPortId;
    }

    public List<PhysicalInputChannel> getChannels() {
        return channels;
    }
}
