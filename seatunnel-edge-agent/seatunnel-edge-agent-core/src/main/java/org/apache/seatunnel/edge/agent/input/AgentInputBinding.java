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

package org.apache.seatunnel.edge.agent.input;

import org.apache.seatunnel.edge.agent.connector.AgentInput;

/** Associates a YAML {@code inputs[].id} with the connector {@link AgentInput} built for it. */
public final class AgentInputBinding {

    private final String logicalId;
    private final AgentInput agentInput;

    public AgentInputBinding(String logicalId, AgentInput agentInput) {
        this.logicalId = logicalId;
        this.agentInput = agentInput;
    }

    public AgentInput getAgentInput() {
        return agentInput;
    }

    public String getLogicalId() {
        return logicalId;
    }
}
