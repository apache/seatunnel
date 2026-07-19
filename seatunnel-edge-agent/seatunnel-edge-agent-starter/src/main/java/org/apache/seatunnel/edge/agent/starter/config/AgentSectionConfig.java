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

package org.apache.seatunnel.edge.agent.starter.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;

import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

@Getter
public class AgentSectionConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String agentId;
    private final EdgeDeliveryGuarantee deliveryGuarantee;

    public AgentSectionConfig(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config");
        ConfigValidator.of(config).validate(EdgeAgentRuntimeOptionRules.agentRule());

        String rawId = config.get(EdgeAgentRuntimeOptions.AGENT_ID);
        if (rawId == null || rawId.trim().isEmpty()) {
            throw new IllegalArgumentException("agent.id must be non-empty after resolution.");
        }
        this.agentId = rawId.trim();

        String rawDeliveryGuarantee = config.get(EdgeAgentRuntimeOptions.DELIVERY_GUARANTEE);
        EdgeDeliveryGuarantee.validateSupported(rawDeliveryGuarantee);
        this.deliveryGuarantee = EdgeDeliveryGuarantee.from(rawDeliveryGuarantee);
    }

    public static AgentSectionConfig from(ReadonlyConfig config) {
        return new AgentSectionConfig(config);
    }
}
