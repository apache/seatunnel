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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EdgeAgentRuntimeOptionRulesTest {

    @Test
    void rulesAreDefined() {
        Assertions.assertNotNull(EdgeAgentRuntimeOptionRules.agentRule());
        Assertions.assertNotNull(EdgeAgentRuntimeOptionRules.queueRule());
        Assertions.assertNotNull(EdgeAgentRuntimeOptionRules.runtimeRule());
        Assertions.assertNotNull(EdgeAgentRuntimeOptionRules.retryRule());
    }

    @Test
    void queueRuleAcceptsEmptyMap() {
        ConfigValidator.of(ReadonlyConfig.fromMap(Collections.emptyMap()))
                .validate(EdgeAgentRuntimeOptionRules.queueRule());
    }

    @Test
    void queueRuleAcceptsMinimalConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeAgentRuntimeOptions.QUEUE_SQLITE_PATH.key(), "data/wal.db");
        ConfigValidator.of(ReadonlyConfig.fromMap(map))
                .validate(EdgeAgentRuntimeOptionRules.queueRule());
    }

    @Test
    void agentRuleAcceptsEmptyMap() {
        ConfigValidator.of(ReadonlyConfig.fromMap(Collections.emptyMap()))
                .validate(EdgeAgentRuntimeOptionRules.agentRule());
    }

    @Test
    void missingSqlitePathUsesDefault() {
        QueueConfig queue = QueueConfig.from(ReadonlyConfig.fromMap(Collections.emptyMap()));
        Assertions.assertEquals(
                EdgeAgentRuntimeOptions.QUEUE_SQLITE_PATH.defaultValue(), queue.getSqlitePath());
    }

    @Test
    void runtimeRuleAcceptsEmptyMap() {
        ConfigValidator.of(ReadonlyConfig.fromMap(Collections.emptyMap()))
                .validate(EdgeAgentRuntimeOptionRules.runtimeRule());
    }

    @Test
    void validMapsBuildRuntimeConfig() {
        Map<String, Object> agent = new HashMap<>();
        agent.put(EdgeAgentRuntimeOptions.AGENT_ID.key(), "agent-1");
        Map<String, Object> queue = new HashMap<>();
        queue.put(EdgeAgentRuntimeOptions.QUEUE_SQLITE_PATH.key(), "target/wal.db");
        AgentRuntimeConfig runtime =
                AgentRuntimeConfig.compose(
                        AgentSectionConfig.from(ReadonlyConfig.fromMap(agent)),
                        QueueConfig.from(ReadonlyConfig.fromMap(queue)),
                        AgentSchedulerConfig.from(ReadonlyConfig.fromMap(Collections.emptyMap())),
                        RetryConfig.from(ReadonlyConfig.fromMap(Collections.emptyMap())));
        Assertions.assertEquals("target/wal.db", runtime.getSqlitePath());
        Assertions.assertEquals(EdgeDeliveryGuarantee.BEST_EFFORT, runtime.getDeliveryGuarantee());
        Assertions.assertEquals("agent-1", runtime.getAgentId());
    }

    @Test
    void nonDeliveryGuaranteeParsedCorrectly() {
        Assertions.assertEquals(EdgeDeliveryGuarantee.NON, EdgeDeliveryGuarantee.from("NON"));
        Assertions.assertEquals(EdgeDeliveryGuarantee.NON, EdgeDeliveryGuarantee.from("non"));
        Assertions.assertEquals(EdgeDeliveryGuarantee.NON, EdgeDeliveryGuarantee.from("NONE"));
        Assertions.assertEquals(EdgeDeliveryGuarantee.NON, EdgeDeliveryGuarantee.from("none"));
    }

    @Test
    void nonDeliveryGuaranteeMapsToMemFactory() {
        Map<String, Object> agent = new HashMap<>();
        agent.put(EdgeAgentRuntimeOptions.AGENT_ID.key(), "agent-non");
        agent.put(EdgeAgentRuntimeOptions.DELIVERY_GUARANTEE.key(), "NON");
        AgentRuntimeConfig runtime =
                AgentRuntimeConfig.compose(
                        AgentSectionConfig.from(ReadonlyConfig.fromMap(agent)),
                        QueueConfig.from(ReadonlyConfig.fromMap(Collections.emptyMap())),
                        AgentSchedulerConfig.from(ReadonlyConfig.fromMap(Collections.emptyMap())),
                        RetryConfig.from(ReadonlyConfig.fromMap(Collections.emptyMap())));
        Assertions.assertEquals(EdgeDeliveryGuarantee.NON, runtime.getDeliveryGuarantee());
        Assertions.assertEquals("mem", runtime.getDeliveryGuarantee().storeFactoryId());
    }

    @Test
    void unsupportedDeliveryGuaranteeThrows() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> EdgeDeliveryGuarantee.from("EXACTLY_ONCE"));
    }
}
