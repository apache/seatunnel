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

import org.apache.seatunnel.api.configuration.util.OptionRule;

public class EdgeAgentRuntimeOptionRules {

    public static OptionRule agentRule() {
        return OptionRule.builder()
                .optional(
                        EdgeAgentRuntimeOptions.AGENT_ID,
                        EdgeAgentRuntimeOptions.DELIVERY_GUARANTEE)
                .build();
    }

    public static OptionRule queueRule() {
        return OptionRule.builder()
                .optional(
                        EdgeAgentRuntimeOptions.QUEUE_SQLITE_PATH,
                        EdgeAgentRuntimeOptions.QUEUE_POLL_BATCH_SIZE,
                        EdgeAgentRuntimeOptions.QUEUE_ACKED_RETENTION_MS,
                        EdgeAgentRuntimeOptions.QUEUE_CLEANUP_BATCH_SIZE,
                        EdgeAgentRuntimeOptions.QUEUE_RESURRECT_BATCH_SIZE,
                        EdgeAgentRuntimeOptions.QUEUE_RESURRECT_INTERVAL_MS)
                .build();
    }

    public static OptionRule runtimeRule() {
        return OptionRule.builder()
                .optional(
                        EdgeAgentRuntimeOptions.RUNTIME_IDLE_SLEEP_MS,
                        EdgeAgentRuntimeOptions.RUNTIME_BULK_MAX_SIZE,
                        EdgeAgentRuntimeOptions.RUNTIME_FLUSH_INTERVAL_MS)
                .build();
    }

    public static OptionRule retryRule() {
        return OptionRule.builder()
                .optional(
                        EdgeAgentRuntimeOptions.RETRY_MAX_ATTEMPTS,
                        EdgeAgentRuntimeOptions.RETRY_BACKOFF_MS,
                        EdgeAgentRuntimeOptions.RETRY_BACKOFF_MAX_MS)
                .build();
    }
}
