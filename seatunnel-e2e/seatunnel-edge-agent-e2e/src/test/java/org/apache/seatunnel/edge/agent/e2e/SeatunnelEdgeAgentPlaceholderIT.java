/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.e2e;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Skeleton integration suite; enable tests after Edge Agent E2E fixtures are implemented. */
public class SeatunnelEdgeAgentPlaceholderIT extends EdgeAgentE2eTestBase {

    @Test
    @Disabled("TODO: Start Edge Agent (classpath Main or dist scripts) and assert readiness.")
    void future_edge_agent_startup() {
        Assertions.fail("Not implemented - placeholder for startup lifecycle assertions.");
    }

    @Test
    @Disabled("TODO: Parse representative agent.yaml and validate required keys without live IO.")
    void future_agent_yaml_config_parsing() {
        Assertions.fail("Not implemented - placeholder for config/schema checks.");
    }

    @Test
    @Disabled(
            "TODO: Minimal path (e.g. noop/stub collector) through batch accumulator + transport.")
    void future_end_to_end_path() {
        Assertions.fail("Not implemented - placeholder for thin pipeline verification.");
    }
}
