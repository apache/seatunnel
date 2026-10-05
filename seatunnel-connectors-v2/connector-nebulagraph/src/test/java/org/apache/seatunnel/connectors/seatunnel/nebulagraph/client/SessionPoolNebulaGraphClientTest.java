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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SessionPoolNebulaGraphClientTest {

    @Test
    void reportsConnectionFailureWithTheReducedRuntimeDependencies() {
        Map<String, Object> values = new HashMap<>();
        values.put("hosts", Arrays.asList("127.0.0.1:1"));
        values.put("username", "root");
        values.put("password", "nebula");
        values.put("space", "test");
        values.put("tag", "person");
        values.put("vid_field", "id");
        values.put("timeout_millis", 10);
        values.put("max_retries", 0);

        assertThrows(
                NebulaGraphConnectorException.class,
                () ->
                        new SessionPoolNebulaGraphClient(
                                NebulaGraphSinkConfig.of(ReadonlyConfig.fromMap(values))));
    }
}
