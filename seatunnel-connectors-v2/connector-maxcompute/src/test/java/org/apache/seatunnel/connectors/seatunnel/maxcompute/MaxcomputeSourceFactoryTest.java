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

package org.apache.seatunnel.connectors.seatunnel.maxcompute;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.sink.MaxcomputeSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.source.MaxcomputeSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

public class MaxcomputeSourceFactoryTest {
    @Test
    void optionRule() {
        Assertions.assertNotNull((new MaxcomputeSourceFactory()).optionRule());
        Assertions.assertNotNull((new MaxcomputeSinkFactory()).optionRule());
    }

    /**
     * The six client timeout / retry options (3 for the ODPS REST client, 3 for the Tunnel client)
     * must be declared as optional in both the source and sink factory option rules, otherwise
     * users cannot override them from job configs.
     */
    @Test
    void optionRuleRegistersTimeoutAndRetryOptions() {
        assertTimeoutAndRetryOptionsRegistered(new MaxcomputeSourceFactory().optionRule());
        assertTimeoutAndRetryOptionsRegistered(new MaxcomputeSinkFactory().optionRule());
    }

    private void assertTimeoutAndRetryOptionsRegistered(OptionRule rule) {
        Set<String> optionalKeys = new HashSet<>();
        for (Option<?> option : rule.getOptionalOptions()) {
            optionalKeys.add(option.key());
        }
        // REST client (control plane)
        Assertions.assertTrue(
                optionalKeys.contains("connect_timeout_ms"), "connect_timeout_ms not registered");
        Assertions.assertTrue(
                optionalKeys.contains("read_timeout_ms"), "read_timeout_ms not registered");
        Assertions.assertTrue(optionalKeys.contains("retry_times"), "retry_times not registered");
        // Tunnel client (data plane)
        Assertions.assertTrue(
                optionalKeys.contains("tunnel_connect_timeout_ms"),
                "tunnel_connect_timeout_ms not registered");
        Assertions.assertTrue(
                optionalKeys.contains("tunnel_read_timeout_ms"),
                "tunnel_read_timeout_ms not registered");
        Assertions.assertTrue(
                optionalKeys.contains("tunnel_retry_times"), "tunnel_retry_times not registered");
    }
}
