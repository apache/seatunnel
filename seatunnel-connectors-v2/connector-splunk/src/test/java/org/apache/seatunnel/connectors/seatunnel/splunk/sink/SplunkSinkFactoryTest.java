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

package org.apache.seatunnel.connectors.seatunnel.splunk.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SplunkSinkFactoryTest {

    @Test
    void factoryIdentifierMatchesThePluginMappingEntry() {
        Assertions.assertEquals("Splunk", new SplunkSinkFactory().factoryIdentifier());
    }

    @Test
    void urlAndTokenAreDeclaredRequired() {
        OptionRule optionRule = new SplunkSinkFactory().optionRule();

        Assertions.assertTrue(
                optionRule.getRequiredOptions().stream()
                        .anyMatch(option -> option.getOptions().contains(SplunkSinkOptions.URL)));
        Assertions.assertTrue(
                optionRule.getRequiredOptions().stream()
                        .anyMatch(option -> option.getOptions().contains(SplunkSinkOptions.TOKEN)));
    }

    @Test
    void batchingAndTlsOptionsAreDeclaredOptional() {
        OptionRule optionRule = new SplunkSinkFactory().optionRule();

        Assertions.assertTrue(optionRule.getOptionalOptions().contains(SplunkSinkOptions.INDEX));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(SplunkSinkOptions.MAX_BATCH_SIZE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(SplunkSinkOptions.MAX_RETRY_COUNT));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(SplunkSinkOptions.TLS_VERIFY_CERTIFICATE));
    }
}
