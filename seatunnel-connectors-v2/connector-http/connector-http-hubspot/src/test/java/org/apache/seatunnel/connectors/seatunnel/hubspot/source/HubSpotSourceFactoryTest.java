/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hubspot.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HubSpotSourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        HubSpotSourceFactory factory = new HubSpotSourceFactory();
        Assertions.assertEquals(
                "HubSpot", factory.factoryIdentifier(), "Factory identifier should be HubSpot");
    }

    @Test
    public void testOptionRule() {
        HubSpotSourceFactory factory = new HubSpotSourceFactory();
        OptionRule optionRule = factory.optionRule();

        Assertions.assertNotNull(optionRule, "OptionRule should not be null");

        // Verifies that ACCESS_TOKEN is required
        Assertions.assertTrue(
                optionRule.getRequiredOptions().contains(HubSpotSourceOptions.ACCESS_TOKEN),
                "ACCESS_TOKEN must be a required option");

        // Verifies that OBJECT_TYPE is optional (which fixes the unit-test crash from earlier)
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(HubSpotSourceOptions.OBJECT_TYPE),
                "OBJECT_TYPE must be an optional option because it has a default value");
    }
}
