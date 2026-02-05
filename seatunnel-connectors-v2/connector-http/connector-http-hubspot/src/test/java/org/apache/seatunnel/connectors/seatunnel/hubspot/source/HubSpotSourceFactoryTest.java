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

import java.util.List;

public class HubSpotSourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        HubSpotSourceFactory factory = new HubSpotSourceFactory();
        Assertions.assertEquals("HubSpot", factory.factoryIdentifier());
    }

    @Test
    public void testOptionRule() {
        HubSpotSourceFactory factory = new HubSpotSourceFactory();
        OptionRule rule = factory.optionRule();

        // 1. Verify Required Options
        List<?> required = rule.getRequiredOptions();
        boolean foundAccessToken = false;

        // Robust check: Look for the key string inside the object's string representation
        for (Object obj : required) {
            if (obj.toString().contains("access_token")) {
                foundAccessToken = true;
                break;
            }
        }
        Assertions.assertTrue(
                foundAccessToken, "Required option 'access_token' not found in: " + required);

        // 2. Verify Optional Options
        List<?> optional = rule.getOptionalOptions();
        boolean foundObjectType = false;

        for (Object obj : optional) {
            if (obj.toString().contains("object_type")) {
                foundObjectType = true;
                break;
            }
        }
        Assertions.assertTrue(
                foundObjectType, "Optional option 'object_type' not found in: " + optional);
    }
}
