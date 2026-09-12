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

package org.apache.seatunnel.connectors.seatunnel.splunk;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSourceParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class SplunkSourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        SplunkSourceFactory factory = new SplunkSourceFactory();
        Assertions.assertEquals("Splunk", factory.factoryIdentifier());
    }

    @Test
    public void testHeadersInitializationWithoutExistingHeaders() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());
        String apiKey = "test-splunk-api-key";

        SplunkSourceParameter parameter = new SplunkSourceParameter();
        parameter.buildWithConfig(config, apiKey);

        Assertions.assertNotNull(parameter.getHeaders());
        Assertions.assertEquals(apiKey, parameter.getHeaders().get("Authorization"));
    }
}
