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

package org.apache.seatunnel.connectors.seatunnel.snmp.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SnmpTargetFactoryTest {

    @Test
    void testInvalidAddressUsesConnectorExceptionWithoutCommunity() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "invalid/host");
        values.put("community", "private-community");
        SnmpSinkConfig config = new SnmpSinkConfig(ReadonlyConfig.fromMap(values));

        SnmpConnectorException exception =
                Assertions.assertThrows(
                        SnmpConnectorException.class, () -> SnmpTargetFactory.create(config));

        Assertions.assertTrue(exception.getMessage().contains("SNMP agent address"));
        Assertions.assertFalse(exception.getMessage().contains("private-community"));
        Assertions.assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    }
}
