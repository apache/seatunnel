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

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DebeziumAdapterFactoryTest {

    @Test
    void getAdapter_returnsAdapter_whenConnectorClassMatches() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        DebeziumAdapter adapter =
                DebeziumAdapterFactory.getAdapter(TestDebeziumAdapter.TEST_CONNECTOR_CLASS, cl);

        Assertions.assertInstanceOf(TestDebeziumAdapter.class, adapter);
        Assertions.assertEquals(
                TestDebeziumAdapter.TEST_DEBEZIUM_VERSION, adapter.getDebeziumVersion());
    }

    @Test
    void getAdapter_throwsIllegalStateException_whenNoAdapterMatches() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        DebeziumAdapterFactory.getAdapter(
                                "io.debezium.connector.unknown.UnknownConnector", cl));
    }
}
