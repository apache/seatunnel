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

/**
 * Test-only adapter that supports the same connector as {@link TestDebeziumAdapter} so duplicate
 * provider detection can be exercised.
 */
public class SecondTestDebeziumAdapter implements DebeziumAdapter {

    /** Shares the same connector class as the primary test adapter to simulate an ambiguous SPI. */
    static final String TEST_CONNECTOR_CLASS = TestDebeziumAdapter.TEST_CONNECTOR_CLASS;

    /** Distinct Debezium version label used to identify this provider in assertions. */
    static final String TEST_DEBEZIUM_VERSION = "3.0.0.Final";

    /** Returns the Debezium version exposed by this test adapter. */
    @Override
    public String getDebeziumVersion() {
        return TEST_DEBEZIUM_VERSION;
    }

    /** Supports the same connector class as the primary test adapter to force ambiguity. */
    @Override
    public boolean supports(String connectorClassName) {
        return TEST_CONNECTOR_CLASS.equals(connectorClassName);
    }
}
