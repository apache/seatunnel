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

/** Test-only {@link DebeziumAdapter} implementation registered via META-INF/services. */
public class TestDebeziumAdapter implements DebeziumAdapter {

    static final String TEST_CONNECTOR_CLASS = "io.debezium.connector.test.TestConnector";
    static final String TEST_DEBEZIUM_VERSION = "1.9.8.Final";

    @Override
    public String getDebeziumVersion() {
        return TEST_DEBEZIUM_VERSION;
    }

    @Override
    public boolean supports(String connectorClassName) {
        return TEST_CONNECTOR_CLASS.equals(connectorClassName)
                || DuplicateTestDebeziumAdapter.DUPLICATE_CONNECTOR_CLASS.equals(
                        connectorClassName);
    }
}
