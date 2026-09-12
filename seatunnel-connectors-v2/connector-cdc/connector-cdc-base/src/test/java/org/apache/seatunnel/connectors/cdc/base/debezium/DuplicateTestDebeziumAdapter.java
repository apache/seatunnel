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
 * Second test-only {@link DebeziumAdapter} that claims the same connector class as {@link
 * TestDebeziumAdapter}, plus a shared duplicate class. Used to verify that {@link
 * DebeziumAdapterFactory} rejects ambiguous matches.
 */
public class DuplicateTestDebeziumAdapter implements DebeziumAdapter {

    static final String DUPLICATE_CONNECTOR_CLASS = "io.debezium.connector.duplicate.DupConnector";

    @Override
    public String getDebeziumVersion() {
        return "2.0.0.Final";
    }

    @Override
    public boolean supports(String connectorClassName) {
        return DUPLICATE_CONNECTOR_CLASS.equals(connectorClassName);
    }
}
