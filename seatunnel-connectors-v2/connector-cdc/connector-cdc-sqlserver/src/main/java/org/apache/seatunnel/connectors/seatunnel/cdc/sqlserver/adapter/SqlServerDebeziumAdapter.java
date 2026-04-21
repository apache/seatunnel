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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.adapter;

import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumAdapter;
import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumEventDispatcherConfig;
import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumSchemaHistory;
import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumTopicNaming;
import org.apache.seatunnel.connectors.cdc.base.debezium.TableChangeInfo;

import java.util.Collection;

public class SqlServerDebeziumAdapter implements DebeziumAdapter {

    private static final String DEBEZIUM_VERSION = "1.9.8.Final";
    private static final String CONNECTOR_TYPE = "sqlserver";

    @Override
    public DebeziumEventDispatcher createEventDispatcher(DebeziumEventDispatcherConfig config) {
        return new SqlServerEventDispatcherAdapter(config);
    }

    @Override
    public DebeziumSchemaHistory createSchemaHistory(
            String instanceName, Collection<TableChangeInfo> tableChanges) {
        return new SqlServerSchemaHistoryAdapter(instanceName, tableChanges);
    }

    @Override
    public DebeziumTopicNaming createTopicNaming(String logicalName, String heartbeatPrefix) {
        throw new UnsupportedOperationException(
                "SqlServer connector does not use adapter for topic naming");
    }

    @Override
    public String getDebeziumVersion() {
        return DEBEZIUM_VERSION;
    }

    @Override
    public boolean supports(String connectorType) {
        return CONNECTOR_TYPE.equalsIgnoreCase(connectorType);
    }
}
