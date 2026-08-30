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

package org.apache.seatunnel.connectors.seatunnel.snmp.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkOptions;

import com.google.auto.service.AutoService;

/** Creates SNMPv2c SET sinks from table factory configuration. */
@AutoService(Factory.class)
public final class SnmpSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return SnmpSinkOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SnmpSinkOptions.HOST, SnmpSinkOptions.COMMUNITY)
                .optional(
                        SnmpSinkOptions.PORT,
                        SnmpSinkOptions.TIMEOUT_MILLIS,
                        SnmpSinkOptions.RETRIES,
                        SnmpSinkOptions.OID_FIELD,
                        SnmpSinkOptions.VALUE_FIELD,
                        SnmpSinkOptions.VALUE_TYPE_FIELD)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        SnmpSinkConfig config = new SnmpSinkConfig(context.getOptions());
        return () -> new SnmpSink(config, context.getCatalogTable());
    }
}
