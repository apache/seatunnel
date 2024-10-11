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

package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.MESSAGE_ROUTING_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SinkProperties.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.ADMIN_SERVICE_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PLUGIN_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CLIENT_SERVICE_URL;

@AutoService(Factory.class)
public class PulsarSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return PulsarConfigUtil.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(CLIENT_SERVICE_URL, ADMIN_SERVICE_URL, TOPIC)
                .optional(FORMAT, FIELD_DELIMITER, MESSAGE_ROUTING_MODE)
                .bundled(AUTH_PLUGIN_CLASS, AUTH_PARAMS)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new PulsarSink(context.getOptions(), context.getCatalogTable());
    }
}
