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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.ADMIN_SERVICE_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PLUGIN_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CLIENT_SERVICE_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_RESET_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_STARTUP_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_STARTUP_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_STOP_MODE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CURSOR_STOP_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.POLL_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.POLL_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.POLL_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.SUBSCRIPTION_NAME;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.TOPIC_DISCOVERY_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.TOPIC_PATTERN;

@AutoService(Factory.class)
public class PulsarSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return PulsarConfigUtil.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SUBSCRIPTION_NAME, CLIENT_SERVICE_URL, ADMIN_SERVICE_URL)
                .optional(
                        CURSOR_STARTUP_MODE,
                        CURSOR_STOP_MODE,
                        TOPIC_DISCOVERY_INTERVAL,
                        POLL_TIMEOUT,
                        POLL_INTERVAL,
                        POLL_BATCH_SIZE,
                        ConnectorCommonOptions.SCHEMA)
                .exclusive(TOPIC, TOPIC_PATTERN)
                .conditional(
                        CURSOR_STARTUP_MODE,
                        SourceProperties.StartMode.TIMESTAMP,
                        CURSOR_STARTUP_TIMESTAMP)
                .conditional(
                        CURSOR_STARTUP_MODE,
                        SourceProperties.StartMode.SUBSCRIPTION,
                        CURSOR_RESET_MODE)
                .conditional(
                        CURSOR_STOP_MODE,
                        SourceProperties.StopMode.TIMESTAMP,
                        CURSOR_STOP_TIMESTAMP)
                .bundled(AUTH_PLUGIN_CLASS, AUTH_PARAMS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return PulsarSource.class;
    }
}
