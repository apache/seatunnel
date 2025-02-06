/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.activemq.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.CHECK_FOR_DUPLICATE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.CLOSE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.CONSUMER_EXPIRY_CHECK_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.DISABLE_TIMESTAMP_BY_DEFAULT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.DISPATCH_ASYNC;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.URI;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT;

@AutoService(Factory.class)
public class ActivemqSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "ActiveMQ";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URI, QUEUE_NAME, SCHEMA)
                .bundled(USERNAME, PASSWORD)
                .optional(
                        FORMAT,
                        FIELD_DELIMITER,
                        CHECK_FOR_DUPLICATE,
                        CLIENT_ID,
                        DISABLE_TIMESTAMP_BY_DEFAULT,
                        CLOSE_TIMEOUT,
                        DISPATCH_ASYNC,
                        CONSUMER_EXPIRY_CHECK_ENABLED,
                        WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ActivemqSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(config);
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new ActivemqSource(config, catalogTable);
    }
}
