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

package org.apache.seatunnel.connectors.seatunnel.activemq.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.ALWAYS_SESSION_ASYNC;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.ALWAYS_SYNC_SEND;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.CHECK_FOR_DUPLICATE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.CLOSE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.COPY_MESSAGE_ON_SEND;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.DISABLE_TIMESTAMP_BY_DEFAULT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.DISPATCH_ASYNC;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.NESTED_MAP_AND_LIST_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.PORT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.URI;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.USE_COMPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig.WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT;

@AutoService(Factory.class)
public class ActivemqSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "ActiveMQ";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(QUEUE_NAME, URI)
                .bundled(USERNAME, PASSWORD)
                .optional(
                        HOST,
                        PORT,
                        CLIENT_ID,
                        CHECK_FOR_DUPLICATE,
                        COPY_MESSAGE_ON_SEND,
                        DISABLE_TIMESTAMP_BY_DEFAULT,
                        USE_COMPRESSION,
                        ALWAYS_SESSION_ASYNC,
                        ALWAYS_SYNC_SEND,
                        CLOSE_TIMEOUT,
                        DISPATCH_ASYNC,
                        NESTED_MAP_AND_LIST_ENABLED,
                        WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new ActivemqSink(context.getOptions(), context.getCatalogTable());
    }
}
