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

import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.ALWAYS_SESSION_ASYNC;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.ALWAYS_SYNC_SEND;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.CHECK_FOR_DUPLICATE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.CLOSE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.DISPATCH_ASYNC;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.NESTED_MAP_AND_LIST_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.URI;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT;

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
                        CLIENT_ID,
                        CHECK_FOR_DUPLICATE,
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
