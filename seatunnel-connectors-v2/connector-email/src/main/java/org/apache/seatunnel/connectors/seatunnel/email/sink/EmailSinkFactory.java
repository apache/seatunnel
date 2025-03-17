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

package org.apache.seatunnel.connectors.seatunnel.email.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_AUTHORIZATION_CODE;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_FROM_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_HOST;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_MESSAGE_CONTENT;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_MESSAGE_HEADLINE;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_SMTP_AUTH;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_TO_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig.EMAIL_TRANSPORT_PROTOCOL;

@AutoService(Factory.class)
public class EmailSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "EmailSink";
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new EmailSink(context.getOptions(), catalogTable);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        EMAIL_FROM_ADDRESS,
                        EMAIL_TO_ADDRESS,
                        EMAIL_HOST,
                        EMAIL_TRANSPORT_PROTOCOL,
                        EMAIL_SMTP_AUTH,
                        EMAIL_AUTHORIZATION_CODE,
                        EMAIL_MESSAGE_HEADLINE,
                        EMAIL_MESSAGE_CONTENT)
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }
}
