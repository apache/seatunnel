/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.mailchimp;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.LIST_ID;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.RETRY;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.mailchimp.MailchimpSourceOptions.TOKEN;

@AutoService(Factory.class)
public class MailchimpSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Mailchimp";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(TOKEN, LIST_ID, ConnectorCommonOptions.SCHEMA)
                .optional(
                        BASE_URL,
                        ALLOW_INSECURE_HTTP,
                        PAGE_SIZE,
                        RETRY,
                        RETRY_BACKOFF_MS,
                        REQUEST_TIMEOUT_MS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        MailchimpSource source = new MailchimpSource(context.getOptions());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MailchimpSource.class;
    }
}
