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
package org.apache.seatunnel.connectors.seatunnel.adobecommerce;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.END_TIME;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.PAGE_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.SOCKET_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.START_TIME;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.STORE_CODE;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.TOKEN;

@AutoService(Factory.class)
public final class AdobeCommerceSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AdobeCommerceSource.NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BASE_URL, TOKEN)
                .optional(
                        STORE_CODE,
                        RESOURCES,
                        START_TIME,
                        END_TIME,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_ATTEMPTS,
                        SOCKET_TIMEOUT_MS,
                        PAGE_DEADLINE_MS,
                        MAX_RESPONSE_BYTES)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new AdobeCommerceSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AdobeCommerceSource.class;
    }
}
