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

package org.apache.seatunnel.connectors.seatunnel.bunnyinc;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.SUBDOMAIN;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.TLS_TRUST_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.TOKEN;

/**
 * Registers the bounded BunnyInc V2 table source.
 *
 * <p>The option rule and distribution mapping use the same public source identity.
 */
@AutoService(Factory.class)
public final class BunnyIncSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return BunnyIncSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SUBDOMAIN, TOKEN, RESOURCE)
                .optional(
                        PAGE_SIZE,
                        MAX_PAGES,
                        CONNECT_TIMEOUT_MS,
                        READ_TIMEOUT_MS,
                        REQUEST_DEADLINE_MS,
                        MAX_ATTEMPTS,
                        MAX_RESPONSE_BYTES,
                        TLS_TRUST_CERTIFICATE)
                .build();
    }

    /**
     * Validates the selected tenant, connection and bounds before any network work can start.
     *
     * @param context the engine's connector factory context
     * @return the selected bounded resource
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        BunnyIncConfig config = new BunnyIncConfig(context.getOptions());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new BunnyIncSource(config);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return BunnyIncSource.class;
    }
}
