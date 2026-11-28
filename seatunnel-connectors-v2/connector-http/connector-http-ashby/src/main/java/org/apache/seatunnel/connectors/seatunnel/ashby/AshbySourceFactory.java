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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.APPLICATION_CHILD_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.MAX_REQUESTS_PER_PASS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.POLL_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.SYNC_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.TRUST_CERTIFICATE;

/**
 * Registers the Ashby source with the Factory SPI.
 *
 * <p>Configuration is verified for the selected resource before any worker connection is opened.
 */
@AutoService(Factory.class)
public final class AshbySourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AshbySource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(RESOURCE, TOKEN)
                .optional(
                        START_DATE,
                        SYNC_TOKEN,
                        POLL_INTERVAL_MS,
                        PAGE_SIZE,
                        MAX_REQUESTS_PER_PASS,
                        APPLICATION_CHILD_INTERVAL_MS,
                        MAX_RESPONSE_BYTES,
                        MAX_ATTEMPTS,
                        REQUEST_TIMEOUT_MS,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new AshbySource(new AshbySettings(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AshbySource.class;
    }
}
