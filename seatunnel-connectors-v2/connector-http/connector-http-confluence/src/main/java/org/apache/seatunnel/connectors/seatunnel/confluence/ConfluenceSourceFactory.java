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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.BODY_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.MAX_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.PAGE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.SITE_URL;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.USERNAME;

@AutoService(Factory.class)
public final class ConfluenceSourceFactory implements TableSourceFactory {
    static final String IDENTIFIER = "Confluence";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SITE_URL, USERNAME, TOKEN, RESOURCE)
                .optional(
                        BODY_FORMAT,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_ATTEMPTS,
                        CONNECT_TIMEOUT,
                        READ_TIMEOUT,
                        PAGE_TIMEOUT,
                        MAX_BYTES)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new ConfluenceSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ConfluenceSource.class;
    }
}
