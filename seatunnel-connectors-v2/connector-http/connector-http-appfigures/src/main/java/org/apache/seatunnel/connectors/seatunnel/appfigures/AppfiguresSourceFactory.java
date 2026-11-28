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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.GROUP_BY;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.MAX_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.REPORT_END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.REPORT_START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.SEARCH_STORE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.TRUST_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.USER_END_TIME;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.USER_START_TIME;

/**
 * Registers the Appfigures source with the SeaTunnel Factory SPI.
 *
 * <p>Resource-specific options are validated before opening network connections.
 */
@AutoService(Factory.class)
public final class AppfiguresSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AppfiguresSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(RESOURCE, TOKEN)
                .optional(
                        SEARCH_STORE,
                        GROUP_BY,
                        REPORT_START_DATE,
                        REPORT_END_DATE,
                        USER_START_TIME,
                        USER_END_TIME,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_ROWS,
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
                        new AppfiguresSource(new AppfiguresSettings(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AppfiguresSource.class;
    }
}
