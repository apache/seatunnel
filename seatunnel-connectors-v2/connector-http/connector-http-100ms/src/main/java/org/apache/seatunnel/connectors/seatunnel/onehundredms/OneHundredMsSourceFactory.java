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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.API_URL;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_PARENTS;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_RECORDS;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.SECRET_KEY;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.TOKEN;

/**
 * Registers the bounded 100ms source and its catalog through the standard factory SPI. Dedicated
 * configuration validation resolves authentication and resource choices before creating readers.
 */
@AutoService(Factory.class)
public final class OneHundredMsSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return OneHundredMsSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        API_URL,
                        TOKEN,
                        ACCESS_KEY,
                        SECRET_KEY,
                        RESOURCES,
                        START_DATE,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_PARENTS,
                        MAX_RESPONSE_BYTES,
                        MAX_RECORDS,
                        CONNECT_TIMEOUT,
                        READ_TIMEOUT,
                        REQUEST_TIMEOUT,
                        MAX_ATTEMPTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        OneHundredMsSource source =
                new OneHundredMsSource(new OneHundredMsConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OneHundredMsSource.class;
    }
}
