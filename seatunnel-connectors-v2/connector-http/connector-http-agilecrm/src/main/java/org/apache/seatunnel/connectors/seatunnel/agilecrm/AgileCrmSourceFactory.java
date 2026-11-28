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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.EVENT_END;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.EVENT_START;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.USERNAME;

/** Exposes the AgileCRM source and its options through the factory SPI. */
@AutoService(Factory.class)
public final class AgileCrmSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AgileCrmSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BASE_URL, USERNAME, PASSWORD, RESOURCE)
                .optional(PAGE_SIZE, ALLOW_INSECURE_HTTP, REQUEST_TIMEOUT_MS, MAX_ATTEMPTS)
                .conditional(RESOURCE, "events", EVENT_START, EVENT_END)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AgileCrmSource source = new AgileCrmSource(new AgileCrmSettings(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AgileCrmSource.class;
    }
}
