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

package org.apache.seatunnel.connectors.seatunnel.aha;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.API_URL;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.MAX_PARENTS;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.TOKEN;

/** Exposes Aha source options and installs its factory identity through SeaTunnel SPI. */
@AutoService(Factory.class)
public final class AhaSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AhaSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(API_URL, TOKEN)
                .optional(
                        RESOURCES,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_PARENTS,
                        MAX_RESPONSE_BYTES,
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
        AhaSource source = new AhaSource(new AhaConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AhaSource.class;
    }
}
