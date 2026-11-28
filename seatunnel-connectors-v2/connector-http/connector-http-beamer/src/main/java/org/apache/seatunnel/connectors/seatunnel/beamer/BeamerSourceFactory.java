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
package org.apache.seatunnel.connectors.seatunnel.beamer;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;

/**
 * Registers the native Beamer NPS source.
 *
 * <p>Schema discovery and creation validate configuration locally and consume no API quota.
 */
@AutoService(Factory.class)
public final class BeamerSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return BeamerSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return BeamerConfig.rules();
    }

    /**
     * Builds an ordered scan with validated, checkpoint-compatible settings.
     *
     * <p>A provider connection is opened later by the worker responsible for the split.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        BeamerConfig options = new BeamerConfig(context.getOptions());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new BeamerSource(options);
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(TableSourceFactoryContext context) {
        return new BeamerSource(new BeamerConfig(context.getOptions())).getProducedCatalogTables();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return BeamerSource.class;
    }
}
