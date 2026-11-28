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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

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
 * Registers the Awin advertiser source and validates its configuration.
 *
 * <p>Schema discovery has no effect on the account quota or provider data.
 */
@AutoService(Factory.class)
public final class AwinSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AwinSource.NAME;
    }

    @Override
    public OptionRule optionRule() {
        return AwinSettings.rules();
    }

    /**
     * Creates a read plan before any network activity.
     *
     * <p>The provider token is only used later by the assigned report reader.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AwinSettings settings = new AwinSettings(context.getOptions());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new AwinSource(settings);
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(TableSourceFactoryContext context) {
        return new AwinSource(new AwinSettings(context.getOptions())).getProducedCatalogTables();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AwinSource.class;
    }
}
