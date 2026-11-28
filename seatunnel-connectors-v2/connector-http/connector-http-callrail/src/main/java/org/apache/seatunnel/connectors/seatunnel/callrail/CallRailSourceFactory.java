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
package org.apache.seatunnel.connectors.seatunnel.callrail;

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
 * Resolves the CallRail identifier through the built-in table source factory SPI.
 *
 * <p>The table schema is discovered from local configuration alone.
 */
@AutoService(Factory.class)
public final class CallRailSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return CallRailSource.NAME;
    }

    @Override
    public OptionRule optionRule() {
        return CallRailConfig.rules();
    }

    /**
     * Validates source configuration before constructing a bounded traversal.
     *
     * <p>Factory discovery does not perform a provider call.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T, S extends SourceSplit, C extends Serializable> TableSource<T, S, C> createSource(
            TableSourceFactoryContext context) {
        CallRailConfig config = new CallRailConfig(context.getOptions());
        return () -> (SeaTunnelSource<T, S, C>) new CallRailSource(config);
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(TableSourceFactoryContext context) {
        return new CallRailSource(new CallRailConfig(context.getOptions()))
                .getProducedCatalogTables();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return CallRailSource.class;
    }
}
