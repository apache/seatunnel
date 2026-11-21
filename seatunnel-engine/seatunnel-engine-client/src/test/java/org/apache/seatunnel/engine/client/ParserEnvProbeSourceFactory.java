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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.mockito.Mockito;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Collections;

/** Test-only source factory that captures the env options from the factory context. */
@AutoService(Factory.class)
public class ParserEnvProbeSourceFactory implements TableSourceFactory {

    public static final String IDENTIFIER = "ParserEnvProbeSource";

    private static volatile ReadonlyConfig capturedEnvOptions;

    public static void reset() {
        capturedEnvOptions = null;
    }

    public static ReadonlyConfig capturedEnvOptions() {
        return capturedEnvOptions;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        if (context != null) {
            capturedEnvOptions = context.getEnvOptions();
        }
        TableSource tableSource = Mockito.mock(TableSource.class);
        SeaTunnelSource source = Mockito.mock(SeaTunnelSource.class);
        Mockito.when(tableSource.createSource()).thenReturn(source);
        Mockito.when(source.getProducedCatalogTables())
                .thenReturn(Collections.singletonList(catalogTable()));
        Mockito.when(source.getPluginName()).thenReturn(IDENTIFIER);
        Mockito.when(source.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        return tableSource;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SeaTunnelSource.class;
    }

    private static CatalogTable catalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        return CatalogTableUtil.getCatalogTable("fake", rowType);
    }
}
