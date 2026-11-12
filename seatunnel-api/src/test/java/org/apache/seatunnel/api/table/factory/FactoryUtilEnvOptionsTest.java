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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;

public class FactoryUtilEnvOptionsTest {

    private static final Option<String> PROBE_OPTION =
            Options.key("probe").stringType().noDefaultValue();

    @Test
    public void testCreateAndPrepareSourceCarriesEnv() {
        EnvProbeSourceFactory.reset();
        EnvProbeSourceFactory factory = new EnvProbeSourceFactory();

        TableSource tableSource = Mockito.mock(TableSource.class);
        SeaTunnelSource source = Mockito.mock(SeaTunnelSource.class);
        Mockito.when(tableSource.createSource()).thenReturn(source);
        Mockito.when(source.getProducedCatalogTables()).thenReturn(Collections.emptyList());
        Mockito.when(source.getPluginName()).thenReturn(EnvProbeSourceFactory.IDENTIFIER);
        EnvProbeSourceFactory.setTableSource(tableSource);

        Map<String, Object> options = new HashMap<>();
        options.put(PLUGIN_NAME.key(), EnvProbeSourceFactory.IDENTIFIER);

        FactoryUtil.createAndPrepareSource(
                ReadonlyConfig.fromMap(options),
                getClass().getClassLoader(),
                EnvProbeSourceFactory.IDENTIFIER,
                pluginIdentifier -> null,
                factory,
                null,
                envOptions());

        Assertions.assertNotNull(EnvProbeSourceFactory.capturedEnvOptions());
        Assertions.assertEquals(
                "env-value", EnvProbeSourceFactory.capturedEnvOptions().get(PROBE_OPTION));
    }

    @Test
    public void testCreateAndPrepareSinkCarriesEnv() {
        EnvProbeSinkFactory.reset();
        EnvProbeSinkFactory factory = new EnvProbeSinkFactory();

        TableSink tableSink = Mockito.mock(TableSink.class);
        SeaTunnelSink sink = Mockito.mock(SeaTunnelSink.class);
        Mockito.when(tableSink.createSink()).thenReturn(sink);
        EnvProbeSinkFactory.setTableSink(tableSink);

        Map<String, Object> options = new HashMap<>();
        options.put(PLUGIN_NAME.key(), EnvProbeSinkFactory.IDENTIFIER);

        SeaTunnelSink<?, ?, ?, ?> result =
                FactoryUtil.createAndPrepareSink(
                        catalogTable(),
                        ReadonlyConfig.fromMap(options),
                        getClass().getClassLoader(),
                        EnvProbeSinkFactory.IDENTIFIER,
                        pluginIdentifier -> null,
                        factory,
                        envOptions());

        Assertions.assertSame(sink, result);
        Assertions.assertNotNull(EnvProbeSinkFactory.capturedEnvOptions());
        Assertions.assertEquals(
                "env-value", EnvProbeSinkFactory.capturedEnvOptions().get(PROBE_OPTION));
    }

    private static CatalogTable catalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        return CatalogTableUtil.getCatalogTable("env_probe_table", rowType);
    }

    private static ReadonlyConfig envOptions() {
        Map<String, Object> env = new HashMap<>();
        // A plain top-level env variable; custom_parameters is not required.
        env.put("probe", "env-value");
        env.put("custom_parameters", Collections.singletonMap("nested", "nested-value"));
        return ReadonlyConfig.fromMap(env);
    }
}
