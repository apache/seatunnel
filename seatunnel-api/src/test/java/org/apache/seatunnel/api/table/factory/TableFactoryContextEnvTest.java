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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TableFactoryContextEnvTest {

    private static final String ENV_KEY = "custom_parameters";

    private static ReadonlyConfig envOptions() {
        Map<String, Object> customParameters = new HashMap<>();
        customParameters.put("probe", "env-value");
        Map<String, Object> env = new HashMap<>();
        env.put(ENV_KEY, customParameters);
        return ReadonlyConfig.fromMap(env);
    }

    private static CatalogTable catalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        return CatalogTableUtil.getCatalogTable("env_probe_table", rowType);
    }

    private static void assertEnv(ReadonlyConfig actual) {
        Assertions.assertNotNull(actual);
        Assertions.assertEquals("env-value", envValue(actual));
    }

    private static final Option<String> TABLE_OPTION =
            Options.key("table").stringType().noDefaultValue();

    private static final Option<Map<String, String>> CUSTOM_PARAMETERS_OPTION =
            Options.key(ENV_KEY).mapType().noDefaultValue();

    private static Object envValue(ReadonlyConfig config) {
        Map<String, String> customParameters =
                config.getOptional(CUSTOM_PARAMETERS_OPTION).orElse(Collections.emptyMap());
        return customParameters.get("probe");
    }

    @Test
    public void testSourceContextCarriesEnv() {
        ReadonlyConfig env = envOptions();
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        getClass().getClassLoader(),
                        null,
                        env);
        Assertions.assertSame(env, context.getEnvOptions());
        assertEnv(context.getEnvOptions());
    }

    @Test
    public void testLegacyConstructorsUseEmptyEnv() {
        ReadonlyConfig options = ReadonlyConfig.fromMap(Collections.emptyMap());
        ClassLoader classLoader = getClass().getClassLoader();

        TableSourceFactoryContext sourceContext =
                new TableSourceFactoryContext(options, classLoader);
        TableTransformFactoryContext transformContext =
                new TableTransformFactoryContext(
                        Collections.singletonList(catalogTable()), options, classLoader);
        TableSinkFactoryContext sinkContext =
                new TableSinkFactoryContext(catalogTable(), options, classLoader);
        MultiTableFactoryContext multiTableContext =
                new MultiTableFactoryContext(
                        options, classLoader, Collections.<TablePath, SeaTunnelSink>emptyMap());

        Assertions.assertNotNull(sourceContext.getEnvOptions());
        Assertions.assertNotNull(transformContext.getEnvOptions());
        Assertions.assertNotNull(sinkContext.getEnvOptions());
        Assertions.assertNotNull(multiTableContext.getEnvOptions());
        Assertions.assertTrue(sourceContext.getEnvOptions().getSourceMap().isEmpty());
        Assertions.assertTrue(transformContext.getEnvOptions().getSourceMap().isEmpty());
        Assertions.assertTrue(sinkContext.getEnvOptions().getSourceMap().isEmpty());
        Assertions.assertTrue(multiTableContext.getEnvOptions().getSourceMap().isEmpty());
    }

    @Test
    public void testTransformContextCarriesEnv() {
        ReadonlyConfig env = envOptions();
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.singletonList(catalogTable()),
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        getClass().getClassLoader(),
                        env);
        Assertions.assertSame(env, context.getEnvOptions());
    }

    @Test
    public void testMultiTableContextCarriesEnv() {
        ReadonlyConfig env = envOptions();
        MultiTableFactoryContext context =
                new MultiTableFactoryContext(
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        getClass().getClassLoader(),
                        Collections.<TablePath, SeaTunnelSink>emptyMap(),
                        env);
        Assertions.assertSame(env, context.getEnvOptions());
        Assertions.assertNull(context.getCatalogTable());
        Assertions.assertTrue(context.getSinks().isEmpty());
    }

    @Test
    public void testSinkPlaceholderRewriteKeepsEnvSeparate() {
        Map<String, Object> sinkOptions = new HashMap<>();
        sinkOptions.put("table", "sink_${table_name}");
        ReadonlyConfig env = envOptions();

        TableSinkFactoryContext context =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        catalogTable(),
                        ReadonlyConfig.fromMap(sinkOptions),
                        getClass().getClassLoader(),
                        Collections.emptyList(),
                        env);

        Assertions.assertEquals("sink_env_probe_table", context.getOptions().get(TABLE_OPTION));
        Assertions.assertSame(env, context.getEnvOptions());
        assertEnv(context.getEnvOptions());
    }

    @Test
    public void testNullEnvFallsBackToEmpty() {
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        getClass().getClassLoader(),
                        null,
                        null);
        Assertions.assertNotNull(context.getEnvOptions());
        Assertions.assertTrue(context.getEnvOptions().getSourceMap().isEmpty());
    }
}
