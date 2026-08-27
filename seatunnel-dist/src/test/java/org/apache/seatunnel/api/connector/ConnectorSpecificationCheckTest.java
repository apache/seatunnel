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

package org.apache.seatunnel.api.connector;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

@Slf4j
public class ConnectorSpecificationCheckTest {

    @Test
    public void testAllConnectorImplementFactoryWithUpToDateMethod() throws ClassNotFoundException {

        ServiceLoader<SeaTunnelSource> sources =
                ServiceLoader.load(
                        SeaTunnelSource.class, Thread.currentThread().getContextClassLoader());
        Map<String, String> sourceWithSPI = new HashMap<>();
        Iterator<SeaTunnelSource> sourceIterator = sources.iterator();
        while (sourceIterator.hasNext()) {
            SeaTunnelSource source = sourceIterator.next();
            sourceWithSPI.put(source.getPluginName(), source.getClass().getName());
        }
        List<TableSourceFactory> sourceFactories =
                FactoryUtil.discoverFactories(
                        Thread.currentThread().getContextClassLoader(), TableSourceFactory.class);

        // Some class can not get method, because it without some necessary jar dependency, like
        // hive-exec.jar. We need to check manually.
        List<String> blockList = new ArrayList<>();
        blockList.add("HiveSourceFactory");
        blockList.add("HiveSinkFactory");

        for (TableSourceFactory factory : sourceFactories) {
            if (ReflectionUtils.getDeclaredMethod(
                                    factory.getClass(),
                                    "createSource",
                                    TableSourceFactoryContext.class)
                            .isPresent()
                    && !blockList.contains(factory.getClass().getSimpleName())) {
                Assertions.assertFalse(
                        sourceWithSPI.containsKey(factory.factoryIdentifier()),
                        "Please remove `@AutoService(SeaTunnelSource.class)` annotation in "
                                + sourceWithSPI.get(factory.factoryIdentifier()));
                Class<? extends SeaTunnelSource> sourceClass = factory.getSourceClass();
                Optional<Method> prepare =
                        ReflectionUtils.getDeclaredMethod(sourceClass, "prepare");
                Optional<Method> getProducedType =
                        ReflectionUtils.getDeclaredMethod(sourceClass, "getProducedType");
                Optional<Method> getProducedCatalogTables =
                        ReflectionUtils.getDeclaredMethod(sourceClass, "getProducedCatalogTables");
                Assertions.assertFalse(
                        prepare.isPresent(),
                        "Please remove `prepare` method, it will not be used any more");
                Assertions.assertFalse(
                        getProducedType.isPresent(),
                        "Please use `getProducedCatalogTables` method, do not implement `getProducedType` method in "
                                + sourceClass.getSimpleName());
                Assertions.assertTrue(
                        getProducedCatalogTables.isPresent(),
                        "Please implement `getProducedCatalogTables` method in "
                                + sourceClass.getSimpleName());
                log.info(
                        "Check source connector {} successfully",
                        factory.getClass().getSimpleName());
            }
        }

        List<TableSinkFactory> sinkFactories =
                FactoryUtil.discoverFactories(
                        Thread.currentThread().getContextClassLoader(), TableSinkFactory.class);
        ServiceLoader<SeaTunnelSink> sinks =
                ServiceLoader.load(
                        SeaTunnelSink.class, Thread.currentThread().getContextClassLoader());
        Map<String, String> sinkWithSPI = new HashMap<>();
        Iterator<SeaTunnelSink> sinkIterator = sinks.iterator();
        while (sinkIterator.hasNext()) {
            SeaTunnelSink sink = sinkIterator.next();
            sinkWithSPI.put(sink.getPluginName(), sink.getClass().getName());
        }
        for (TableSinkFactory factory : sinkFactories) {
            String factoryName = factory.getClass().getSimpleName();
            if (ReflectionUtils.getDeclaredMethod(
                                    factory.getClass(), "createSink", TableSinkFactoryContext.class)
                            .isPresent()
                    && !blockList.contains(factoryName)) {
                Assertions.assertFalse(
                        sinkWithSPI.containsKey(factory.factoryIdentifier()),
                        "Please remove `@AutoService(SeaTunnelSink.class)` annotation in "
                                + sinkWithSPI.get(factory.factoryIdentifier()));
                Class<? extends SeaTunnelSink> sinkClass =
                        (Class<? extends SeaTunnelSink>)
                                Class.forName(
                                        factory.getClass()
                                                .getName()
                                                .replace(
                                                        factoryName,
                                                        factoryName.replace("Factory", "")));
                Optional<Method> prepare = ReflectionUtils.getDeclaredMethod(sinkClass, "prepare");
                Optional<Method> setTypeInfo =
                        ReflectionUtils.getDeclaredMethod(
                                sinkClass, "setTypeInfo", SeaTunnelRowType.class);
                Optional<Method> getConsumedType =
                        ReflectionUtils.getDeclaredMethod(sinkClass, "getConsumedType");
                Optional<Method> getWriteCatalogTable =
                        ReflectionUtils.getDeclaredMethod(sinkClass, "getWriteCatalogTable");
                Assertions.assertFalse(
                        prepare.isPresent(),
                        "Please remove `prepare` method in " + sinkClass.getSimpleName());
                Assertions.assertFalse(
                        setTypeInfo.isPresent(),
                        "Please remove `setTypeInfo` method in " + sinkClass.getSimpleName());
                Assertions.assertFalse(
                        getConsumedType.isPresent(),
                        "Please remove `getConsumedType` method in " + sinkClass.getSimpleName());
                Assertions.assertTrue(
                        getWriteCatalogTable.isPresent(),
                        "Please implement `getWriteCatalogTable` method in "
                                + sinkClass.getSimpleName());
                Assertions.assertEquals(
                        Optional.class,
                        getWriteCatalogTable.get().getReturnType(),
                        "The `getWriteCatalogTable` method should return Optional<CatalogTable> in "
                                + sinkClass.getSimpleName());

                log.info(
                        "Check sink connector {} successfully", factory.getClass().getSimpleName());

                checkSupportMultiTableSink(factory, sinkClass);
                checkSupportSchemaEvolutionSink(sinkClass);
            }
        }
    }

    private void checkSupportMultiTableSink(
            TableSinkFactory sinkFactory, Class<? extends SeaTunnelSink> sinkClass) {
        if (!SupportMultiTableSink.class.isAssignableFrom(sinkClass)) {
            return;
        }

        OptionRule sinkOptionRule = sinkFactory.optionRule();
        Assertions.assertTrue(
                sinkOptionRule
                        .getOptionalOptions()
                        .contains(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA),
                "Please add `SinkCommonOptions.MULTI_TABLE_SINK_REPLICA` optional into the `optionRule` method optional of `"
                        + sinkFactory.getClass().getSimpleName()
                        + "`");

        // Validate the `createWriter` method return type
        Optional<Method> createWriter =
                ReflectionUtils.getDeclaredMethod(
                        sinkClass, "createWriter", SinkWriter.Context.class);
        Assertions.assertTrue(
                createWriter.isPresent(),
                "Please add `createWriter` method in " + sinkClass.getSimpleName());
        Class<? extends SinkWriter> createWriterClass =
                (Class<? extends SinkWriter>) createWriter.get().getReturnType();
        Assertions.assertTrue(
                SupportMultiTableSinkWriter.class.isAssignableFrom(createWriterClass),
                String.format(
                        "Please update the `createWriter` method return type to the subclass of `SupportMultiTableSinkWriter`, "
                                + "because `%s` implements `SupportMultiTableSink` interface",
                        sinkClass.getSimpleName()));
    }

    private void checkSupportSchemaEvolutionSink(Class<? extends SeaTunnelSink> sinkClass) {
        if (!SupportSchemaEvolutionSink.class.isAssignableFrom(sinkClass)) {
            return;
        }
        if (MultiTableSink.class.equals(sinkClass)) {
            return;
        }

        // Validate the `createWriter` method return type
        Optional<Method> createWriter =
                ReflectionUtils.getDeclaredMethod(
                        sinkClass, "createWriter", SinkWriter.Context.class);
        Assertions.assertTrue(
                createWriter.isPresent(),
                "Please add `createWriter` method in " + sinkClass.getSimpleName());
        Class<? extends SinkWriter> createWriterClass =
                (Class<? extends SinkWriter>) createWriter.get().getReturnType();
        Assertions.assertTrue(
                SupportSchemaEvolutionSinkWriter.class.isAssignableFrom(createWriterClass),
                String.format(
                        "Please update the `createWriter` method return type to the subclass of `SupportSchemaEvolutionSinkWriter`, "
                                + "because `%s` implements `SupportSchemaEvolutionSink` interface",
                        sinkClass.getSimpleName()));
    }
}
