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

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConnectorSpecificationCheckTest {

    @Test
    public void testAllConnectorImplementFactoryWithUpToDateMethod() throws ClassNotFoundException {
        List<TableSourceFactory> sourceFactories =
                FactoryUtil.discoverFactories(
                        Thread.currentThread().getContextClassLoader(), TableSourceFactory.class);

        // Some class can not get method, because it without some necessary jar dependency, like
        // hive-exec.jar. We need to check manually.
        List<String> blockList = new ArrayList<>();
        blockList.add("HiveSourceFactory");

        for (TableSourceFactory factory : sourceFactories) {
            if (ReflectionUtils.getDeclaredMethod(
                                    factory.getClass(),
                                    "createSource",
                                    TableSourceFactoryContext.class)
                            .isPresent()
                    && !blockList.contains(factory.getClass().getSimpleName())) {
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
            }
        }

        List<TableSinkFactory> sinkFactories =
                FactoryUtil.discoverFactories(
                        Thread.currentThread().getContextClassLoader(), TableSinkFactory.class);
        for (TableSinkFactory factory : sinkFactories) {
            String factoryName = factory.getClass().getSimpleName();
            if (ReflectionUtils.getDeclaredMethod(
                                    factory.getClass(), "createSink", TableSinkFactoryContext.class)
                            .isPresent()
                    && !blockList.contains(factoryName)) {
                Class<? extends SeaTunnelSource> sinkClass =
                        (Class<? extends SeaTunnelSource>)
                                Class.forName(
                                        factory.getClass()
                                                .getName()
                                                .replace(
                                                        factoryName,
                                                        factoryName.replace("Factory", "")));
                Optional<Method> prepare = ReflectionUtils.getDeclaredMethod(sinkClass, "prepare");
                Optional<Method> setTypeInfo =
                        ReflectionUtils.getDeclaredMethod(sinkClass, "setTypeInfo");
                Assertions.assertFalse(
                        prepare.isPresent(),
                        "Please remove `prepare` method in " + sinkClass.getSimpleName());
                Assertions.assertFalse(
                        setTypeInfo.isPresent(),
                        "Please remove `setTypeInfo` method in " + sinkClass.getSimpleName());
            }
        }
    }
}
