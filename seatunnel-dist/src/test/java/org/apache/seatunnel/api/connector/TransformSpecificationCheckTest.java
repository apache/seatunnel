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

import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.ServiceLoader;

@Slf4j
public class TransformSpecificationCheckTest {

    @Test
    public void testAllTransformUseFactory() {
        ServiceLoader<SeaTunnelTransform> transforms =
                ServiceLoader.load(
                        SeaTunnelTransform.class, Thread.currentThread().getContextClassLoader());
        Assertions.assertFalse(transforms.iterator().hasNext());
        List<TableTransformFactory> factories =
                FactoryUtil.discoverFactories(
                        Thread.currentThread().getContextClassLoader(),
                        TableTransformFactory.class);
        Assertions.assertEquals(16, factories.size());
    }

    @Test
    public void testAllTransformSupportMultiTable() {
        List<TableTransformFactory> factories =
                FactoryUtil.discoverFactories(
                        Thread.currentThread().getContextClassLoader(),
                        TableTransformFactory.class);
        factories.forEach(
                factory ->
                        factory.optionRule().getOptionalOptions().stream()
                                .filter(
                                        option ->
                                                option.key()
                                                        .equals(
                                                                TransformCommonOptions.MULTI_TABLES
                                                                        .key()))
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new RuntimeException(
                                                        TransformCommonOptions.MULTI_TABLES.key()
                                                                + " not found in "
                                                                + factory.factoryIdentifier())));
    }
}
