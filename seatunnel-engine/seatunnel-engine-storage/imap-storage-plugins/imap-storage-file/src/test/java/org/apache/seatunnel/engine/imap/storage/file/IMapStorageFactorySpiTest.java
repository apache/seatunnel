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

package org.apache.seatunnel.engine.imap.storage.file;

import org.apache.seatunnel.engine.imap.storage.api.IMapStorageFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public class IMapStorageFactorySpiTest {

    @Test
    public void testHdfsFactoryIsDiscoverableViaSpi() {
        List<IMapStorageFactory> factories = new ArrayList<>();
        ServiceLoader.load(IMapStorageFactory.class, Thread.currentThread().getContextClassLoader())
                .forEach(factories::add);

        Assertions.assertFalse(
                factories.isEmpty(),
                "ServiceLoader found no IMapStorageFactory implementations. "
                        + "This likely means META-INF/services was lost during shade packaging. "
                        + "Ensure ServicesResourceTransformer is configured in seatunnel-starter/pom.xml.");

        List<String> identifiers =
                factories.stream()
                        .map(IMapStorageFactory::factoryIdentifier)
                        .collect(Collectors.toList());

        Assertions.assertTrue(
                identifiers.contains("hdfs"),
                "No IMapStorageFactory with identifier 'hdfs' found. Available: " + identifiers);
    }

    @Test
    public void testNoAmbiguousFactoryIdentifiers() {
        List<String> identifiers = new ArrayList<>();
        ServiceLoader.load(IMapStorageFactory.class, Thread.currentThread().getContextClassLoader())
                .forEach(f -> identifiers.add(f.factoryIdentifier()));

        long distinctCount = identifiers.stream().distinct().count();
        Assertions.assertEquals(
                identifiers.size(),
                distinctCount,
                "Duplicate IMapStorageFactory identifiers detected after SPI merge: "
                        + identifiers
                        + ". ServicesResourceTransformer may have merged conflicting registrations.");
    }
}
