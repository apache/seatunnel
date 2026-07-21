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

package org.apache.seatunnel.edge.agent.connector;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryException;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.edge.agent.connector.config.EdgeInputOptions;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectOptions;
import org.apache.seatunnel.edge.agent.connector.file.FileCollectReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EdgeInputReaderFactorySpiTest {

    private static final ClassLoader CLASS_LOADER = Thread.currentThread().getContextClassLoader();

    @Test
    void discoversFileFactory() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeInputOptions.TYPE.key(), "file");
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList("/tmp/a.log"));

        EdgeInputReaderFactory factory =
                FactoryUtil.discoverFactory(CLASS_LOADER, EdgeInputReaderFactory.class, "file");
        Assertions.assertInstanceOf(
                FileCollectReader.class, factory.create(ReadonlyConfig.fromMap(map), null));
    }

    @Test
    void rejectsUnknownInputType() {
        FactoryException ex =
                Assertions.assertThrows(
                        FactoryException.class,
                        () ->
                                FactoryUtil.discoverFactory(
                                        CLASS_LOADER, EdgeInputReaderFactory.class, "unknown"));

        Assertions.assertTrue(
                ex.getMessage().contains("unknown")
                        || ex.getMessage().contains("Could not find any factory"));
    }
}
