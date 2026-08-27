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

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/** Verifies the deterministic adapter-selection contract used by CDC connector modules. */
class DebeziumAdapterFactoryTest {

    /**
     * Verifies that a single matching service registration resolves to the expected adapter
     * implementation.
     */
    @Test
    void getAdapter_returnsAdapter_whenConnectorClassMatches() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        DebeziumAdapter adapter =
                DebeziumAdapterFactory.getAdapter(TestDebeziumAdapter.TEST_CONNECTOR_CLASS, cl);

        Assertions.assertInstanceOf(TestDebeziumAdapter.class, adapter);
        Assertions.assertEquals(
                TestDebeziumAdapter.TEST_DEBEZIUM_VERSION, adapter.getDebeziumVersion());
    }

    @Test
    void getAdapter_throwsIllegalStateException_whenNoAdapterMatches() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                DebeziumAdapterFactory.getAdapter(
                                        "io.debezium.connector.unknown.UnknownConnector", cl));
        Assertions.assertTrue(exception.getMessage().contains("No DebeziumAdapter found"));
    }

    /**
     * Verifies that the factory fails fast when the same class loader exposes multiple matching
     * providers for one connector class.
     */
    @Test
    void getAdapter_throwsIllegalStateException_whenMultipleAdaptersMatch(@TempDir Path tempDir)
            throws Exception {
        Path serviceFile =
                tempDir.resolve("META-INF")
                        .resolve("services")
                        .resolve(DebeziumAdapter.class.getName());
        Files.createDirectories(serviceFile.getParent());
        Files.write(
                serviceFile,
                Collections.singletonList(SecondTestDebeziumAdapter.class.getName()),
                StandardCharsets.UTF_8);

        try (URLClassLoader classLoader =
                new URLClassLoader(
                        new URL[] {tempDir.toUri().toURL()},
                        Thread.currentThread().getContextClassLoader())) {
            IllegalStateException exception =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    DebeziumAdapterFactory.getAdapter(
                                            TestDebeziumAdapter.TEST_CONNECTOR_CLASS, classLoader));
            Assertions.assertTrue(
                    exception.getMessage().contains(TestDebeziumAdapter.class.getName()));
            Assertions.assertTrue(
                    exception.getMessage().contains(SecondTestDebeziumAdapter.class.getName()));
        }
    }

    @Test
    void getAdapter_throwsIllegalStateException_whenRegisteredAdaptersMatchSameConnector() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        IllegalStateException ex =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                DebeziumAdapterFactory.getAdapter(
                                        DuplicateTestDebeziumAdapter.DUPLICATE_CONNECTOR_CLASS,
                                        cl));
        Assertions.assertTrue(ex.getMessage().contains("Multiple DebeziumAdapters matched"));
        Assertions.assertTrue(ex.getMessage().contains(TestDebeziumAdapter.class.getName()));
        Assertions.assertTrue(
                ex.getMessage().contains(DuplicateTestDebeziumAdapter.class.getName()));
    }
}
