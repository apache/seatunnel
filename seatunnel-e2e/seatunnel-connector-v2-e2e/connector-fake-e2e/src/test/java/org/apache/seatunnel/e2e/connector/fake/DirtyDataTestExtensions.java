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

package org.apache.seatunnel.e2e.connector.fake;

import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Packages top-level test-only SPI fixtures (validator, collector provider/collector, and a
 * synchronous single-table test sink) into a temporary JAR for Flink and Spark E2E containers so
 * production {@code seatunnel-api} stays free of test extensions and engine containers never need
 * this test-harness helper class on their classpath.
 */
public final class DirtyDataTestExtensions {

    static final String CONTAINER_EXTENSION_JAR = "/tmp/dirty-data-e2e-ext.jar";
    private static final String FACTORY_SERVICE = "META-INF/services/" + Factory.class.getName();

    private DirtyDataTestExtensions() {}

    /**
     * Packages the test-only SPI classes and service descriptor into a temporary JAR and copies it
     * into the target engine container at {@link #CONTAINER_EXTENSION_JAR}.
     */
    static void installExtensionJar(TestContainer container) throws IOException {
        Path jarPath = Files.createTempFile("dirty-data-e2e-ext-", ".jar");
        try {
            List<Class<?>> classes =
                    Arrays.asList(
                            TestAlwaysDirtyDataValidator.class,
                            TestCountingDirtyRecordCollector.class,
                            TestCountingDirtyRecordCollectorProvider.class,
                            TestSingleTableSinkFactory.class,
                            TestSingleTableSink.class,
                            TestSingleTableSinkWriter.class);
            try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(jarPath))) {
                for (Class<?> clazz : classes) {
                    String resourceName = clazz.getName().replace('.', '/') + ".class";
                    jarOut.putNextEntry(new JarEntry(resourceName));
                    try (InputStream in =
                            clazz.getClassLoader().getResourceAsStream(resourceName)) {
                        if (in == null) {
                            throw new IOException("Could not load class bytes for " + resourceName);
                        }
                        byte[] buffer = new byte[4096];
                        int read;
                        while ((read = in.read(buffer)) != -1) {
                            jarOut.write(buffer, 0, read);
                        }
                    }
                    jarOut.closeEntry();
                }
                jarOut.putNextEntry(new JarEntry(FACTORY_SERVICE));
                String serviceContent =
                        TestAlwaysDirtyDataValidator.class.getName()
                                + "\n"
                                + TestCountingDirtyRecordCollectorProvider.class.getName()
                                + "\n"
                                + TestSingleTableSinkFactory.class.getName()
                                + "\n";
                jarOut.write(serviceContent.getBytes(StandardCharsets.UTF_8));
                jarOut.closeEntry();
            }
            container.copyAbsolutePathToContainer(
                    jarPath.toAbsolutePath().toString(), CONTAINER_EXTENSION_JAR);
        } finally {
            Files.deleteIfExists(jarPath);
        }
    }
}
