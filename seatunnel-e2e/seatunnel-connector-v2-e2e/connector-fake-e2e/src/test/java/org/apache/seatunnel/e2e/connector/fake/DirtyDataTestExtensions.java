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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.DirtyDataValidator;
import org.apache.seatunnel.api.sink.DirtyRecordCollector;
import org.apache.seatunnel.api.sink.DirtyRecordCollectorProvider;
import org.apache.seatunnel.api.sink.LogDirtyRecordCollector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Test-only SPI fixtures packaged into a temporary JAR for Flink and Spark E2E containers so
 * production {@code seatunnel-api} stays free of test validators and demonstration collectors.
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
                            TestCountingDirtyRecordCollectorProvider.class);
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

    /** Test-only validator that marks every record dirty to exercise pre-write collection. */
    public static class TestAlwaysDirtyDataValidator implements DirtyDataValidator {

        private static final long serialVersionUID = 1L;

        @Override
        public String factoryIdentifier() {
            return "test-always-dirty";
        }

        @Override
        public ValidationResult validate(SeaTunnelRow record, CatalogTable catalogTable) {
            return ValidationResult.dirty(
                    "[AlwaysDirtyValidator] Record marked dirty by custom validator rule");
        }
    }

    /** Test-only SPI provider for {@link TestCountingDirtyRecordCollector}. */
    public static class TestCountingDirtyRecordCollectorProvider
            implements DirtyRecordCollectorProvider {

        @Override
        public String getType() {
            return "test-counting";
        }

        @Override
        public DirtyRecordCollector createCollector() {
            return new TestCountingDirtyRecordCollector();
        }
    }

    /** Test-only collector that counts dirty rows and logs one summary on close. */
    @Slf4j
    public static class TestCountingDirtyRecordCollector implements DirtyRecordCollector {

        private static final long serialVersionUID = 1L;

        private final AtomicLong dirtyRecordCount = new AtomicLong();
        private final AtomicBoolean closeSummaryLogged = new AtomicBoolean();
        private long threshold = -1;
        private boolean failOnThreshold = false;

        @Override
        public void init(Config config) {
            if (config.hasPath(LogDirtyRecordCollector.THRESHOLD.key())) {
                this.threshold = config.getLong(LogDirtyRecordCollector.THRESHOLD.key());
            }
            if (config.hasPath(LogDirtyRecordCollector.FAIL_ON_THRESHOLD.key())) {
                this.failOnThreshold =
                        config.getBoolean(LogDirtyRecordCollector.FAIL_ON_THRESHOLD.key());
            }
        }

        @Override
        public void collect(
                int subTaskIndex,
                Object dirtyRecord,
                Throwable exception,
                String errorMessage,
                CatalogTable catalogTable) {
            long count = dirtyRecordCount.incrementAndGet();
            if (threshold > 0 && count >= threshold && failOnThreshold) {
                throw new RuntimeException(
                        String.format(
                                "[CountingCollector] threshold exceeded: %d >= %d",
                                count, threshold));
            }
        }

        @Override
        public long getDirtyRecordCount() {
            return dirtyRecordCount.get();
        }

        @Override
        public void close() {
            if (closeSummaryLogged.compareAndSet(false, true)) {
                log.info("[CountingCollector] total dirty records={}", getDirtyRecordCount());
            }
        }
    }
}
