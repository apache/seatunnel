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

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorageFactory;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import com.hazelcast.core.HazelcastInstance;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for FileMapStore.
 *
 * <p>Covers two scenarios introduced/improved by the fix for
 * https://github.com/apache/seatunnel/issues/10883:
 *
 * <ol>
 *   <li>init() with a valid local-fs config succeeds end-to-end (SPI discovery + storage init).
 *   <li>init() with an unknown storage type throws immediately instead of silently failing.
 * </ol>
 *
 * <p>Note: HDFS/S3/OSS backends are NOT tested here because they require Hadoop uber jars and
 * remote infrastructure. Those are covered by IMapFileStorageTest. What we verify here is that the
 * SPI discovery path (FactoryUtil.discoverFactory) is wired correctly through FileMapStore, and
 * that failures surface as exceptions rather than silent no-ops.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
public class FileMapStoreTest {

    private static final String TEST_STORAGE_TYPE = "isolated-test-storage";
    private static final String FACTORY_CLASS_NAME = "isolated.IsolatedIMapStorageFactory";

    @TempDir Path tempDir;

    /**
     * Verifies that FileMapStore.init() can successfully resolve the "hdfs" factory via SPI and
     * initialize local-filesystem storage without errors.
     *
     * <p>This test fails if ServicesResourceTransformer is absent from the shade config, because
     * ServiceLoader would return no IMapStorageFactory implementations.
     */
    @Test
    public void testInitSucceedsWithLocalFileSystem() {
        FileMapStore store = new FileMapStore();
        Properties props = buildLocalFsProperties(tempDir.toString());

        Assertions.assertDoesNotThrow(
                () -> store.init(mock(HazelcastInstance.class), props, "test-map"),
                "FileMapStore.init() should succeed with local-fs config. "
                        + "If this fails with 'Could not find any factories', "
                        + "ServicesResourceTransformer is likely missing from seatunnel-starter shade config.");

        store.destroy();
    }

    /**
     * Verifies that FileMapStore.init() throws (rather than silently swallowing) when the storage
     * type is unknown.
     *
     * <p>Before the fix, Hazelcast's MapLoader lifecycle could absorb the exception and leave
     * mapStorage null, causing NullPointerException on the first store/load call with no indication
     * of the real root cause.
     */
    @Test
    public void testInitThrowsOnUnknownStorageType() {
        FileMapStore store = new FileMapStore();
        Properties props = new Properties();
        props.setProperty("type", "non-existent-storage-type");

        RuntimeException ex =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> store.init(mock(HazelcastInstance.class), props, "test-map"),
                        "FileMapStore.init() must throw on unknown storage type instead of silently failing.");

        String msg = ex.getMessage() != null ? ex.getMessage() : "";
        Assertions.assertTrue(
                msg.contains("non-existent-storage-type")
                        || msg.contains("Could not find any factories"),
                "Exception message should identify the unknown type. Got: " + msg);
    }

    @Test
    public void testMetricsSnapshotMapSkipsPersistentStorageInitialization() {
        FileMapStore store = new FileMapStore();
        Properties props = new Properties();
        props.setProperty("type", "non-existent-storage-type");

        Assertions.assertDoesNotThrow(
                () ->
                        store.init(
                                mock(HazelcastInstance.class),
                                props,
                                EngineStateStoreNames.RUNNING_JOB_METRICS));
        Assertions.assertEquals(Collections.emptySet(), store.loadAllKeys());
        Assertions.assertDoesNotThrow(() -> store.store("k", "v"));
        Assertions.assertDoesNotThrow(() -> store.delete("k"));

        store.destroy();
    }

    /** Proves that FileMapStore reads SPI resources from the split starter layout. */
    @Test
    public void testLoadsMapStorageFactoryFromZetaStarterJar() throws IOException {
        Path zetaDirectory = tempDir.resolve("zeta");
        createServiceJar(
                zetaDirectory.resolve("s3/imap-test.jar"),
                IMapStorageFactory.class,
                FACTORY_CLASS_NAME,
                imapFactorySource());
        Properties props = new Properties();
        props.setProperty("type", TEST_STORAGE_TYPE);
        props.setProperty("storage.type", "s3");

        FileMapStore store = new FileMapStore();
        Assertions.assertDoesNotThrow(
                () -> store.init(mock(HazelcastInstance.class), props, "test-map", zetaDirectory));
        Assertions.assertDoesNotThrow(
                () -> store.storeAll(Collections.singletonMap("key", "value")));
        Assertions.assertEquals(Collections.emptySet(), store.loadAllKeys());

        store.destroy();
    }

    private static void createServiceJar(
            Path jarPath, Class<?> serviceClass, String implementationClass, String source)
            throws IOException {
        Path compilationRoot = jarPath.getParent().resolve("compiled-provider");
        Path sourcePath =
                compilationRoot.resolve("src/" + implementationClass.replace('.', '/') + ".java");
        Path classesPath = compilationRoot.resolve("classes");
        Files.createDirectories(sourcePath.getParent());
        Files.createDirectories(classesPath);
        Files.write(sourcePath, source.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assertions.assertNotNull(compiler, "A JDK compiler is required for this classloader test");
        String classPath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        int compilationResult =
                compiler.run(
                        null,
                        null,
                        null,
                        "-proc:none",
                        "-classpath",
                        classPath,
                        "-d",
                        classesPath.toString(),
                        sourcePath.toString());
        Assertions.assertEquals(0, compilationResult, "The isolated test provider must compile");

        String classEntry = implementationClass.replace('.', '/') + ".class";
        Files.createDirectories(jarPath.getParent());
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jarPath))) {
            output.putNextEntry(new JarEntry(classEntry));
            Files.copy(classesPath.resolve(classEntry), output);
            output.closeEntry();
            output.putNextEntry(new JarEntry("META-INF/services/" + serviceClass.getName()));
            output.write(implementationClass.getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
    }

    /** Returns a factory implementation that is compiled only into the temporary starter jar. */
    private static String imapFactorySource() {
        return "package isolated;\n"
                + "public final class IsolatedIMapStorageFactory implements "
                + IMapStorageFactory.class.getName()
                + " {\n"
                + "  public String factoryIdentifier() { return \""
                + TEST_STORAGE_TYPE
                + "\"; }\n"
                + "  public "
                + IMapStorage.class.getName()
                + " create(java.util.Map<String, Object> configuration) {\n"
                + "    return ("
                + IMapStorage.class.getName()
                + ") java.lang.reflect.Proxy.newProxyInstance(\n"
                + "        getClass().getClassLoader(), new Class<?>[] {"
                + IMapStorage.class.getName()
                + ".class}, (proxy, method, args) -> {\n"
                + "          if (method.getReturnType().equals(boolean.class)) return true;\n"
                + "          if (method.getReturnType().equals(java.util.Set.class)) "
                + "return java.util.Collections.emptySet();\n"
                + "          if (method.getReturnType().equals(java.util.Map.class)) "
                + "return java.util.Collections.emptyMap();\n"
                + "          return null;\n"
                + "        });\n"
                + "  }\n"
                + "}\n";
    }

    private Properties buildLocalFsProperties(String namespace) {
        Properties props = new Properties();
        // "hdfs" is the factoryIdentifier of IMapFileStorageFactory — covers local fs via
        // fs.defaultFS=file:///
        props.setProperty("type", "hdfs");
        props.setProperty("fs.defaultFS", "file:///");
        props.setProperty("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        props.setProperty(FileConstants.FileInitProperties.BUSINESS_KEY, "test");
        props.setProperty(FileConstants.FileInitProperties.NAMESPACE_KEY, namespace);
        props.setProperty(FileConstants.FileInitProperties.CLUSTER_NAME, "test-cluster");
        return props;
    }
}
