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

package org.apache.seatunnel.engine.core.classloader;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.engine.common.exception.ClassLoaderException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ClassLoaderServiceTest extends AbstractClassLoaderServiceTest {

    @Override
    boolean cacheMode() {
        return false;
    }

    @Test
    void testSameJarInSameJob() throws MalformedURLException {
        classLoaderService.getClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///fake.jar"), new URL("file:///console.jar")));
        classLoaderService.getClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertEquals(1, classLoaderService.queryClassLoaderCount());
        classLoaderService.releaseClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertEquals(1, classLoaderService.queryClassLoaderCount());
        classLoaderService.releaseClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertEquals(0, classLoaderService.queryClassLoaderCount());
    }

    @Test
    void testSameJarInDifferentJob() throws MalformedURLException {
        classLoaderService.getClassLoader(
                2L,
                Lists.newArrayList(new URL("file:///fake.jar"), new URL("file:///console.jar")));
        classLoaderService.getClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertEquals(2, classLoaderService.queryClassLoaderCount());
        classLoaderService.releaseClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertEquals(1, classLoaderService.queryClassLoaderCount());
        classLoaderService.releaseClassLoader(
                2L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertEquals(0, classLoaderService.queryClassLoaderCount());
    }

    @Test
    void testRecycleClassLoaderFromThread() throws MalformedURLException, InterruptedException {
        ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
        ClassLoader classLoader =
                classLoaderService.getClassLoader(
                        3L,
                        Lists.newArrayList(
                                new URL("file:///console.jar"), new URL("file:///fake.jar")));
        ClassLoader originalTCCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        Thread thread =
                new Thread(
                        () -> {
                            while (Thread.currentThread().getContextClassLoader() != systemLoader) {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
        thread.start();
        Thread.currentThread().setContextClassLoader(originalTCCL);
        Assertions.assertEquals(classLoader, thread.getContextClassLoader());
        classLoaderService.releaseClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertEquals(systemLoader, thread.getContextClassLoader());
        thread.join(5000);
        Assertions.assertFalse(thread.isAlive());
    }

    @Test
    void testPreCheckJar() throws IOException {

        // Mocking Node and NodeEngineImpl for testing
        Node mockNode = Mockito.mock(Node.class);
        Mockito.when(mockNode.getThisAddress()).thenReturn(new Address("localhost", 5801));
        NodeEngineImpl mockNodeEngine = Mockito.mock(NodeEngineImpl.class);
        Mockito.when(mockNodeEngine.getNode()).thenReturn(mockNode);
        // Creating DefaultClassLoaderService object for testing
        DefaultClassLoaderService defaultClassLoaderService =
                new DefaultClassLoaderService(cacheMode(), mockNodeEngine);
        // Test case to check ClassLoaderException when file is not found
        Assertions.assertThrows(
                ClassLoaderException.class,
                () -> {
                    try {
                        defaultClassLoaderService.getClassLoader(
                                3L, Lists.newArrayList(new URL("file:/fake.jar")));
                    } catch (ClassLoaderException e) {
                        Assertions.assertTrue(
                                e.getMessage()
                                        .contains(
                                                "The jar file file:/fake.jar can not be found in node localhost, please ensure that the deployment paths of SeaTunnel on different nodes are consistent."));
                        throw e;
                    }
                });

        // Creating a temporary jar file for testing
        File tempJar = File.createTempFile("console", ".jar");
        String tempJarPath = tempJar.toURI().toURL().toString();

        // Test case to check successful class loader creation with existing jar file
        Assertions.assertDoesNotThrow(
                () ->
                        defaultClassLoaderService.getClassLoader(
                                3L, Lists.newArrayList(new URL(tempJarPath))));

        // Deleting the temporary jar file after test
        tempJar.delete();
    }

    @Test
    void testClassLoaderClosedOnRelease() throws IOException {
        File tempJar = File.createTempFile("test", ".jar");
        URL jarUrl = tempJar.toURI().toURL();

        ClassLoader classLoader = classLoaderService.getClassLoader(1L, Lists.newArrayList(jarUrl));
        Assertions.assertNotNull(classLoader);
        Assertions.assertTrue(classLoader instanceof URLClassLoader);

        classLoaderService.releaseClassLoader(1L, Lists.newArrayList(jarUrl));

        Assertions.assertFalse(
                classLoaderService
                        .queryClassLoaderById(1L, Lists.newArrayList(jarUrl))
                        .isPresent());

        tempJar.delete();
    }

    @Test
    void testCloseAllClassLoadersOnServiceClose() throws IOException {
        File tempJar1 = File.createTempFile("test1", ".jar");
        File tempJar2 = File.createTempFile("test2", ".jar");

        classLoaderService.getClassLoader(1L, Lists.newArrayList(tempJar1.toURI().toURL()));
        classLoaderService.getClassLoader(2L, Lists.newArrayList(tempJar2.toURI().toURL()));

        Assertions.assertEquals(2, classLoaderService.queryClassLoaderCount());

        classLoaderService.close();

        Assertions.assertEquals(0, classLoaderService.queryClassLoaderCount());

        tempJar1.delete();
        tempJar2.delete();
    }

    @Test
    void testDeepCleanModeDisabledByDefault() {
        Assertions.assertFalse(
                Boolean.parseBoolean(
                        System.getProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN, "false")));
    }

    @Test
    void testDeepCleanEnabledResolution() {
        // property wins over env
        Assertions.assertTrue(DefaultClassLoaderService.resolveDeepCleanEnabled("true", "false"));
        // env is used when property is absent
        Assertions.assertTrue(DefaultClassLoaderService.resolveDeepCleanEnabled(null, "true"));
        // neither present -> false
        Assertions.assertFalse(DefaultClassLoaderService.resolveDeepCleanEnabled(null, null));
        Assertions.assertFalse(DefaultClassLoaderService.resolveDeepCleanEnabled(null, "false"));
    }

    @Test
    void testDeepCleanModeEnabled() throws IOException {
        File tempJar = File.createTempFile("test", ".jar");
        URL jarUrl = tempJar.toURI().toURL();

        try {
            System.setProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN, "true");

            DefaultClassLoaderService serviceWithDeepClean =
                    new DefaultClassLoaderService(false, null);

            ClassLoader classLoader =
                    serviceWithDeepClean.getClassLoader(1L, Lists.newArrayList(jarUrl));
            Assertions.assertNotNull(classLoader);

            serviceWithDeepClean.releaseClassLoader(1L, Lists.newArrayList(jarUrl));
            serviceWithDeepClean.close();
        } finally {
            System.clearProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN);
        }

        tempJar.delete();
    }

    @Test
    void testDefaultPathServiceConstructionSucceeds() {
        DefaultClassLoaderService newService = new DefaultClassLoaderService(false, null);
        Assertions.assertNotNull(newService);
        newService.close();
    }

    @Test
    @Order(1)
    void testDefaultPathDoesNotMutateUrlCacheDefaults() throws Exception {
        // Behavior under test: the default (non-deep-clean) path must not touch
        // URLConnection default useCaches for any protocol. Verifies the fix for the
        // JDK 8 global-mutation blocker raised in review.
        //
        // Why @Order(1): URLConnection default cache state is JVM-global and permanent.
        // Any deep-clean test that runs before this one would have already flipped it,
        // making the assumption below skip (and the test trivially "pass" with no actual
        // coverage). Forcing this test to run first preserves the pristine state and
        // makes the assertion meaningful in CI.
        //
        // Note on API choice: on JDK 9+, getDefaultUseCaches() (instance 0-arg) returns
        // the JVM-wide defaultUseCaches field which is NOT updated by the per-protocol
        // setDefaultUseCaches(String, boolean) call. We use getUseCaches() on a freshly
        // opened connection instead, which reflects the per-protocol default consulted
        // at URLConnection construction time on both JDK 8 and JDK 9+.
        boolean jarBefore = new URL("jar:file://dummy.jar!/").openConnection().getUseCaches();
        boolean httpBefore = new URL("http://localhost").openConnection().getUseCaches();
        boolean fileBefore = new URL("file:/localhost").openConnection().getUseCaches();
        Assumptions.assumeTrue(
                jarBefore && httpBefore && fileBefore,
                "URL cache defaults already mutated by a prior test; skipping");

        DefaultClassLoaderService service = new DefaultClassLoaderService(false, null);
        try {
            Assertions.assertTrue(
                    new URL("jar:file://dummy.jar!/").openConnection().getUseCaches(),
                    "JAR useCaches must be preserved (not mutated) on default path");
            Assertions.assertTrue(
                    new URL("http://localhost").openConnection().getUseCaches(),
                    "HTTP useCaches must be preserved (not mutated) on default path");
            Assertions.assertTrue(
                    new URL("file:/localhost").openConnection().getUseCaches(),
                    "file useCaches must be preserved (not mutated) on default path");
        } finally {
            service.close();
        }
    }

    @Test
    @Order(2)
    void testDeepCleanDisablesJarUrlCache() throws Exception {
        // Behavior under test: when SEATUNNEL_CLASSLOADER_DEEP_CLEAN=true, the JAR
        // URLConnection per-protocol default useCaches must be false after service
        // construction. Verified via getUseCaches() on a freshly opened JAR connection,
        // which is initialized from the per-protocol default at URLConnection
        // construction time (JDK 9+) or the JVM-global default (JDK 8 fallback path).
        // Note: on JDK 8 the fallback uses the JVM-global 1-arg toggle, so deep-clean
        // there actually disables caching for ALL protocols (http/file included), not
        // just JAR; the "jar-only" scope holds only on JDK 9+ via the protocol-scoped
        // API. This test asserts only the JAR outcome, which holds on every JDK.
        // Robust to test ordering: if a prior deep-clean test already flipped it, this
        // assertion still holds; if this test runs first, this test itself flips it.
        try {
            System.setProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN, "true");
            DefaultClassLoaderService service = new DefaultClassLoaderService(false, null);
            try {
                Assertions.assertFalse(
                        new URL("jar:file://dummy.jar!/").openConnection().getUseCaches(),
                        "JAR URLConnection useCaches must be false under deep-clean");
            } finally {
                service.close();
            }
        } finally {
            System.clearProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN);
        }
    }

    @Test
    @Order(3)
    void testDeepCleanPreservesNonJarProtocolsOnJdk9Plus() throws Exception {
        // Behavior under test: on JDK 9+, the protocol-scoped setDefaultUseCaches("jar", false)
        // overload must be used so non-JAR protocols (http/file) keep their default useCaches
        // value. This is the actual fix for the nzw921rx blocker: the previous code used the
        // JDK 8 global 1-arg API on every JDK, flipping HTTP/file defaults too.
        boolean hasProtocolScopedApi;
        try {
            URLConnection.class.getMethod("setDefaultUseCaches", String.class, boolean.class);
            hasProtocolScopedApi = true;
        } catch (NoSuchMethodException e) {
            hasProtocolScopedApi = false;
        }
        Assumptions.assumeTrue(
                hasProtocolScopedApi,
                "Test only meaningful on JDK 9+ with protocol-scoped setDefaultUseCaches");

        try {
            System.setProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN, "true");
            DefaultClassLoaderService service = new DefaultClassLoaderService(false, null);
            try {
                Assertions.assertTrue(
                        new URL("http://localhost").openConnection().getUseCaches(),
                        "HTTP useCaches must be preserved on JDK 9+");
                Assertions.assertTrue(
                        new URL("file:/localhost").openConnection().getUseCaches(),
                        "file useCaches must be preserved on JDK 9+");
            } finally {
                service.close();
            }
        } finally {
            System.clearProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN);
        }
    }

    @Test
    @Order(4)
    void testDeepCleanGracefulOnReflectionFailure() throws Exception {
        // Behavior under test: when the reflective URLClassPath cache clearing fails
        // (e.g. JDK 9+ without --add-opens java.base/jdk.internal.loader), the failure
        // must be caught and degraded to a WARN log. The caller (releaseClassLoader /
        // close) must not observe the exception, and the classloader must still be
        // evicted from the cache. URLClassLoader.close() (called above the reflective
        // hook) already released the underlying JarFile fd handles; only the
        // stale-reference cleanup is skipped on failure.
        File tempJar = File.createTempFile("test", ".jar");
        URL jarUrl = tempJar.toURI().toURL();
        System.setProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN, "true");
        try {
            // 'service' is effectively final (single assignment) so it can be captured
            // by the lambdas below. Nested try/finally ensures close is invoked even if
            // an assertion fails before the explicit close-assertion runs.
            DefaultClassLoaderService service =
                    new DefaultClassLoaderService(false, null) {
                        @Override
                        protected void clearUrlClassPathCacheReflectively(URLClassLoader cl) {
                            // Simulate InaccessibleObjectException thrown by the JDK on JDK 9+
                            // without --add-opens java.base/jdk.internal.loader=ALL-UNNAMED.
                            // InaccessibleObjectException is a RuntimeException on JDK 9+ but
                            // does not exist on JDK 8; using a plain RuntimeException keeps
                            // this test compilable on every JDK.
                            throw new RuntimeException(
                                    "Simulated InaccessibleObjectException for test purposes");
                        }
                    };
            try {
                ClassLoader classLoader = service.getClassLoader(1L, Lists.newArrayList(jarUrl));
                Assertions.assertNotNull(classLoader);

                // Behavior: release must not propagate the reflective failure
                Assertions.assertDoesNotThrow(
                        () -> service.releaseClassLoader(1L, Lists.newArrayList(jarUrl)));

                // Behavior: classloader must still be evicted from the cache
                Assertions.assertFalse(
                        service.queryClassLoaderById(1L, Lists.newArrayList(jarUrl)).isPresent());

                // Behavior: service close must also not propagate
                Assertions.assertDoesNotThrow(service::close);
            } finally {
                service.close();
            }
        } finally {
            System.clearProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN);
            tempJar.delete();
        }
    }

    @Test
    void testCloseWithEmptyClassLoaderCache() {
        DefaultClassLoaderService service = new DefaultClassLoaderService(false, null);
        Assertions.assertDoesNotThrow(service::close);
    }

    @Test
    void testReleaseNonExistentClassLoader() throws MalformedURLException {
        URL fakeUrl = new URL("file:///nonexistent.jar");
        Assertions.assertDoesNotThrow(
                () -> classLoaderService.releaseClassLoader(999L, Lists.newArrayList(fakeUrl)));
    }

    @Test
    void testMultipleReleaseCalls() throws IOException {
        File tempJar = File.createTempFile("test", ".jar");
        URL jarUrl = tempJar.toURI().toURL();

        classLoaderService.getClassLoader(1L, Lists.newArrayList(jarUrl));
        classLoaderService.releaseClassLoader(1L, Lists.newArrayList(jarUrl));

        Assertions.assertDoesNotThrow(
                () -> classLoaderService.releaseClassLoader(1L, Lists.newArrayList(jarUrl)));

        tempJar.delete();
    }

    @Test
    void testClassLoaderClosedOnReleaseWithDeepClean() throws Exception {
        TestJar testJar =
                createTestJar(
                        "release",
                        "org.apache.seatunnel.test.release.BeforeRelease",
                        "org.apache.seatunnel.test.release.AfterRelease");
        DefaultClassLoaderService serviceWithDeepClean = null;
        try {
            System.setProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN, "true");
            serviceWithDeepClean = new DefaultClassLoaderService(false, null);

            ClassLoader classLoader =
                    serviceWithDeepClean.getClassLoader(
                            1L, Lists.newArrayList(testJar.getJarUrl()));
            Assertions.assertNotNull(classLoader);
            Assertions.assertTrue(classLoader instanceof URLClassLoader);
            Assertions.assertSame(
                    classLoader,
                    classLoader
                            .loadClass("org.apache.seatunnel.test.release.BeforeRelease")
                            .getClassLoader());

            serviceWithDeepClean.releaseClassLoader(1L, Lists.newArrayList(testJar.getJarUrl()));

            Assertions.assertFalse(
                    serviceWithDeepClean
                            .queryClassLoaderById(1L, Lists.newArrayList(testJar.getJarUrl()))
                            .isPresent());
            Assertions.assertThrows(
                    ClassNotFoundException.class,
                    () -> classLoader.loadClass("org.apache.seatunnel.test.release.AfterRelease"));

            URL resourceAfter =
                    classLoader.getResource("org/apache/seatunnel/test/release/AfterRelease.class");
            Assertions.assertNull(resourceAfter);
        } finally {
            if (serviceWithDeepClean != null) {
                serviceWithDeepClean.close();
            }
            System.clearProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN);
            testJar.close();
        }
    }

    @Test
    void testCloseAllClassLoadersOnServiceCloseWithDeepClean() throws Exception {
        TestJar firstJar =
                createTestJar(
                        "close-one",
                        "org.apache.seatunnel.test.closeone.BeforeClose",
                        "org.apache.seatunnel.test.closeone.AfterClose");
        TestJar secondJar =
                createTestJar(
                        "close-two",
                        "org.apache.seatunnel.test.closetwo.BeforeClose",
                        "org.apache.seatunnel.test.closetwo.AfterClose");
        DefaultClassLoaderService serviceWithDeepClean = null;
        try {
            System.setProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN, "true");
            serviceWithDeepClean = new DefaultClassLoaderService(false, null);

            ClassLoader firstClassLoader =
                    serviceWithDeepClean.getClassLoader(
                            1L, Lists.newArrayList(firstJar.getJarUrl()));
            ClassLoader secondClassLoader =
                    serviceWithDeepClean.getClassLoader(
                            2L, Lists.newArrayList(secondJar.getJarUrl()));

            Assertions.assertSame(
                    firstClassLoader,
                    firstClassLoader
                            .loadClass("org.apache.seatunnel.test.closeone.BeforeClose")
                            .getClassLoader());
            Assertions.assertSame(
                    secondClassLoader,
                    secondClassLoader
                            .loadClass("org.apache.seatunnel.test.closetwo.BeforeClose")
                            .getClassLoader());
            Assertions.assertEquals(2, serviceWithDeepClean.queryClassLoaderCount());

            serviceWithDeepClean.close();

            Assertions.assertEquals(0, serviceWithDeepClean.queryClassLoaderCount());
            Assertions.assertThrows(
                    ClassNotFoundException.class,
                    () ->
                            firstClassLoader.loadClass(
                                    "org.apache.seatunnel.test.closeone.AfterClose"));
            Assertions.assertThrows(
                    ClassNotFoundException.class,
                    () ->
                            secondClassLoader.loadClass(
                                    "org.apache.seatunnel.test.closetwo.AfterClose"));
        } finally {
            if (serviceWithDeepClean != null) {
                serviceWithDeepClean.close();
            }
            System.clearProperty(DefaultClassLoaderService.ENABLE_DEEP_CLEAN);
            firstJar.close();
            secondJar.close();
        }
    }

    private TestJar createTestJar(String jarName, String... classNames) throws IOException {
        Path rootDir = Files.createTempDirectory("classloader-test-");
        Path sourceDir = Files.createDirectories(rootDir.resolve("src"));
        Path classesDir = Files.createDirectories(rootDir.resolve("classes"));
        List<File> sourceFiles = new ArrayList<>();
        for (String className : classNames) {
            sourceFiles.add(createSourceFile(sourceDir, className).toFile());
        }
        compileSourceFiles(sourceFiles, classesDir);
        Path jarPath = rootDir.resolve(jarName + ".jar");
        createJar(classesDir, jarPath);
        return new TestJar(rootDir, jarPath.toUri().toURL());
    }

    private Path createSourceFile(Path sourceDir, String className) throws IOException {
        int packageSeparator = className.lastIndexOf('.');
        String packageName = packageSeparator >= 0 ? className.substring(0, packageSeparator) : "";
        String simpleName =
                packageSeparator >= 0 ? className.substring(packageSeparator + 1) : className;
        Path packageDir =
                packageName.isEmpty()
                        ? sourceDir
                        : Files.createDirectories(
                                sourceDir.resolve(packageName.replace('.', File.separatorChar)));
        Path sourceFile = packageDir.resolve(simpleName + ".java");
        String sourceCode =
                (packageName.isEmpty() ? "" : "package " + packageName + ";\n\n")
                        + "public class "
                        + simpleName
                        + " {\n"
                        + "    public String value() {\n"
                        + "        return \""
                        + simpleName
                        + "\";\n"
                        + "    }\n"
                        + "}\n";
        Files.write(sourceFile, sourceCode.getBytes(StandardCharsets.UTF_8));
        return sourceFile;
    }

    private void compileSourceFiles(List<File> sourceFiles, Path classesDir) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assertions.assertNotNull(compiler, "A JDK compiler is required for this test");
        try (StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(null, null, null)) {
            Iterable<? extends JavaFileObject> compilationUnits =
                    fileManager.getJavaFileObjectsFromFiles(sourceFiles);
            Boolean success =
                    compiler.getTask(
                                    null,
                                    fileManager,
                                    null,
                                    Arrays.asList("-d", classesDir.toString(), "-proc:none"),
                                    null,
                                    compilationUnits)
                            .call();
            Assertions.assertEquals(Boolean.TRUE, success, "Failed to compile test classes");
        }
    }

    private void createJar(Path classesDir, Path jarPath) throws IOException {
        try (JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(jarPath));
                Stream<Path> classFiles = Files.walk(classesDir)) {
            for (Path classFile :
                    classFiles.filter(Files::isRegularFile).collect(Collectors.toList())) {
                String entryName =
                        classesDir
                                .relativize(classFile)
                                .toString()
                                .replace(File.separatorChar, '/');
                jarOutputStream.putNextEntry(new JarEntry(entryName));
                Files.copy(classFile, jarOutputStream);
                jarOutputStream.closeEntry();
            }
        }
    }

    private void deleteRecursively(Path rootDir) throws IOException {
        if (rootDir == null || Files.notExists(rootDir)) {
            return;
        }
        IOException deleteException = null;
        try (Stream<Path> paths = Files.walk(rootDir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    if (deleteException == null) {
                        deleteException = e;
                    } else {
                        deleteException.addSuppressed(e);
                    }
                }
            }
        }
        if (deleteException != null) {
            throw deleteException;
        }
    }

    private final class TestJar implements AutoCloseable {
        private final Path rootDir;
        private final URL jarUrl;

        private TestJar(Path rootDir, URL jarUrl) {
            this.rootDir = rootDir;
            this.jarUrl = jarUrl;
        }

        private URL getJarUrl() {
            return jarUrl;
        }

        @Override
        public void close() throws IOException {
            deleteRecursively(rootDir);
        }
    }
}
