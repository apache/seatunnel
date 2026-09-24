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
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class ClassLoaderServiceTest extends AbstractClassLoaderServiceTest {
    @TempDir private Path jarDirectory;

    @Test
    void testInjectedResolverLoadsLocalJarAndRetainsOriginalIdentity() throws Exception {
        Path localJar = jarDirectory.resolve("plugin with spaces.jar");
        createJar(localJar);
        URL original = new URL("file:/remote-node/connectors/plugin.jar");
        AtomicInteger resolutions = new AtomicInteger();
        DefaultClassLoaderService service =
                new DefaultClassLoaderService(
                        false,
                        null,
                        jars -> {
                            Assertions.assertEquals(Collections.singletonList(original), jars);
                            resolutions.incrementAndGet();
                            return Collections.singletonList(localJar.toUri().toURL());
                        });
        try {
            SeaTunnelChildFirstClassLoader loader =
                    (SeaTunnelChildFirstClassLoader)
                            service.getClassLoader(7L, Collections.singletonList(original));
            Assertions.assertEquals(localJar.toUri().toURL(), loader.getURLs()[0]);
            try (InputStream resource = loader.getResourceAsStream("resolver-marker.txt")) {
                Assertions.assertNotNull(resource);
                Assertions.assertEquals('w', resource.read());
            }
            Assertions.assertSame(
                    loader, service.getClassLoader(7L, Collections.singletonList(original)));
            Assertions.assertEquals(1, resolutions.get());
            Assertions.assertTrue(
                    service.queryClassLoaderById(7L, Collections.singletonList(original))
                            .isPresent());
            service.releaseClassLoader(7L, Collections.singletonList(original));
            Assertions.assertEquals(1, service.queryClassLoaderCount());
            service.releaseClassLoader(7L, Collections.singletonList(original));
            Assertions.assertEquals(0, service.queryClassLoaderCount());
        } finally {
            service.close();
        }
    }

    @Test
    void testExistingConstructorKeepsOriginalJarUrl() throws Exception {
        Path jar = jarDirectory.resolve("unchanged.jar");
        createJar(jar);
        URL original = jar.toUri().toURL();
        DefaultClassLoaderService service = new DefaultClassLoaderService(false, null);
        try {
            SeaTunnelChildFirstClassLoader loader =
                    (SeaTunnelChildFirstClassLoader)
                            service.getClassLoader(8L, Collections.singletonList(original));
            Assertions.assertEquals(original, loader.getURLs()[0]);
        } finally {
            service.close();
        }
    }

    @Test
    void testResolverFailureDoesNotCacheFailedLoader() throws Exception {
        Path jar = jarDirectory.resolve("retry.jar");
        createJar(jar);
        URL original = jar.toUri().toURL();
        AtomicInteger attempts = new AtomicInteger();
        DefaultClassLoaderService service =
                new DefaultClassLoaderService(
                        false,
                        null,
                        jars -> {
                            if (attempts.getAndIncrement() == 0) {
                                throw new IOException("artifact unavailable");
                            }
                            return jars;
                        });
        try {
            Assertions.assertThrows(
                    IOException.class,
                    () -> service.getClassLoader(9L, Collections.singletonList(original)));
            Assertions.assertEquals(0, service.queryClassLoaderCount());
            Assertions.assertNotNull(
                    service.getClassLoader(9L, Collections.singletonList(original)));
            service.releaseClassLoader(9L, Collections.singletonList(original));
            Assertions.assertEquals(0, service.queryClassLoaderCount());
        } finally {
            service.close();
        }
    }

    private void createJar(Path jar) throws IOException {
        Files.createDirectories(jar.getParent());
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry("resolver-marker.txt"));
            output.write("worker-local-resource".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
    }

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
        ClassLoader classLoader =
                classLoaderService.getClassLoader(
                        3L,
                        Lists.newArrayList(
                                new URL("file:///console.jar"), new URL("file:///fake.jar")));
        ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        Thread thread =
                new Thread(
                        () -> {
                            while (Thread.currentThread().getContextClassLoader() != null) {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
        thread.start();
        Thread.currentThread().setContextClassLoader(appClassLoader);
        Assertions.assertEquals(classLoader, thread.getContextClassLoader());
        classLoaderService.releaseClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertNull(thread.getContextClassLoader());
        Thread.sleep(2000);
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
}
