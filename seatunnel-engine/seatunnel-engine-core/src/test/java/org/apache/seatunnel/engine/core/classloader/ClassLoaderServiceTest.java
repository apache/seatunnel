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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

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
    void testJarUrlCacheDisabledOnStartup() {
        DefaultClassLoaderService newService = new DefaultClassLoaderService(false, null);
        Assertions.assertNotNull(newService);
        newService.close();
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
}
