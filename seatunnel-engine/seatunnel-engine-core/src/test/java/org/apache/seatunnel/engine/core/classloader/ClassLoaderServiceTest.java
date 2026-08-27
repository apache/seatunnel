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
