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

import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;

public abstract class AbstractClassLoaderServiceTest {

    protected static DefaultClassLoaderService classLoaderService;

    abstract boolean cacheMode();

    @BeforeEach
    void setUp() {
        classLoaderService = new DefaultClassLoaderService(cacheMode(), null);
    }

    @Test
    void testBasicFunction() {
        SeaTunnelChildFirstClassLoader classLoader =
                (SeaTunnelChildFirstClassLoader)
                        classLoaderService.getClassLoader(2L, Collections.emptyList());
        Assertions.assertEquals(0, classLoader.getURLs().length);
        ClassLoader classLoader2 =
                classLoaderService.queryClassLoaderById(2L, Collections.emptyList()).get();
        Assertions.assertSame(classLoader, classLoader2);
        Assertions.assertEquals(
                1, classLoaderService.queryClassLoaderReferenceCount(2L, Collections.emptyList()));
        classLoaderService.releaseClassLoader(2L, Collections.emptyList());
        Assertions.assertEquals(
                0, classLoaderService.queryClassLoaderReferenceCount(2L, Collections.emptyList()));
        if (cacheMode()) {
            Assertions.assertTrue(
                    classLoaderService
                            .queryClassLoaderById(2L, Collections.emptyList())
                            .isPresent());
        } else {
            Assertions.assertFalse(
                    classLoaderService
                            .queryClassLoaderById(2L, Collections.emptyList())
                            .isPresent());
        }
    }

    @Test
    void testJarOrderMismatch() throws MalformedURLException {
        ClassLoader classLoader1 =
                classLoaderService.getClassLoader(
                        3L,
                        Lists.newArrayList(
                                new URL("file:///fake.jar"), new URL("file:///console.jar")));
        ClassLoader classLoader2 =
                classLoaderService.getClassLoader(
                        3L,
                        Lists.newArrayList(
                                new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertSame(classLoader1, classLoader2);
        Assertions.assertEquals(
                2,
                classLoaderService.queryClassLoaderReferenceCount(
                        3L,
                        Lists.newArrayList(
                                new URL("file:///console.jar"), new URL("file:///fake.jar"))));
        classLoaderService.releaseClassLoader(
                3L,
                Lists.newArrayList(new URL("file:///fake.jar"), new URL("file:///console.jar")));
        Assertions.assertEquals(
                1,
                classLoaderService.queryClassLoaderReferenceCount(
                        3L,
                        Lists.newArrayList(
                                new URL("file:///console.jar"), new URL("file:///fake.jar"))));
    }

    @Test
    void testErrorInvoke() throws MalformedURLException {
        classLoaderService.releaseClassLoader(
                2L,
                Lists.newArrayList(new URL("file:///fake.jar"), new URL("file:///console.jar")));
        Assertions.assertEquals(0, classLoaderService.queryClassLoaderCount());
    }

    @AfterEach
    void close() {
        classLoaderService.close();
    }
}
