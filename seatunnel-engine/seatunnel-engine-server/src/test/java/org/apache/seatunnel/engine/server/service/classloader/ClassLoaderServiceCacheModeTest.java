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

package org.apache.seatunnel.engine.server.service.classloader;

import org.apache.curator.shaded.com.google.common.collect.Lists;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;

public class ClassLoaderServiceCacheModeTest extends AbstractClassLoaderServiceTest {

    @Override
    boolean cacheMode() {
        return true;
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
        Assertions.assertEquals(1, classLoaderService.queryClassLoaderCount());
    }

    @Test
    void testSameJarInDifferentJob() throws MalformedURLException {
        classLoaderService.getClassLoader(
                2L,
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
                2L,
                Lists.newArrayList(new URL("file:///console.jar"), new URL("file:///fake.jar")));
        Assertions.assertEquals(1, classLoaderService.queryClassLoaderCount());
    }
}
