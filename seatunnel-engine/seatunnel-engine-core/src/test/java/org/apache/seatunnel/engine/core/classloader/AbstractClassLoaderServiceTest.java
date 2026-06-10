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

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

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

    /**
     * Verifies that child-first classloaders can isolate identical Debezium class names when the
     * backing jars differ.
     */
    @Test
    void testDifferentClassLoadersCanHostConflictingDebeziumClasses() throws Exception {
        File versionOneJar = createVersionProbeJar("v1");
        File versionTwoJar = createVersionProbeJar("v2");
        URL versionOneUrl = versionOneJar.toURI().toURL();
        URL versionTwoUrl = versionTwoJar.toURI().toURL();

        ClassLoader versionOneClassLoader =
                classLoaderService.getClassLoader(11L, Collections.singletonList(versionOneUrl));
        ClassLoader versionTwoClassLoader =
                classLoaderService.getClassLoader(12L, Collections.singletonList(versionTwoUrl));

        Class<?> versionOneProbe =
                versionOneClassLoader.loadClass("io.debezium.testing.VersionProbe");
        Class<?> versionTwoProbe =
                versionTwoClassLoader.loadClass("io.debezium.testing.VersionProbe");

        Assertions.assertNotSame(versionOneProbe, versionTwoProbe);
        Assertions.assertEquals("v1", versionOneProbe.getMethod("version").invoke(null));
        Assertions.assertEquals("v2", versionTwoProbe.getMethod("version").invoke(null));

        classLoaderService.releaseClassLoader(11L, Collections.singletonList(versionOneUrl));
        classLoaderService.releaseClassLoader(12L, Collections.singletonList(versionTwoUrl));
    }

    /**
     * Builds a temporary jar that exports the same Debezium-like class name but returns a
     * caller-provided version string.
     */
    private static File createVersionProbeJar(String version) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assertions.assertNotNull(compiler, "A JDK compiler is required to build test probe jars");

        Path tempRoot = Files.createTempDirectory("debezium-version-probe-" + version);
        Path sourceFile = tempRoot.resolve("io/debezium/testing/VersionProbe.java");
        Files.createDirectories(sourceFile.getParent());
        Files.write(
                sourceFile,
                Collections.singletonList(
                        "package io.debezium.testing;"
                                + " public class VersionProbe {"
                                + " public static String version() { return \""
                                + version
                                + "\"; }"
                                + " }"),
                StandardCharsets.UTF_8);

        try (StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
            Iterable<? extends JavaFileObject> compilationUnits =
                    fileManager.getJavaFileObjectsFromFiles(
                            Collections.singletonList(sourceFile.toFile()));
            Boolean success =
                    compiler.getTask(
                                    null,
                                    fileManager,
                                    null,
                                    Lists.newArrayList("-proc:none", "-d", tempRoot.toString()),
                                    null,
                                    compilationUnits)
                            .call();
            Assertions.assertTrue(Boolean.TRUE.equals(success), "Failed to compile probe jar");
        }

        Path classFile = tempRoot.resolve("io/debezium/testing/VersionProbe.class");
        File jarFile = tempRoot.resolve("version-probe-" + version + ".jar").toFile();
        try (JarOutputStream jarOutputStream = new JarOutputStream(new FileOutputStream(jarFile))) {
            jarOutputStream.putNextEntry(new JarEntry("io/debezium/testing/VersionProbe.class"));
            jarOutputStream.write(Files.readAllBytes(classFile));
            jarOutputStream.closeEntry();
        }
        return jarFile;
    }

    @AfterEach
    void close() {
        classLoaderService.close();
    }
}
