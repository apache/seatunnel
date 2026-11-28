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

package org.apache.seatunnel.core.starter.flink.execution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarFile;

/**
 * Loads {@code FlinkLineageHooks} from the packaged flink-20 starter jar.
 *
 * <p>The module tests run with {@code target/classes} ahead of dependencies, so they always see
 * this module's copy. The shaded jar is what users launch. The common starter contributes a class
 * of the same name whose {@code isSupported()} is false; this test fails if that copy is the one
 * that ended up in the jar.
 */
class TestPackagedFlinkLineageHooksIT {

    private static final String HOOKS =
            "org.apache.seatunnel.core.starter.flink.execution.FlinkLineageHooks";

    @Test
    void packagedStarterReportsLineageSupported() throws Exception {
        File jar = new File("target/seatunnel-flink-20-starter.jar");
        Assertions.assertTrue(jar.isFile(), "package the starter before this IT: " + jar);
        try (JarFile jarFile = new JarFile(jar, false)) {
            Assertions.assertNotNull(
                    jarFile.getJarEntry(HOOKS.replace('.', '/') + ".class"),
                    "the starter jar must contain FlinkLineageHooks");
        }
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        try (ChildFirst loader = new ChildFirst(new URL[] {jar.toURI().toURL()}, parent)) {
            Class<?> hooks = loader.loadClass(HOOKS);
            Assertions.assertSame(loader, hooks.getClassLoader());
            Method isSupported = hooks.getDeclaredMethod("isSupported");
            isSupported.setAccessible(true);
            Assertions.assertEquals(
                    Boolean.TRUE,
                    isSupported.invoke(null),
                    "the packaged starter must contain the Flink 1.20 FlinkLineageHooks");
        }
    }

    /**
     * Defines {@code FlinkLineageHooks} from the starter jar. Every other class still comes from
     * the test classpath, which has the Flink types the class file refers to.
     */
    private static final class ChildFirst extends URLClassLoader {
        private ChildFirst(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!HOOKS.equals(name)) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    loaded = findClass(name);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }
    }
}
