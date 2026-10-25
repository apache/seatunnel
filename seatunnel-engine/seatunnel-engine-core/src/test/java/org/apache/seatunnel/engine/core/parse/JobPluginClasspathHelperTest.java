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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class JobPluginClasspathHelperTest {

    @Test
    void thirdPartyJarsFromEnv_emptyReadonlyConfig() {
        ReadonlyConfig env = ReadonlyConfig.fromMap(Collections.emptyMap());
        Assertions.assertTrue(JobPluginClasspathHelper.thirdPartyJarsFromEnv(env).isEmpty());
    }

    @Test
    void mergedSourceAndTransformJarUrls_distinctPreservesFirstEncounter(@TempDir Path dir)
            throws Exception {
        Path p1 = dir.resolve("a.jar");
        Path p2 = dir.resolve("b.jar");
        Path p3 = dir.resolve("c.jar");
        Files.createFile(p1);
        Files.createFile(p2);
        Files.createFile(p3);
        URL a = p1.toUri().toURL();
        URL b = p2.toUri().toURL();
        URL c = p3.toUri().toURL();

        List<URL> merged =
                JobPluginClasspathHelper.mergedSourceAndTransformJarUrls(
                        Arrays.asList(a, b), Arrays.asList(c, a));

        Assertions.assertEquals(3, merged.size());
        Assertions.assertEquals(a, merged.get(0));
        Assertions.assertEquals(b, merged.get(1));
        Assertions.assertEquals(c, merged.get(2));
    }

    @Test
    void connectorJarList_emptyConfigs_returnsOnlyCommonJars(@TempDir Path dir) throws Exception {
        Path jar = dir.resolve("common.jar");
        Files.createFile(jar);
        URL commonUrl = jar.toUri().toURL();
        List<URL> common = Collections.singletonList(commonUrl);

        for (PluginType type :
                new PluginType[] {PluginType.SOURCE, PluginType.TRANSFORM, PluginType.SINK}) {
            List<URL> urls =
                    JobPluginClasspathHelper.connectorJarList(
                            Collections.emptyList(), type, common);
            Assertions.assertEquals(
                    Collections.singletonList(commonUrl),
                    urls,
                    "empty plugin configs should still attach env common jars; type=" + type);
        }
    }

    @Test
    void createLocalPluginClassLoader_setsParentClassLoader() {
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        ClassLoader pluginLoader =
                JobPluginClasspathHelper.createLocalPluginClassLoader(
                        parent, Collections.emptyList());
        Assertions.assertSame(parent, pluginLoader.getParent());
    }
}
