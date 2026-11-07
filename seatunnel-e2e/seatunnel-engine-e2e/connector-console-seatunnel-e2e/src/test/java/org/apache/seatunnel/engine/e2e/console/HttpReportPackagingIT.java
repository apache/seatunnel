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
package org.apache.seatunnel.engine.e2e.console;

import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.MavenJarUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Verifies the packaged HTTP client with the distribution's lib-before-starter classpath. */
class HttpReportPackagingIT {

    @Test
    void isolatesHttpClientFromHadoopOkio() throws Exception {
        Path starter =
                Paths.get(
                        ContainerUtil.PROJECT_ROOT_PATH,
                        "seatunnel-core",
                        "seatunnel-starter",
                        "target",
                        "seatunnel-starter.jar");
        Assertions.assertTrue(Files.isRegularFile(starter), "Run after the package phase");
        URL starterJar = starter.toUri().toURL();
        Path hadoop = Paths.get(MavenJarUtil.getHadoop3UberJarPath());
        Assertions.assertTrue(Files.isRegularFile(hadoop), "The E2E Hadoop uber jar must exist");
        URL hadoopJar = hadoop.toUri().toURL();
        // Do not inherit Maven's dependency ordering, which places the newer Okio first.
        try (URLClassLoader loader =
                new URLClassLoader(
                        new URL[] {hadoopJar, starterJar},
                        ClassLoader.getSystemClassLoader().getParent())) {
            Assertions.assertEquals(
                    hadoopJar,
                    Class.forName("okio.ByteString", false, loader)
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation());
            Class<?> handler =
                    Class.forName(
                            "org.apache.seatunnel.engine.server.event.JobEventHttpReportHandler",
                            false,
                            loader);
            Class<?> clientType = handler.getDeclaredField("httpClient").getType();
            // Construct the client referenced by real packaged engine bytecode, not a test import.
            Object client = clientType.getConstructor().newInstance();
            Assertions.assertNotNull(client);
            Assertions.assertEquals(
                    "org.apache.seatunnel.shade.engine.okhttp3.OkHttpClient", clientType.getName());
            Assertions.assertEquals(
                    starterJar, clientType.getProtectionDomain().getCodeSource().getLocation());
            Assertions.assertEquals(
                    starterJar,
                    Class.forName(
                                    "org.apache.seatunnel.shade.engine.okio.ByteString",
                                    false,
                                    loader)
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation());
        }
    }
}
