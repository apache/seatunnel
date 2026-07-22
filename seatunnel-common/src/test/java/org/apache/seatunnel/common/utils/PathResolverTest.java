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

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.config.Common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;

public class PathResolverTest {

    @BeforeEach
    public void setUp() {
        System.clearProperty("SEATUNNEL_HOME");
        Common.setSeaTunnelHome(null);
    }

    /**
     * SEATUNNEL_HOME will be calculated, and it will be recalculated even if the SEATUNNEL_HOME has
     * been set to null in the {@link #setUp()}
     */
    @Test
    public void testReplacePathWithEnvWithNoStHome() throws MalformedURLException {
        // assert SEATUNNEL_HOME not blank
        Assertions.assertTrue(StringUtils.isNotBlank(Common.getSeaTunnelHome()));

        String jarPath = "/opt/seatunnel-client/connectors/seatunnel/connector-kafka.jar";
        // Handle Windows path separator if needed for test robustness
        if (File.separatorChar == '\\') {
            jarPath = jarPath.replace('/', '\\');
        }

        URL absoluteUrl = new File(jarPath).toURI().toURL();
        URL logicalUrl = PathResolver.replacePathWithEnv(absoluteUrl);

        Assertions.assertEquals(absoluteUrl.getPath(), logicalUrl.getPath());
    }

    @Test
    public void testReplacePathWithEnv() throws MalformedURLException {
        // Simulate Client Side
        String clientHome = "/opt/seatunnel-client";
        System.setProperty("SEATUNNEL_HOME", clientHome);
        Common.setSeaTunnelHome(clientHome);

        // Test file inside SEATUNNEL_HOME
        String jarPath = clientHome + "/connectors/seatunnel/connector-kafka.jar";
        // Handle Windows path separator if needed for test robustness
        if (File.separatorChar == '\\') {
            jarPath = jarPath.replace('/', '\\');
            clientHome = clientHome.replace('/', '\\');
            System.setProperty("SEATUNNEL_HOME", clientHome);
            Common.setSeaTunnelHome(clientHome);
        }

        URL absoluteUrl = new File(jarPath).toURI().toURL();
        URL logicalUrl = PathResolver.replacePathWithEnv(absoluteUrl);

        Assertions.assertEquals(
                "/$SEATUNNEL_HOME/connectors/seatunnel/connector-kafka.jar", logicalUrl.getPath());

        // Test file OUTSIDE SEATUNNEL_HOME
        String outsidePath = "/tmp/other/connector.jar";
        if (File.separatorChar == '\\') {
            outsidePath = "C:\\tmp\\other\\connector.jar";
        }
        URL outsideUrl = new File(outsidePath).toURI().toURL();
        URL resultUrl = PathResolver.replacePathWithEnv(outsideUrl);

        Assertions.assertEquals(outsideUrl, resultUrl);
    }

    @Test
    public void testReplacePathWithEnvDoesNotRewriteSiblingPrefixPath()
            throws MalformedURLException {
        String clientHome = "/opt/seatunnel";
        String siblingPath = "/opt/seatunnel-backup/connectors/seatunnel/connector-kafka.jar";
        if (File.separatorChar == '\\') {
            clientHome = "C:\\opt\\seatunnel";
            siblingPath = "C:\\opt\\seatunnel-backup\\connectors\\seatunnel\\connector-kafka.jar";
        }

        System.setProperty("SEATUNNEL_HOME", clientHome);
        Common.setSeaTunnelHome(clientHome);

        URL siblingUrl = new File(siblingPath).toURI().toURL();
        URL resultUrl = PathResolver.replacePathWithEnv(siblingUrl);

        Assertions.assertEquals(siblingUrl, resultUrl);
    }

    @Test
    public void testReplacePathWithEnvIgnoresNonFileUrl() throws MalformedURLException {
        String clientHome = "/opt/seatunnel-client";
        if (File.separatorChar == '\\') {
            clientHome = "C:\\opt\\seatunnel-client";
        }

        System.setProperty("SEATUNNEL_HOME", clientHome);
        Common.setSeaTunnelHome(clientHome);

        URL remoteUrl = new URL("https://example.com/opt/seatunnel-client/lib/test.jar");
        URL resultUrl = PathResolver.replacePathWithEnv(remoteUrl);

        Assertions.assertEquals(remoteUrl, resultUrl);
    }

    @Test
    public void testResolvePathEnv() throws MalformedURLException {
        // Simulate Server Side
        String serverHome = "/opt/seatunnel-server";
        System.setProperty("SEATUNNEL_HOME", serverHome);
        Common.setSeaTunnelHome(serverHome);

        if (File.separatorChar == '\\') {
            serverHome = serverHome.replace('/', '\\');
            System.setProperty("SEATUNNEL_HOME", serverHome);
            Common.setSeaTunnelHome(serverHome);
        }

        // Logical URL from client
        URL logicalUrl = new URL("file:$SEATUNNEL_HOME/connectors/seatunnel/connector-kafka.jar");
        URL resolvedUrl = PathResolver.resolvePathEnv(logicalUrl);

        String expectedPath =
                Paths.get(serverHome, "connectors/seatunnel/connector-kafka.jar")
                        .toUri()
                        .toURL()
                        .getPath();
        Assertions.assertEquals(expectedPath, resolvedUrl.getPath());
    }

    @Test
    public void testEndToEndFlow() throws MalformedURLException {
        // 1. Client Environment
        String clientHome = "/home/user/client";
        if (File.separatorChar == '\\') {
            clientHome = "C:\\home\\user\\client";
        }
        System.setProperty("SEATUNNEL_HOME", clientHome);
        Common.setSeaTunnelHome(clientHome);

        String jarPath = Paths.get(clientHome, "lib", "test.jar").toString();
        URL clientUrl = new File(jarPath).toURI().toURL();

        // 2. Client replaces path
        URL logicalUrl = PathResolver.replacePathWithEnv(clientUrl);
        Assertions.assertTrue(logicalUrl.getPath().contains("$SEATUNNEL_HOME"));

        // 3. Server Environment (Different Path)
        String serverHome = "/var/lib/server";
        if (File.separatorChar == '\\') {
            serverHome = "D:\\var\\lib\\server";
        }
        System.setProperty("SEATUNNEL_HOME", serverHome);
        Common.setSeaTunnelHome(serverHome);

        // 4. Server resolves path
        URL resolvedUrl = PathResolver.resolvePathEnv(logicalUrl);

        String expectedServerPath =
                Paths.get(serverHome, "lib", "test.jar").toUri().toURL().getPath();
        Assertions.assertEquals(expectedServerPath, resolvedUrl.getPath());
    }
}
