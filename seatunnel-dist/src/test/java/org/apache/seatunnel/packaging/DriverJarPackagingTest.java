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

package org.apache.seatunnel.packaging;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Guards the JDBC driver jars shipped in the distribution {@code lib/} directory.
 *
 * <p>The non "-og" {@code org.opengauss:opengauss-jdbc} variant is a pgJDBC fork that still bundles
 * unrelocated {@code org.postgresql.*} classes, including {@code org/postgresql/Driver.class}. If
 * that variant reaches the distribution dependency graph it is copied into {@code lib/} where the
 * flat classpath lets it hijack {@code org.postgresql.*} class resolution, breaking every
 * PostgreSQL connection with "Protocol error. Session setup failed" (issue #11510).
 *
 * <p>The check works on the pom text directly instead of an XML parser so it stays reliable on the
 * seatunnel-dist test classpath, which carries many connector jars that may ship their own XML
 * parser implementations.
 */
public class DriverJarPackagingTest {

    private static final Pattern DEPENDENCY_BLOCK =
            Pattern.compile("<dependency>(.*?)</dependency>", Pattern.DOTALL);

    @Test
    public void testCdcOpenGaussDoesNotLeakUnrelocatedDriver() throws Exception {
        String pom = readDistPom();

        List<String> cdcOpenGaussDeps =
                findDependencyBlocks(pom, "org.apache.seatunnel", "connector-cdc-opengauss");
        Assertions.assertFalse(
                cdcOpenGaussDeps.isEmpty(),
                "connector-cdc-opengauss dependency not found in seatunnel-dist/pom.xml");

        for (String block : cdcOpenGaussDeps) {
            Assertions.assertTrue(
                    declaresExclusion(block, "org.opengauss", "opengauss-jdbc"),
                    "connector-cdc-opengauss must exclude org.opengauss:opengauss-jdbc, "
                            + "otherwise the unrelocated driver fork is copied into lib/ and "
                            + "hijacks org.postgresql.* classes (issue #11510)");
        }
    }

    @Test
    public void testOnlyRelocatedOpenGaussDriverIsDeclared() throws Exception {
        String pom = readDistPom();

        List<String> openGaussDeps = findDependencyBlocks(pom, "org.opengauss", "opengauss-jdbc");
        Assertions.assertFalse(
                openGaussDeps.isEmpty(),
                "expected the relocated openGauss driver to be declared in seatunnel-dist/pom.xml "
                        + "so the Jdbc connector's openGauss dialect keeps working");

        for (String block : openGaussDeps) {
            String version = resolveProperty(pom, extractTag(block, "version"));
            Assertions.assertNotNull(version, "opengauss-jdbc dependency must pin a version");
            Assertions.assertTrue(
                    version.endsWith("-og"),
                    "only the fully relocated '-og' variant of opengauss-jdbc may be declared in "
                            + "seatunnel-dist, but found version: "
                            + version);
        }
    }

    private String readDistPom() throws Exception {
        File pom = locateDistPom();
        return new String(Files.readAllBytes(pom.toPath()), StandardCharsets.UTF_8);
    }

    private File locateDistPom() {
        // Surefire runs with the module base directory as the working directory.
        File direct = new File("pom.xml");
        if (isDistPom(direct)) {
            return direct;
        }
        File nested = new File("seatunnel-dist/pom.xml");
        if (isDistPom(nested)) {
            return nested;
        }
        throw new IllegalStateException(
                "Unable to locate seatunnel-dist/pom.xml from " + new File(".").getAbsolutePath());
    }

    private boolean isDistPom(File candidate) {
        if (!candidate.isFile()) {
            return false;
        }
        try {
            String content =
                    new String(Files.readAllBytes(candidate.toPath()), StandardCharsets.UTF_8);
            Matcher matcher = Pattern.compile("<artifactId>([^<]+)</artifactId>").matcher(content);
            // The parent block comes first; the module's own artifactId is the first one
            // outside of it (the second occurrence, or the first when there is no parent).
            if (matcher.find()) {
                if ("seatunnel-dist".equals(matcher.group(1).trim())) {
                    return true;
                }
                return matcher.find() && "seatunnel-dist".equals(matcher.group(1).trim());
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    private List<String> findDependencyBlocks(String pom, String groupId, String artifactId) {
        List<String> matched = new ArrayList<>();
        Matcher matcher = DEPENDENCY_BLOCK.matcher(pom);
        while (matcher.find()) {
            String block = matcher.group(1);
            if (groupId.equals(extractTag(block, "groupId"))
                    && artifactId.equals(extractTag(block, "artifactId"))) {
                matched.add(block);
            }
        }
        return matched;
    }

    private boolean declaresExclusion(String dependencyBlock, String groupId, String artifactId) {
        Matcher exclusions =
                Pattern.compile("<exclusions>(.*?)</exclusions>", Pattern.DOTALL)
                        .matcher(dependencyBlock);
        while (exclusions.find()) {
            String block = exclusions.group(1);
            if (groupId.equals(extractTag(block, "groupId"))
                    && artifactId.equals(extractTag(block, "artifactId"))) {
                return true;
            }
        }
        return false;
    }

    private String extractTag(String block, String tag) {
        Matcher matcher = Pattern.compile("<" + tag + ">([^<]+)</" + tag + ">").matcher(block);
        return matcher.find() ? matcher.group(1).trim() : null;
    }

    private String resolveProperty(String pom, String value) {
        if (value == null) {
            return null;
        }
        Matcher placeholder = Pattern.compile("\\$\\{([^}]+)}").matcher(value);
        if (!placeholder.find()) {
            return value;
        }
        String quotedKey = Pattern.quote(placeholder.group(1));
        Matcher property =
                Pattern.compile("<" + quotedKey + ">([^<]+)</" + quotedKey + ">").matcher(pom);
        return property.find() ? property.group(1).trim() : value;
    }
}
